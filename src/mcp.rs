use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use axum::extract::{Request, State};
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use clap::Args;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::Api;
use rmcp::handler::server::common::Extension;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ServerHandler, schemars, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::backend::{Backend, PodOpts, Resources};
use crate::build::POD_READY_TIMEOUT;
use crate::pod::{BuilderPod, ExecStatus, LogSink};
use crate::sandbox::{RESULTS_DIR, RelPath, SandboxName, Workspace};

pub const MCP_PATH: &str = "/mcp";
pub const SANDBOX_HEADER: &str = "x-crucible-sandbox";

const SANDBOX_HOSTS: [&str; 2] = ["host.containers.internal", "host.openshell.internal"];

const DEFAULT_BUILD_TIMEOUT_S: u64 = 1800;
const DEFAULT_RUN_TIMEOUT_S: u64 = 600;
const LOG_TAIL_LINES: usize = 60;
const LOG_TAIL_BYTES: usize = 8 * 1024;
const FETCH_LIMIT: u64 = 512 * 1024 * 1024;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
const CLEAN_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Args)]
pub struct McpArgs {
    /// Listen address for the streamable-http endpoint
    #[arg(long, env = "BROKER_BIND", default_value = "0.0.0.0:8849")]
    pub bind: String,
    /// Sandbox path holding the agent's tree
    #[arg(long, env = "BROKER_SANDBOX_WORKDIR")]
    pub sandbox_workdir: Option<String>,
    /// Dev: use a local directory in place of the openshell sandbox
    #[arg(long, value_name = "DIR")]
    pub local_workdir: Option<PathBuf>,
    /// Dev: run off-cluster against this kubeconfig context
    #[arg(long)]
    pub kubecontext: Option<String>,
    /// Dev: namespace for builder pods (in-cluster default: the service account's)
    #[arg(short, long)]
    pub namespace: Option<String>,
    #[arg(long, value_enum, default_value_t = Backend::Buildah)]
    pub backend: Backend,
    /// Builder pod lifetime (activeDeadlineSeconds)
    #[arg(long, default_value_t = 7200)]
    pub pod_deadline: i64,
    /// Live builder pods kept for `run`; the oldest is deleted past this
    #[arg(long, default_value_t = 4)]
    pub max_sessions: usize,
    /// Resource requests for builder pods, repeatable: --request cpu=2
    #[arg(long = "request", value_name = "KEY=QTY", value_parser = crate::parse_kv)]
    pub requests: Vec<(String, String)>,
    /// Resource limits for builder pods, repeatable: --limit memory=8Gi
    #[arg(long = "limit", value_name = "KEY=QTY", value_parser = crate::parse_kv)]
    pub limits: Vec<(String, String)>,
}

pub async fn serve(args: McpArgs) -> Result<()> {
    let token = std::env::var("BROKER_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| anyhow!("BROKER_TOKEN is not set; refusing to serve unauthenticated"))?;
    let workspace = match (&args.local_workdir, &args.sandbox_workdir) {
        (Some(dir), _) => Workspace::Local {
            dir: dir
                .canonicalize()
                .with_context(|| format!("resolving {}", dir.display()))?,
        },
        (None, Some(workdir)) => Workspace::Openshell {
            workdir: workdir.clone(),
        },
        (None, None) => bail!("set BROKER_SANDBOX_WORKDIR (or --local-workdir for dev)"),
    };
    let (client, namespace, in_cluster) =
        kube_client(args.kubecontext.as_deref(), args.namespace.as_deref()).await?;
    let owner = if in_cluster {
        self_owner(client.clone(), &namespace).await
    } else {
        None
    };
    let broker = Arc::new(Broker {
        client,
        namespace,
        workspace,
        backend: args.backend,
        resources: Resources {
            requests: args.requests,
            limits: args.limits,
        },
        deadline_secs: args.pod_deadline,
        max_sessions: args.max_sessions.max(1),
        owner,
        sessions: Mutex::new(Vec::new()),
        runs: AtomicU64::new(0),
    });

    let listener = tokio::net::TcpListener::bind(&args.bind)
        .await
        .with_context(|| format!("binding {}", args.bind))?;
    tracing::info!(
        "buildit mcp listening on http://{}{MCP_PATH} (namespace {}, backend {:?})",
        args.bind,
        broker.namespace,
        broker.backend
    );
    let hosts = allowed_hosts(env_hosts().as_deref());
    let deleted = serve_until(
        listener,
        broker,
        token,
        hosts,
        shutdown_signal()?,
        SHUTDOWN_GRACE,
    )
    .await?;
    tracing::info!("deleted {} builder pod(s) on shutdown", deleted.len());
    Ok(())
}

async fn serve_until(
    listener: tokio::net::TcpListener,
    broker: Arc<Broker>,
    token: String,
    hosts: Vec<String>,
    shutdown: impl Future<Output = ()>,
    grace: Duration,
) -> Result<Vec<String>> {
    let ct = CancellationToken::new();
    let app = router(broker.clone(), token, hosts, ct.clone());
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(ct.clone().cancelled_owned())
        .into_future();
    tokio::pin!(server);
    let served = tokio::select! {
        result = &mut server => result,
        () = shutdown => {
            tracing::info!("shutting down; closing open streams");
            ct.cancel();
            match tokio::time::timeout(grace, &mut server).await {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!("connections still open after {grace:?}; exiting anyway");
                    Ok(())
                }
            }
        }
    };
    let deleted = match tokio::time::timeout(CLEAN_TIMEOUT, broker.clean(None, None)).await {
        Ok(deleted) => deleted,
        Err(_) => {
            tracing::warn!("deleting builder pods timed out after {CLEAN_TIMEOUT:?}");
            Vec::new()
        }
    };
    served.context("serving buildit mcp")?;
    Ok(deleted)
}

pub fn router(
    broker: Arc<Broker>,
    token: String,
    hosts: Vec<String>,
    ct: CancellationToken,
) -> axum::Router {
    let config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(hosts)
        .with_cancellation_token(ct);
    let service: StreamableHttpService<BuilditMcp, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(BuilditMcp::new(broker.clone())),
            Default::default(),
            config,
        );
    axum::Router::new()
        .nest_service(MCP_PATH, service)
        .layer(axum::middleware::from_fn_with_state(
            Arc::new(token),
            require_bearer,
        ))
}

fn shutdown_signal() -> Result<impl Future<Output = ()>> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("installing the SIGTERM handler")?;
    Ok(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    })
}

async fn kube_client(
    kubecontext: Option<&str>,
    namespace: Option<&str>,
) -> Result<(kube::Client, String, bool)> {
    let (config, in_cluster) = match kubecontext {
        Some(ctx) => (
            kube::Config::from_kubeconfig(&kube::config::KubeConfigOptions {
                context: Some(ctx.to_string()),
                ..Default::default()
            })
            .await
            .with_context(|| format!("loading kubeconfig context {ctx}"))?,
            false,
        ),
        None => (
            kube::Config::incluster().context("loading in-cluster config")?,
            true,
        ),
    };
    let namespace = namespace
        .map(str::to_string)
        .unwrap_or_else(|| config.default_namespace.clone());
    let client = kube::Client::try_from(config).context("building kube client")?;
    Ok((client, namespace, in_cluster))
}

async fn self_owner(client: kube::Client, namespace: &str) -> Option<OwnerReference> {
    let name = std::env::var("HOSTNAME").ok()?;
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    match pods.get(&name).await {
        Ok(pod) => Some(OwnerReference {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            name,
            uid: pod.metadata.uid?,
            ..Default::default()
        }),
        Err(e) => {
            tracing::warn!("cannot read own pod {name} ({e}); builder pods get no owner");
            None
        }
    }
}

fn env_hosts() -> Option<String> {
    std::env::var("BROKER_ALLOWED_HOSTS").ok()
}

pub fn allowed_hosts(extra: Option<&str>) -> Vec<String> {
    let mut hosts = StreamableHttpServerConfig::default().allowed_hosts;
    let extra = SANDBOX_HOSTS.iter().copied().chain(
        extra
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|h| !h.is_empty()),
    );
    for host in extra {
        if !hosts.iter().any(|h| h == host) {
            hosts.push(host.to_string());
        }
    }
    hosts
}

async fn require_bearer(State(token): State<Arc<String>>, req: Request, next: Next) -> Response {
    let got = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if authorized(got, &token) {
        return next.run(req).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::CONTENT_TYPE, "application/json")],
        r#"{"error":"missing or wrong bearer token"}"#,
    )
        .into_response()
}

fn authorized(header: Option<&str>, want: &str) -> bool {
    header
        .and_then(|h| h.strip_prefix("Bearer "))
        .is_some_and(|got| constant_time_eq(got.as_bytes(), want.as_bytes()))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

struct Session {
    id: String,
    pod: BuilderPod,
    sandbox: Option<SandboxName>,
}

impl Session {
    fn tag(&self) -> String {
        local_tag(&self.id)
    }
}

fn local_tag(id: &str) -> String {
    format!("localhost/buildit/{id}:latest")
}

pub struct Broker {
    client: kube::Client,
    namespace: String,
    workspace: Workspace,
    backend: Backend,
    resources: Resources,
    deadline_secs: i64,
    max_sessions: usize,
    owner: Option<OwnerReference>,
    // oldest first
    sessions: Mutex<Vec<Arc<Session>>>,
    runs: AtomicU64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BuildParams {
    /// Build context: a subdirectory of the workspace, e.g. "svc/api". The workspace root is refused.
    pub context: String,
    /// Dockerfile path relative to the context (default "Dockerfile")
    #[serde(default)]
    pub dockerfile: Option<String>,
    /// Multi-stage target to stop at
    #[serde(default)]
    pub target: Option<String>,
    /// Build args, NAME -> value
    #[serde(default)]
    pub build_args: BTreeMap<String, String>,
    /// Build timeout in seconds (default 1800)
    #[serde(default)]
    pub timeout_s: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunParams {
    /// build_id returned by a successful build
    pub build_id: String,
    /// argv to run in a fresh container of the built image; use ["sh", "-c", "..."] for a shell
    pub cmd: Vec<String>,
    /// Run timeout in seconds (default 600)
    #[serde(default)]
    pub timeout_s: Option<u64>,
    /// Container paths to copy back into .buildit/<build_id>/<path> after the run
    #[serde(default)]
    pub fetch_paths: Vec<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CleanParams {
    /// Build to delete; omit to delete every build of this sandbox
    #[serde(default)]
    pub build_id: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct BuildReply {
    pub build_id: String,
    pub exit: Option<i32>,
    pub timed_out: bool,
    pub runnable: bool,
    pub log: String,
    pub log_tail: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RunReply {
    pub exit: Option<i32>,
    pub timed_out: bool,
    pub log: String,
    pub log_tail: String,
    pub fetched: Vec<String>,
    pub missing: Vec<String>,
    pub skipped: Vec<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct CleanReply {
    pub deleted: Vec<String>,
}

struct BuildRequest {
    context: RelPath,
    dockerfile: RelPath,
    target: Option<String>,
    build_args: Vec<String>,
    timeout: Duration,
}

impl BuildRequest {
    fn parse(p: BuildParams) -> Result<Self> {
        let context = RelPath::parse(&p.context)?;
        let dockerfile = RelPath::parse(p.dockerfile.as_deref().unwrap_or("Dockerfile"))?;
        let target = p.target.map(|t| t.trim().to_string());
        if let Some(t) = &target
            && (t.is_empty() || !t.bytes().all(|b| b.is_ascii_graphic()))
        {
            bail!("invalid target {t:?}");
        }
        let build_args = p
            .build_args
            .into_iter()
            .map(|(k, v)| {
                if k.is_empty() || k.contains('=') || k.contains(char::is_whitespace) {
                    bail!("invalid build arg name {k:?}");
                }
                Ok(format!("{k}={v}"))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            context,
            dockerfile,
            target,
            build_args,
            timeout: timeout(p.timeout_s, DEFAULT_BUILD_TIMEOUT_S),
        })
    }
}

fn timeout(requested: Option<u64>, default: u64) -> Duration {
    Duration::from_secs(requested.unwrap_or(default).clamp(1, 7200))
}

fn from_argv(tag: &str, ctr: &str) -> Vec<String> {
    ["buildah", "from", "--pull-never", "--name", ctr, tag]
        .map(String::from)
        .to_vec()
}

fn run_argv(ctr: &str, cmd: &[String], timeout: Duration) -> Vec<String> {
    let mut argv = [
        "timeout",
        "-k",
        "10",
        &timeout.as_secs().to_string(),
        "buildah",
        "run",
        ctr,
        "--",
    ]
    .map(String::from)
    .to_vec();
    argv.extend(cmd.iter().cloned());
    argv
}

const FETCH_SCRIPT: &str = r#"ctr="$1"; shift; mnt=$(buildah mount "$ctr") || exit 1; cd "$mnt" || exit 1; exec tar -cf - --ignore-failed-read -- "$@""#;

fn fetch_argv(ctr: &str, paths: &[RelPath]) -> Vec<String> {
    let mut argv = ["buildah", "unshare", "sh", "-c", FETCH_SCRIPT, "sh", ctr]
        .map(String::from)
        .to_vec();
    argv.extend(paths.iter().map(|p| p.as_str().to_string()));
    argv
}

fn rm_argv(ctr: &str) -> Vec<String> {
    ["buildah", "rm", ctr].map(String::from).to_vec()
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Fetched {
    files: Vec<String>,
    skipped: Vec<String>,
    missing: Vec<String>,
}

fn unpack_fetched(tar: &[u8], dest: &Path, requested: &[RelPath]) -> Result<Fetched> {
    use tar::EntryType;
    let mut out = Fetched::default();
    let mut seen = Vec::new();
    let mut archive = tar::Archive::new(tar);
    for entry in archive.entries().context("reading fetched tar")? {
        let mut entry = entry.context("reading fetched tar entry")?;
        let raw = entry
            .path()
            .context("fetched entry path")?
            .to_string_lossy()
            .into_owned();
        let Ok(rel) = RelPath::parse(&raw) else {
            out.skipped.push(raw);
            continue;
        };
        seen.push(rel.as_str().to_string());
        match entry.header().entry_type() {
            EntryType::Regular | EntryType::Continuous => {
                if !entry
                    .unpack_in(dest)
                    .with_context(|| format!("unpacking {raw}"))?
                {
                    out.skipped.push(raw);
                    continue;
                }
                out.files.push(rel.as_str().to_string());
            }
            EntryType::Directory => {
                std::fs::create_dir_all(dest.join(rel.as_str()))
                    .with_context(|| format!("creating {raw}"))?;
            }
            _ => out.skipped.push(raw),
        }
    }
    for want in requested {
        let w = want.as_str();
        let found = seen
            .iter()
            .any(|s| s == w || s.strip_prefix(w).is_some_and(|r| r.starts_with('/')));
        if !found {
            out.missing.push(w.to_string());
        }
    }
    Ok(out)
}

fn tail(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(LOG_TAIL_LINES);
    let joined = lines[start..].join("\n");
    if joined.len() <= LOG_TAIL_BYTES {
        return joined;
    }
    let mut cut = joined.len() - LOG_TAIL_BYTES;
    while !joined.is_char_boundary(cut) {
        cut += 1;
    }
    joined[cut..].to_string()
}

fn read_tail(path: &Path) -> String {
    std::fs::read(path)
        .map(|b| tail(&String::from_utf8_lossy(&b)))
        .unwrap_or_default()
}

fn results_path(id: &str, file: &str) -> String {
    format!("{RESULTS_DIR}/{id}/{file}")
}

impl Broker {
    fn sandbox_for(&self, parts: &Parts) -> Result<Option<SandboxName>> {
        let raw = parts
            .headers
            .get(SANDBOX_HEADER)
            .map(|v| v.to_str().context("sandbox header is not ascii"))
            .transpose()?;
        match (raw, self.workspace.needs_sandbox()) {
            (Some(raw), _) => Ok(Some(SandboxName::parse(raw)?)),
            (None, true) => bail!("request carries no {SANDBOX_HEADER} header; refusing"),
            (None, false) => Ok(None),
        }
    }

    async fn publish(&self, sandbox: Option<SandboxName>, results: PathBuf) -> Result<()> {
        let ws = &self.workspace;
        tokio::task::block_in_place(|| ws.publish(sandbox.as_ref(), &results))
    }

    pub async fn build(
        &self,
        sandbox: Option<SandboxName>,
        params: BuildParams,
    ) -> Result<BuildReply> {
        let req = BuildRequest::parse(params)?;
        let id = crate::pod::unique_name();
        let stage = tempfile::tempdir().context("creating staging dir")?;
        let ctx_dir = stage.path().join("ctx");
        let results = stage.path().join(&id);
        std::fs::create_dir_all(&results).context("creating results dir")?;
        let log_path = results.join("build.log");

        let ws = &self.workspace;
        tokio::task::block_in_place(|| ws.fetch_context(sandbox.as_ref(), &req.context, &ctx_dir))?;
        let tarball = crate::context::tarball(&ctx_dir)?;
        tracing::info!(
            "build {id}: context {} ({} KiB)",
            req.context.as_str(),
            tarball.len() / 1024
        );

        let opts = PodOpts {
            idle_nodes: &[],
            resources: &self.resources,
            cache: None,
            node: None,
            deadline_secs: self.deadline_secs,
            owner: self.owner.as_ref(),
        };
        let pod = BuilderPod::create_named(
            self.client.clone(),
            &self.namespace,
            self.backend,
            &id,
            &opts,
        )
        .await?;
        let mut sink = LogSink::file(&log_path)?;
        let steps = self.backend.local_build_steps(
            &local_tag(&id),
            req.dockerfile.as_str(),
            req.target.as_deref(),
            &req.build_args,
        );
        let outcome = tokio::time::timeout(
            req.timeout,
            drive(&pod, self.backend, &tarball, &steps, &mut sink),
        )
        .await;
        let (status, timed_out) = match outcome {
            Ok(Ok(status)) => (Some(status), false),
            Ok(Err(e)) => {
                sink.line(&format!("buildit: {e:#}")).await.ok();
                (None, false)
            }
            Err(_) => {
                sink.line(&format!("buildit: timed out after {:?}", req.timeout))
                    .await
                    .ok();
                (None, true)
            }
        };
        sink.flush().await.ok();
        let success = status.as_ref().is_some_and(ExecStatus::success);
        let runnable = success && self.backend == Backend::Buildah;
        if runnable {
            self.keep(Session {
                id: id.clone(),
                pod,
                sandbox: sandbox.clone(),
            })
            .await;
        } else if let Err(e) = pod.delete().await {
            tracing::warn!("deleting builder pod {id}: {e:#}");
        }
        let log_tail = read_tail(&log_path);
        self.publish(sandbox, results).await?;
        Ok(BuildReply {
            log: results_path(&id, "build.log"),
            build_id: id,
            exit: status.and_then(|s| s.code),
            timed_out,
            runnable,
            log_tail,
        })
    }

    async fn keep(&self, session: Session) {
        let evicted = {
            let mut live = self.sessions.lock().await;
            live.push(Arc::new(session));
            let excess = live.len().saturating_sub(self.max_sessions);
            live.drain(..excess).collect::<Vec<_>>()
        };
        for old in evicted {
            tracing::info!("evicting build {} (max {} live)", old.id, self.max_sessions);
            if let Err(e) = old.pod.delete().await {
                tracing::warn!("deleting builder pod {}: {e:#}", old.id);
            }
        }
    }

    async fn session(&self, sandbox: Option<&SandboxName>, id: &str) -> Result<Arc<Session>> {
        self.sessions
            .lock()
            .await
            .iter()
            .find(|s| s.id == id && s.sandbox.as_ref() == sandbox)
            .cloned()
            .ok_or_else(|| anyhow!("no live build {id:?}; build again"))
    }

    pub async fn run(&self, sandbox: Option<SandboxName>, params: RunParams) -> Result<RunReply> {
        if params.cmd.is_empty() {
            bail!("cmd must not be empty");
        }
        let paths = params
            .fetch_paths
            .iter()
            .map(|p| RelPath::parse_container(p))
            .collect::<Result<Vec<_>>>()?;
        let session = self.session(sandbox.as_ref(), &params.build_id).await?;
        let limit = timeout(params.timeout_s, DEFAULT_RUN_TIMEOUT_S);
        let stage = tempfile::tempdir().context("creating staging dir")?;
        let results = stage.path().join(&session.id);
        std::fs::create_dir_all(&results).context("creating results dir")?;
        let log_path = results.join("run.log");
        let mut sink = LogSink::file(&log_path)?;

        let n = self.runs.fetch_add(1, Ordering::Relaxed);
        let ctr = format!("{}-run{n}", session.id);
        let pod = &session.pod;
        pod.exec_capture(&from_argv(&session.tag(), &ctr)).await?;
        let argv = run_argv(&ctr, &params.cmd, limit);
        sink.line(&format!("$ {}", params.cmd.join(" "))).await?;
        let status = tokio::time::timeout(
            limit + Duration::from_secs(30),
            pod.exec_logged(&argv, &mut sink),
        )
        .await;
        let (exit, timed_out) = match status {
            Ok(Ok(s)) => (s.code, s.code == Some(124)),
            Ok(Err(e)) => {
                sink.line(&format!("buildit: {e:#}")).await.ok();
                (None, false)
            }
            Err(_) => (None, true),
        };
        sink.flush().await.ok();

        let fetched = if paths.is_empty() {
            Fetched::default()
        } else {
            let tar = pod
                .exec_capture_bytes(&fetch_argv(&ctr, &paths), FETCH_LIMIT)
                .await
                .context("fetching paths from the container")?;
            let dest = results.clone();
            tokio::task::block_in_place(|| unpack_fetched(&tar, &dest, &paths))?
        };
        if let Err(e) = pod.exec_capture(&rm_argv(&ctr)).await {
            tracing::warn!("removing container {ctr}: {e:#}");
        }
        let log_tail = read_tail(&log_path);
        self.publish(sandbox, results).await?;
        Ok(RunReply {
            exit,
            timed_out,
            log: results_path(&session.id, "run.log"),
            log_tail,
            fetched: fetched
                .files
                .iter()
                .map(|f| results_path(&session.id, f))
                .collect(),
            missing: fetched.missing,
            skipped: fetched.skipped,
        })
    }

    pub async fn clean(&self, sandbox: Option<&SandboxName>, id: Option<&str>) -> Vec<String> {
        let doomed = {
            let mut live = self.sessions.lock().await;
            let (doomed, keep): (Vec<_>, Vec<_>) = live.drain(..).partition(|s| match id {
                Some(id) => s.id == id && s.sandbox.as_ref() == sandbox,
                None => sandbox.is_none() || s.sandbox.as_ref() == sandbox,
            });
            *live = keep;
            doomed
        };
        let mut deleted = Vec::new();
        for s in doomed {
            match s.pod.delete().await {
                Ok(()) => deleted.push(s.id.clone()),
                Err(e) => tracing::warn!("deleting builder pod {}: {e:#}", s.id),
            }
        }
        deleted
    }
}

async fn drive(
    pod: &BuilderPod,
    backend: Backend,
    tarball: &[u8],
    steps: &[Vec<String>],
    sink: &mut LogSink,
) -> Result<ExecStatus> {
    pod.wait_ready(POD_READY_TIMEOUT).await?;
    pod.exec_stream(&backend.setup_command(), sink).await?;
    pod.exec_with_stdin(&backend.untar_command(), tarball)
        .await?;
    let mut last = None;
    for step in steps {
        sink.line(&format!("$ {}", step.join(" "))).await?;
        let status = pod.exec_logged(step, sink).await?;
        if !status.success() {
            return Ok(status);
        }
        last = Some(status);
    }
    last.ok_or_else(|| anyhow!("backend produced no build steps"))
}

#[derive(Clone)]
pub struct BuilditMcp {
    broker: Arc<Broker>,
}

fn reply<T: Serialize>(result: Result<T>) -> Result<String, String> {
    result
        .and_then(|r| serde_json::to_string(&r).context("serializing reply"))
        .map_err(|e| format!("{e:#}"))
}

#[tool_router]
impl BuilditMcp {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    #[tool(
        description = "Build a Dockerfile from a workspace subdirectory on a remote builder. \
        Pushes nothing. Returns build_id, exit, and the log tail; the full log is at \
        .buildit/<build_id>/build.log. A runnable build keeps its builder alive for `run`."
    )]
    async fn build(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(params): Parameters<BuildParams>,
    ) -> Result<String, String> {
        let broker = &self.broker;
        reply(
            async {
                let sandbox = broker.sandbox_for(&parts)?;
                broker.build(sandbox, params).await
            }
            .await,
        )
    }

    #[tool(
        description = "Run a command in a fresh container of a built image. Returns exit and \
        the log tail (full log at .buildit/<build_id>/run.log); fetch_paths are copied out of \
        the container into .buildit/<build_id>/<path>."
    )]
    async fn run(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(params): Parameters<RunParams>,
    ) -> Result<String, String> {
        let broker = &self.broker;
        reply(
            async {
                let sandbox = broker.sandbox_for(&parts)?;
                broker.run(sandbox, params).await
            }
            .await,
        )
    }

    #[tool(description = "Delete a build's builder pod, or all of this workspace's builds.")]
    async fn clean(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(params): Parameters<CleanParams>,
    ) -> Result<String, String> {
        let broker = &self.broker;
        reply(
            async {
                let sandbox = broker.sandbox_for(&parts)?;
                let deleted = broker
                    .clean(sandbox.as_ref(), params.build_id.as_deref())
                    .await;
                Ok(CleanReply { deleted })
            }
            .await,
        )
    }
}

#[tool_handler]
impl ServerHandler for BuilditMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("buildit", env!("CARGO_PKG_VERSION")))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use rmcp::ServiceExt;
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    use crate::backend::{Backend, PodOpts, Resources};
    use crate::mcp::{
        Broker, BuildParams, BuildRequest, MCP_PATH, SANDBOX_HEADER, Session, allowed_hosts,
        authorized, constant_time_eq, fetch_argv, from_argv, rm_argv, router, run_argv,
        serve_until, shutdown_signal, tail, unpack_fetched,
    };
    use crate::pod::BuilderPod;
    use crate::sandbox::{RelPath, Workspace};

    const TOKEN: &str = "s3cr3t-token";

    fn params(context: &str) -> BuildParams {
        BuildParams {
            context: context.to_string(),
            dockerfile: None,
            target: None,
            build_args: BTreeMap::new(),
            timeout_s: None,
        }
    }

    #[test]
    fn bearer_must_match_exactly() {
        assert!(authorized(Some("Bearer s3cr3t"), "s3cr3t"));
        assert!(!authorized(None, "s3cr3t"));
        assert!(!authorized(Some("s3cr3t"), "s3cr3t"));
        assert!(!authorized(Some("Bearer wrong"), "s3cr3t"));
        assert!(!authorized(Some("Bearer s3cr3t2"), "s3cr3t"));
        assert!(!authorized(Some("Bearer "), "s3cr3t"));
        assert!(!authorized(Some("bearer s3cr3t"), "s3cr3t"));
        assert!(constant_time_eq(b"", b""));
        assert!(!constant_time_eq(b"abc", b"abd"));
    }

    #[test]
    fn sandbox_hosts_join_loopback() {
        let hosts = allowed_hosts(None);
        for h in [
            "localhost",
            "127.0.0.1",
            "host.containers.internal",
            "host.openshell.internal",
        ] {
            assert!(hosts.iter().any(|x| x == h), "{h} in {hosts:?}");
        }
        let hosts = allowed_hosts(Some(
            " broker.svc , ,host.openshell.internal,broker.svc:8849",
        ));
        assert_eq!(
            hosts
                .iter()
                .filter(|h| *h == "host.openshell.internal")
                .count(),
            1
        );
        assert!(hosts.iter().any(|h| h == "broker.svc"));
        assert!(hosts.iter().any(|h| h == "broker.svc:8849"));
        assert!(!hosts.iter().any(String::is_empty));
    }

    #[test]
    fn build_request_confines_paths() {
        assert!(BuildRequest::parse(params("svc")).is_ok());
        for bad in [
            "",
            ".",
            "/",
            "/abs",
            "..",
            "../up",
            "svc/../..",
            ".git",
            ".claude/x",
        ] {
            assert!(BuildRequest::parse(params(bad)).is_err(), "{bad:?}");
        }
        let mut p = params("svc");
        p.dockerfile = Some("../Dockerfile".to_string());
        assert!(BuildRequest::parse(p).is_err());
        let mut p = params("svc");
        p.dockerfile = Some("/etc/Dockerfile".to_string());
        assert!(BuildRequest::parse(p).is_err());
        let mut p = params("svc");
        p.dockerfile = Some("docker/Dockerfile.dev".to_string());
        assert_eq!(
            BuildRequest::parse(p).unwrap().dockerfile.as_str(),
            "docker/Dockerfile.dev"
        );
    }

    #[test]
    fn build_request_validates_args_target_and_timeout() {
        let mut p = params("svc");
        p.build_args = BTreeMap::from([
            ("A".to_string(), "1".to_string()),
            ("B".to_string(), "x=y z".to_string()),
        ]);
        p.target = Some(" builder ".to_string());
        p.timeout_s = Some(99_999);
        let req = BuildRequest::parse(p).unwrap();
        assert_eq!(req.build_args, ["A=1", "B=x=y z"]);
        assert_eq!(req.target.as_deref(), Some("builder"));
        assert_eq!(req.timeout, Duration::from_secs(7200));
        for bad in ["", "A=B", "A B"] {
            let mut p = params("svc");
            p.build_args = BTreeMap::from([(bad.to_string(), "v".to_string())]);
            assert!(BuildRequest::parse(p).is_err(), "{bad:?}");
        }
        for bad in ["", " ", "a b", "a\tb"] {
            let mut p = params("svc");
            p.target = Some(bad.to_string());
            assert!(BuildRequest::parse(p).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn run_argv_shapes() {
        assert_eq!(
            from_argv("localhost/buildit/b1:latest", "b1-run0"),
            [
                "buildah",
                "from",
                "--pull-never",
                "--name",
                "b1-run0",
                "localhost/buildit/b1:latest"
            ]
        );
        let cmd = vec![
            "sh".to_string(),
            "-c".to_string(),
            "echo hi; exit 3".to_string(),
        ];
        assert_eq!(
            run_argv("b1-run0", &cmd, Duration::from_secs(42)),
            [
                "timeout",
                "-k",
                "10",
                "42",
                "buildah",
                "run",
                "b1-run0",
                "--",
                "sh",
                "-c",
                "echo hi; exit 3"
            ]
        );
        let paths = [
            RelPath::parse_container("/out/app").unwrap(),
            RelPath::parse_container("/etc/os-release").unwrap(),
        ];
        let fetch = fetch_argv("b1-run0", &paths);
        assert_eq!(fetch[..4], ["buildah", "unshare", "sh", "-c"]);
        assert_eq!(fetch[5..], ["sh", "b1-run0", "out/app", "etc/os-release"]);
        assert_eq!(rm_argv("b1-run0"), ["buildah", "rm", "b1-run0"]);
    }

    fn tar_with(build: impl FnOnce(&mut tar::Builder<Vec<u8>>)) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        build(&mut b);
        b.into_inner().unwrap()
    }

    fn file_header(path: &str, len: usize) -> tar::Header {
        let mut h = tar::Header::new_gnu();
        h.set_path(path).unwrap();
        h.set_size(len as u64);
        h.set_mode(0o755);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        h
    }

    #[cfg(unix)]
    #[test]
    fn unpack_keeps_files_and_dirs_only() {
        let data = tar_with(|b| {
            let mut dir = tar::Header::new_gnu();
            dir.set_path("out/").unwrap();
            dir.set_entry_type(tar::EntryType::Directory);
            dir.set_mode(0o755);
            dir.set_size(0);
            dir.set_cksum();
            b.append(&dir, &[][..]).unwrap();
            b.append(&file_header("out/app", 3), &b"bin"[..]).unwrap();
            let mut link = tar::Header::new_gnu();
            link.set_entry_type(tar::EntryType::Symlink);
            link.set_size(0);
            b.append_link(&mut link, "out/passwd", "/etc/passwd")
                .unwrap();
            let mut hard = tar::Header::new_gnu();
            hard.set_entry_type(tar::EntryType::Link);
            hard.set_size(0);
            b.append_link(&mut hard, "out/hard", "/etc/shadow").unwrap();
        });
        let mut raw = data;
        // a hostile `..` entry, written past the tar crate's own path checks
        let mut evil = file_header("xx/evil", 4);
        let name = b"../evil";
        evil.as_old_mut().name[..name.len()].copy_from_slice(name);
        evil.as_old_mut().name[name.len()] = 0;
        evil.set_cksum();
        let end = raw.len() - 1024;
        raw.truncate(end);
        raw.extend_from_slice(evil.as_bytes());
        let mut body = b"evil".to_vec();
        body.resize(512, 0);
        raw.extend_from_slice(&body);
        raw.extend_from_slice(&[0u8; 1024]);

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("b1");
        std::fs::create_dir_all(&dest).unwrap();
        let requested = [
            RelPath::parse_container("/out").unwrap(),
            RelPath::parse_container("/not/there").unwrap(),
        ];
        let got = unpack_fetched(&raw, &dest, &requested).unwrap();
        assert_eq!(got.files, ["out/app"]);
        assert_eq!(got.missing, ["not/there"]);
        assert!(got.skipped.iter().any(|s| s == "out/passwd"), "{got:?}");
        assert!(got.skipped.iter().any(|s| s == "out/hard"), "{got:?}");
        assert!(got.skipped.iter().any(|s| s == "../evil"), "{got:?}");
        assert_eq!(std::fs::read(dest.join("out/app")).unwrap(), b"bin");
        assert!(std::fs::symlink_metadata(dest.join("out/passwd")).is_err());
        assert!(!dir.path().join("evil").exists());
    }

    #[test]
    fn tail_caps_lines_and_bytes() {
        let text: String = (0..100).map(|i| format!("line {i}\n")).collect();
        let t = tail(&text);
        assert!(t.starts_with("line 40"), "{t}");
        assert!(t.ends_with("line 99"), "{t}");
        let wide = "é".repeat(10_000);
        let t = tail(&wide);
        assert!(t.len() <= 8 * 1024);
        assert!(t.chars().all(|c| c == 'é'));
    }

    fn broker(workspace: Workspace) -> Arc<Broker> {
        broker_at(workspace, "http://127.0.0.1:9")
    }

    fn broker_at(workspace: Workspace, api: &str) -> Arc<Broker> {
        let config = kube::Config::new(api.parse().unwrap());
        Arc::new(Broker {
            client: kube::Client::try_from(config).unwrap(),
            namespace: "builds".to_string(),
            workspace,
            backend: Backend::Buildah,
            resources: Resources::default(),
            deadline_secs: 600,
            max_sessions: 2,
            owner: None,
            sessions: Mutex::new(Vec::new()),
            runs: AtomicU64::new(0),
        })
    }

    async fn serve(workspace: Workspace) -> String {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let app = router(
            broker(workspace),
            TOKEN.to_string(),
            allowed_hosts(None),
            CancellationToken::new(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr.to_string()
    }

    // one initialize POST over a bare socket, so Host and Authorization are
    // exactly what we say; returns the status code
    async fn raw_initialize(addr: &str, host: &str, auth: Option<&str>) -> u16 {
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}"#;
        let auth = auth
            .map(|a| format!("Authorization: {a}\r\n"))
            .unwrap_or_default();
        let req = format!(
            "POST {MCP_PATH} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n{auth}Content-Length: {}\r\n\
             Connection: close\r\n\r\n{body}",
            body.len()
        );
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut buf = [0u8; 64];
        let mut read = 0;
        while !buf[..read].windows(2).any(|w| w == b"\r\n") {
            let n = sock.read(&mut buf[read..]).await.unwrap();
            assert!(n > 0, "connection closed before a status line");
            read += n;
        }
        let line = String::from_utf8_lossy(&buf[..read]).into_owned();
        line.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_rejects_missing_or_wrong_token_and_foreign_hosts() {
        let addr = serve(Workspace::Local {
            dir: std::env::temp_dir(),
        })
        .await;
        let bearer = format!("Bearer {TOKEN}");
        assert_eq!(raw_initialize(&addr, "127.0.0.1", None).await, 401);
        assert_eq!(
            raw_initialize(&addr, "127.0.0.1", Some("Bearer nope")).await,
            401
        );
        assert_eq!(raw_initialize(&addr, "127.0.0.1", Some(TOKEN)).await, 401);
        assert_eq!(
            raw_initialize(&addr, "evil.example", Some(&bearer)).await,
            403
        );
        assert_eq!(
            raw_initialize(&addr, "host.openshell.internal:8849", Some(&bearer)).await,
            200
        );
        assert_eq!(
            raw_initialize(&addr, "host.containers.internal", Some(&bearer)).await,
            200
        );
        assert_eq!(raw_initialize(&addr, "127.0.0.1", Some(&bearer)).await, 200);
    }

    async fn client(
        addr: &str,
        token: &str,
        sandbox: Option<&str>,
    ) -> Result<
        rmcp::service::RunningService<rmcp::RoleClient, ()>,
        Box<rmcp::service::ClientInitializeError>,
    > {
        let mut headers = HashMap::new();
        if let Some(name) = sandbox {
            headers.insert(SANDBOX_HEADER.parse().unwrap(), name.parse().unwrap());
        }
        let config =
            StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}{MCP_PATH}"))
                .auth_header(token)
                .custom_headers(headers);
        ().serve(StreamableHttpClientTransport::from_config(config))
            .await
            .map_err(Box::new)
    }

    fn text(result: &rmcp::model::CallToolResult) -> String {
        result
            .content
            .iter()
            .filter_map(|c| c.as_text().map(|t| t.text.clone()))
            .collect()
    }

    fn call(name: &'static str, args: serde_json::Value) -> CallToolRequestParams {
        let serde_json::Value::Object(map) = args else {
            panic!("tool args must be an object")
        };
        CallToolRequestParams::new(name).with_arguments(map)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mcp_round_trip_lists_tools_and_reports_errors() {
        let dir = tempfile::tempdir().unwrap();
        let addr = serve(Workspace::Local {
            dir: dir.path().to_path_buf(),
        })
        .await;

        assert!(
            client(&addr, "wrong", None).await.is_err(),
            "initialize must fail without the right token"
        );

        let c = client(&addr, TOKEN, Some("ci-1-abc")).await.unwrap();
        let tools = c.list_all_tools().await.unwrap();
        let mut names: Vec<_> = tools.iter().map(|t| t.name.to_string()).collect();
        names.sort();
        assert_eq!(names, ["build", "clean", "run"]);
        let build = tools.iter().find(|t| t.name == "build").unwrap();
        let props = build.input_schema["properties"].as_object().unwrap();
        let mut keys: Vec<_> = props.keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            ["build_args", "context", "dockerfile", "target", "timeout_s"]
        );
        let schemas = serde_json::to_string(&tools).unwrap();
        assert!(!schemas.to_lowercase().contains("secret"), "{schemas}");

        let r = c
            .call_tool(call("build", serde_json::json!({ "context": "../escape" })))
            .await
            .unwrap();
        assert_eq!(r.is_error, Some(true));
        assert!(text(&r).contains("`..`"), "{}", text(&r));

        let r = c
            .call_tool(call("build", serde_json::json!({ "context": "." })))
            .await
            .unwrap();
        assert_eq!(r.is_error, Some(true));
        assert!(text(&r).contains("subdirectory"), "{}", text(&r));

        let r = c
            .call_tool(call(
                "run",
                serde_json::json!({ "build_id": "buildit-nope", "cmd": ["true"] }),
            ))
            .await
            .unwrap();
        assert_eq!(r.is_error, Some(true));
        assert!(text(&r).contains("no live build"), "{}", text(&r));

        let r = c
            .call_tool(call("clean", serde_json::json!({})))
            .await
            .unwrap();
        assert_ne!(r.is_error, Some(true));
        assert_eq!(text(&r), r#"{"deleted":[]}"#);
        c.cancel().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openshell_mode_refuses_requests_without_a_sandbox_header() {
        let addr = serve(Workspace::Openshell {
            workdir: "/sandbox/repo".to_string(),
        })
        .await;
        let c = client(&addr, TOKEN, None).await.unwrap();
        let r = c
            .call_tool(call("build", serde_json::json!({ "context": "svc" })))
            .await
            .unwrap();
        assert_eq!(r.is_error, Some(true));
        assert!(text(&r).contains(SANDBOX_HEADER), "{}", text(&r));
        c.cancel().await.unwrap();

        let c = client(&addr, TOKEN, Some("--gateway=evil")).await.unwrap();
        let r = c
            .call_tool(call("clean", serde_json::json!({})))
            .await
            .unwrap();
        assert_eq!(r.is_error, Some(true));
        assert!(text(&r).contains("invalid sandbox name"), "{}", text(&r));
        c.cancel().await.unwrap();
    }

    #[test]
    fn local_tag_is_not_pushable() {
        assert!(Path::new(&crate::mcp::local_tag("buildit-1")).starts_with("localhost"));
    }

    async fn apiserver() -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        use axum::extract::{Path as UrlPath, State};
        use axum::http::{StatusCode, header};
        use axum::routing::{delete, post};
        type Seen = Arc<std::sync::Mutex<Vec<String>>>;
        type Reply = (StatusCode, [(header::HeaderName, &'static str); 1], String);
        async fn create(State(seen): State<Seen>, pod: String) -> Reply {
            seen.lock().unwrap().push("POST pods".to_string());
            (
                StatusCode::CREATED,
                [(header::CONTENT_TYPE, "application/json")],
                pod,
            )
        }
        async fn remove(
            State(seen): State<Seen>,
            UrlPath((ns, name)): UrlPath<(String, String)>,
        ) -> Reply {
            seen.lock().unwrap().push(format!("DELETE {ns}/{name}"));
            let pod = serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": name, "namespace": ns },
            });
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                pod.to_string(),
            )
        }
        let seen: Seen = Arc::default();
        let app = axum::Router::new()
            .route("/api/v1/namespaces/{ns}/pods", post(create))
            .route("/api/v1/namespaces/{ns}/pods/{name}", delete(remove))
            .with_state(seen.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    async fn exchange(addr: &str, req: String) -> String {
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), sock.read_to_end(&mut out))
            .await
            .expect("response did not finish")
            .unwrap();
        String::from_utf8_lossy(&out).into_owned()
    }

    fn post(body: &str, session: Option<&str>) -> String {
        let session = session
            .map(|s| format!("Mcp-Session-Id: {s}\r\nMCP-Protocol-Version: 2025-06-18\r\n"))
            .unwrap_or_default();
        format!(
            "POST {MCP_PATH} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {TOKEN}\r\n\
             Content-Type: application/json\r\nAccept: application/json, text/event-stream\r\n\
             {session}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    async fn open_sse(addr: &str) -> tokio::net::TcpStream {
        let init = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}"#;
        let reply = exchange(addr, post(init, None)).await;
        let session = reply
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("mcp-session-id:")
                    .map(|_| l["mcp-session-id:".len()..].trim().to_string())
            })
            .unwrap_or_else(|| panic!("no session id in {reply}"));
        let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        let reply = exchange(addr, post(note, Some(&session))).await;
        assert!(reply.starts_with("HTTP/1.1 202"), "{reply}");

        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let get = format!(
            "GET {MCP_PATH} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {TOKEN}\r\n\
             Accept: text/event-stream\r\nMcp-Session-Id: {session}\r\n\
             MCP-Protocol-Version: 2025-06-18\r\n\r\n"
        );
        sock.write_all(get.as_bytes()).await.unwrap();
        let mut head = Vec::new();
        let mut buf = [0u8; 1024];
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = sock.read(&mut buf).await.unwrap();
            assert!(n > 0, "sse stream closed before its headers");
            head.extend_from_slice(&buf[..n]);
        }
        let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
        assert!(head.starts_with("http/1.1 200"), "{head}");
        assert!(head.contains("text/event-stream"), "{head}");
        sock
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sigterm_closes_open_streams_and_deletes_builder_pods() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (api, seen) = apiserver().await;
        let broker = broker_at(
            Workspace::Local {
                dir: std::env::temp_dir(),
            },
            &api,
        );
        let resources = Resources::default();
        let opts = PodOpts {
            idle_nodes: &[],
            resources: &resources,
            cache: None,
            node: None,
            deadline_secs: 600,
            owner: None,
        };
        let pod = BuilderPod::create_named(
            broker.client.clone(),
            "builds",
            Backend::Buildah,
            "buildit-term",
            &opts,
        )
        .await
        .unwrap();
        broker
            .keep(Session {
                id: "buildit-term".to_string(),
                pod,
                sandbox: None,
            })
            .await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(serve_until(
            listener,
            broker,
            TOKEN.to_string(),
            allowed_hosts(None),
            shutdown_signal().unwrap(),
            Duration::from_secs(60),
        ));
        let mut sse = open_sse(&addr).await;

        let killed = std::process::Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .unwrap();
        assert!(killed.success());

        let deleted = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("server did not exit within 5s of SIGTERM")
            .unwrap()
            .unwrap();
        assert_eq!(deleted, ["buildit-term"]);
        assert_eq!(
            *seen.lock().unwrap(),
            ["POST pods", "DELETE builds/buildit-term"]
        );
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), sse.read_to_end(&mut rest))
            .await
            .expect("sse stream stayed open")
            .unwrap();
    }
}
