use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
use kube::api::{DeleteParams, ListParams};
use rmcp::handler::server::common::Extension;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ServerHandler, schemars, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::backend::{BACKEND_LABEL, Backend, PodMeta, PodOpts, Resources};
use crate::build::POD_READY_TIMEOUT;
use crate::buildlog::{
    LOG_DIR, LogName, LogPage, LogQuery, LogSelector, LogSummary, LogText, QueryParams, READ_LIMIT,
    alloc_run_argv, list_argv, note_argv, parse_list, parse_run_number, read_argv, step_argv,
    summarize,
};
use crate::pod::{BuilderPod, ExecStatus};
use crate::sandbox::{RESULTS_DIR, RelPath, SandboxName, Workspace};

pub const MCP_PATH: &str = "/mcp";
pub const SANDBOX_HEADER: &str = "x-crucible-sandbox";

const SANDBOX_HOSTS: [&str; 2] = ["host.containers.internal", "host.openshell.internal"];

// the caller identity when a local-dir server gets no sandbox header
const LOCAL_CALLER: &str = "local";

const LABEL_MANAGED_BY: &str = "buildit.dev/managed-by";
const MANAGED_BY: &str = "mcp";
const LABEL_BUILD_ID: &str = "buildit.dev/build-id";
const LABEL_SANDBOX: &str = "buildit.dev/sandbox";
const LABEL_STATE: &str = "buildit.dev/state";
const ANNOTATION_CONTEXT: &str = "buildit.dev/context";
const ANNOTATION_DOCKERFILE: &str = "buildit.dev/dockerfile";
const ANNOTATION_TARGET: &str = "buildit.dev/target";

const DEFAULT_BUILD_TIMEOUT_S: u64 = 1800;
const DEFAULT_RUN_TIMEOUT_S: u64 = 600;
const FROM_TIMEOUT_S: u64 = 300;
const EXEC_SLACK: Duration = Duration::from_secs(60);
const FETCH_LIMIT: u64 = 512 * 1024 * 1024;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

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
    /// Live builder pods per sandbox; `build` is refused past this
    #[arg(long, default_value_t = 4)]
    pub max_builds: usize,
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
        max_builds: args.max_builds.max(1),
        owner,
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
    serve_until(
        listener,
        broker,
        token,
        hosts,
        shutdown_signal()?,
        SHUTDOWN_GRACE,
    )
    .await
}

async fn serve_until(
    listener: tokio::net::TcpListener,
    broker: Arc<Broker>,
    token: String,
    hosts: Vec<String>,
    shutdown: impl Future<Output = ()>,
    grace: Duration,
) -> Result<()> {
    let ct = CancellationToken::new();
    let app = router(broker, token, hosts, ct.clone());
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(ct.clone().cancelled_owned())
        .into_future();
    tokio::pin!(server);
    let served = tokio::select! {
        result = &mut server => result,
        () = shutdown => {
            tracing::info!("shutting down; cancelling in-flight calls");
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
    served.context("serving buildit mcp")
}

pub fn router(
    broker: Arc<Broker>,
    token: String,
    hosts: Vec<String>,
    ct: CancellationToken,
) -> axum::Router {
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_sse_keep_alive(None)
        .with_sse_retry(None)
        .with_allowed_hosts(hosts)
        .with_cancellation_token(ct);
    let service: StreamableHttpService<BuilditMcp, NeverSessionManager> =
        StreamableHttpService::new(
            move || Ok(BuilditMcp::new(broker.clone())),
            Arc::new(NeverSessionManager::default()),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildId(String);

impl BuildId {
    fn fresh() -> Self {
        Self(crate::pod::unique_name())
    }

    fn parse(raw: &str) -> Result<Self> {
        let bytes = raw.as_bytes();
        let ok = raw.len() <= 63
            && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
            && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
            && bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-');
        if !ok {
            bail!("invalid build_id {raw:?}");
        }
        Ok(Self(raw.to_string()))
    }

    fn as_str(&self) -> &str {
        &self.0
    }

    fn tag(&self) -> String {
        local_tag(&self.0)
    }
}

impl fmt::Display for BuildId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn local_tag(id: &str) -> String {
    format!("localhost/buildit/{id}:latest")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildState {
    Building,
    Built,
    Failed,
}

impl BuildState {
    fn label(self) -> &'static str {
        match self {
            BuildState::Building => "building",
            BuildState::Built => "built",
            BuildState::Failed => "failed",
        }
    }

    fn from_label(raw: &str) -> Option<Self> {
        [BuildState::Building, BuildState::Built, BuildState::Failed]
            .into_iter()
            .find(|s| s.label() == raw)
    }
}

fn label<'a>(pod: &'a Pod, key: &str) -> Option<&'a str> {
    pod.metadata.labels.as_ref()?.get(key).map(String::as_str)
}

fn selector(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn owned_by(pod: &Pod, caller: &SandboxName) -> bool {
    label(pod, LABEL_MANAGED_BY) == Some(MANAGED_BY)
        && label(pod, LABEL_SANDBOX) == Some(caller.as_str())
}

fn alive(pod: &Pod) -> bool {
    pod.metadata.deletion_timestamp.is_none()
        && !matches!(
            pod.status.as_ref().and_then(|s| s.phase.as_deref()),
            Some("Succeeded" | "Failed")
        )
}

fn pod_name(pod: &Pod) -> &str {
    pod.metadata.name.as_deref().unwrap_or_default()
}

// oldest first, ties by name
fn within_cap(live: &[Pod], cap: usize, id: &BuildId) -> bool {
    let mut ranked: Vec<&Pod> = live.iter().collect();
    ranked.sort_by(|a, b| {
        let at = a.metadata.creation_timestamp.as_ref().map(|t| t.0);
        let bt = b.metadata.creation_timestamp.as_ref().map(|t| t.0);
        at.cmp(&bt).then_with(|| pod_name(a).cmp(pod_name(b)))
    });
    ranked
        .iter()
        .position(|p| label(p, LABEL_BUILD_ID) == Some(id.as_str()))
        .is_some_and(|i| i < cap)
}

fn pod_labels(caller: &SandboxName, id: &BuildId) -> Vec<(String, String)> {
    [
        (LABEL_MANAGED_BY, MANAGED_BY),
        (LABEL_SANDBOX, caller.as_str()),
        (LABEL_BUILD_ID, id.as_str()),
        (LABEL_STATE, BuildState::Building.label()),
    ]
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .to_vec()
}

struct LiveBuild {
    id: BuildId,
    pod: BuilderPod,
    backend: Backend,
    state: Option<BuildState>,
}

impl LiveBuild {
    fn runnable(&self) -> bool {
        self.state == Some(BuildState::Built) && self.backend == Backend::Buildah
    }
}

pub struct Broker {
    client: kube::Client,
    namespace: String,
    workspace: Workspace,
    backend: Backend,
    resources: Resources,
    deadline_secs: i64,
    max_builds: usize,
    owner: Option<OwnerReference>,
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
pub struct LogsParams {
    /// build_id from build
    pub build_id: String,
    /// "build", "run:<n>", or "latest" (default: the newest run, else the build)
    #[serde(default)]
    pub which: Option<String>,
    /// Only the last N selected lines
    #[serde(default)]
    pub tail_lines: Option<u64>,
    /// Only the first N selected lines
    #[serde(default)]
    pub head_lines: Option<u64>,
    /// First line number to consider (1-based, inclusive)
    #[serde(default)]
    pub from_line: Option<u64>,
    /// Last line number to consider (inclusive)
    #[serde(default)]
    pub to_line: Option<u64>,
    /// Regex (Rust syntax; prefix (?i) to ignore case); keeps matching lines
    #[serde(default)]
    pub grep: Option<String>,
    /// Lines of context around each grep match (max 20)
    #[serde(default)]
    pub context_lines: Option<u64>,
    /// Output cap in bytes (default 16384, 2048..=65536)
    #[serde(default)]
    pub max_bytes: Option<u64>,
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
    pub log: LogSummary,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RunReply {
    pub exit: Option<i32>,
    pub timed_out: bool,
    pub log: LogSummary,
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

    fn annotations(&self) -> Vec<(String, String)> {
        let mut out = vec![
            (
                ANNOTATION_CONTEXT.to_string(),
                self.context.as_str().to_string(),
            ),
            (
                ANNOTATION_DOCKERFILE.to_string(),
                self.dockerfile.as_str().to_string(),
            ),
        ];
        if let Some(t) = &self.target {
            out.push((ANNOTATION_TARGET.to_string(), t.clone()));
        }
        out
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

fn run_argv(ctr: &str, cmd: &[String]) -> Vec<String> {
    let mut argv = ["buildah", "run", ctr, "--"].map(String::from).to_vec();
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

fn results_path(id: &BuildId, file: &str) -> String {
    format!("{RESULTS_DIR}/{id}/{file}")
}

// exit code and timeout flag from a step run under `timeout`
fn step_outcome(status: Result<Result<ExecStatus>, tokio::time::error::Elapsed>) -> StepOutcome {
    match status {
        Ok(Ok(s)) => StepOutcome {
            exit: s.code,
            timed_out: s.code == Some(124),
            error: None,
        },
        Ok(Err(e)) => StepOutcome {
            exit: None,
            timed_out: false,
            error: Some(format!("buildit: {e:#}")),
        },
        Err(_) => StepOutcome {
            exit: None,
            timed_out: true,
            error: Some("buildit: timed out waiting for the builder".to_string()),
        },
    }
}

struct StepOutcome {
    exit: Option<i32>,
    timed_out: bool,
    error: Option<String>,
}

impl StepOutcome {
    fn success(&self) -> bool {
        self.exit == Some(0)
    }
}

impl Broker {
    fn caller(&self, parts: &Parts) -> Result<SandboxName> {
        let raw = parts
            .headers
            .get(SANDBOX_HEADER)
            .map(|v| v.to_str().context("sandbox header is not ascii"))
            .transpose()?;
        match (raw, self.workspace.needs_sandbox()) {
            (Some(raw), _) => SandboxName::parse(raw),
            (None, true) => bail!("request carries no {SANDBOX_HEADER} header; refusing"),
            (None, false) => SandboxName::parse(LOCAL_CALLER),
        }
    }

    fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    async fn list(&self, pairs: &[(&str, &str)]) -> Result<Vec<Pod>> {
        let sel = selector(pairs);
        Ok(self
            .pods()
            .list(&ListParams::default().labels(&sel))
            .await
            .with_context(|| format!("listing builder pods ({sel})"))?
            .items)
    }

    async fn live_builds(&self, caller: &SandboxName) -> Result<Vec<Pod>> {
        let pods = self
            .list(&[
                (LABEL_MANAGED_BY, MANAGED_BY),
                (LABEL_SANDBOX, caller.as_str()),
            ])
            .await?;
        Ok(pods
            .into_iter()
            .filter(|p| alive(p) && owned_by(p, caller))
            .collect())
    }

    async fn check_cap(&self, caller: &SandboxName, me: Option<&BuildId>) -> Result<()> {
        let live = self.live_builds(caller).await?;
        let ok = match me {
            None => live.len() < self.max_builds,
            Some(id) => within_cap(&live, self.max_builds, id),
        };
        if ok {
            return Ok(());
        }
        let ids: Vec<&str> = live
            .iter()
            .filter_map(|p| label(p, LABEL_BUILD_ID))
            .filter(|id| me.is_none_or(|me| me.as_str() != *id))
            .collect();
        bail!(
            "this sandbox has {} live builds (max {}): {}; clean one first",
            ids.len(),
            self.max_builds,
            ids.join(", ")
        )
    }

    // pods of other sandboxes look exactly like missing ones
    async fn find(&self, caller: &SandboxName, id: &BuildId) -> Result<LiveBuild> {
        let gone = || anyhow!("no live build {id:?}; build again");
        let pods = self
            .list(&[
                (LABEL_MANAGED_BY, MANAGED_BY),
                (LABEL_BUILD_ID, id.as_str()),
            ])
            .await?;
        let pod = pods
            .into_iter()
            .find(|p| owned_by(p, caller) && p.metadata.deletion_timestamp.is_none())
            .ok_or_else(gone)?;
        if !alive(&pod) {
            bail!("build {id} expired (builder pod deadline); build again");
        }
        let backend = label(&pod, BACKEND_LABEL)
            .and_then(Backend::from_label)
            .ok_or_else(gone)?;
        Ok(LiveBuild {
            id: id.clone(),
            pod: BuilderPod::existing(self.client.clone(), &self.namespace, pod_name(&pod)),
            backend,
            state: label(&pod, LABEL_STATE).and_then(BuildState::from_label),
        })
    }

    async fn publish(&self, caller: &SandboxName, results: &Path) -> Result<()> {
        let ws = &self.workspace;
        tokio::task::block_in_place(|| ws.publish(caller, results))
    }

    async fn note(&self, pod: &BuilderPod, backend: Backend, log: LogName, line: &str) {
        if let Err(e) = pod
            .exec_capture(&note_argv(backend.shell(), log, line))
            .await
        {
            tracing::warn!("writing to {log} log in {}: {e:#}", pod.name);
        }
    }

    async fn read_log(&self, pod: &BuilderPod, backend: Backend, log: LogName) -> Result<LogText> {
        let out = pod
            .exec_capture_bytes(&read_argv(backend.shell(), log), READ_LIMIT + 64)
            .await
            .with_context(|| format!("reading the {log} log"))?;
        LogText::parse(out)
    }

    // pulls the pod's log into results/<file> and summarizes it
    async fn collect_log(
        &self,
        pod: &BuilderPod,
        backend: Backend,
        log: LogName,
        id: &BuildId,
        results: &Path,
        failed: bool,
    ) -> Result<LogSummary> {
        let text = match self.read_log(pod, backend, log).await {
            Ok(text) => text,
            Err(e) => LogText::whole(format!("buildit: {e:#}\n").into_bytes()),
        };
        let file = log.file_name();
        std::fs::write(results.join(&file), text.body())
            .with_context(|| format!("writing {file}"))?;
        summarize(&text, log, results_path(id, &file), failed)
    }

    pub async fn build(&self, caller: &SandboxName, params: BuildParams) -> Result<BuildReply> {
        let req = BuildRequest::parse(params)?;
        self.check_cap(caller, None).await?;
        let id = BuildId::fresh();
        let stage = tempfile::tempdir().context("creating staging dir")?;
        let ctx_dir = stage.path().join("ctx");
        let results = stage.path().join(id.as_str());
        std::fs::create_dir_all(&results).context("creating results dir")?;

        let ws = &self.workspace;
        tokio::task::block_in_place(|| ws.fetch_context(caller, &req.context, &ctx_dir))?;
        let tarball = crate::context::tarball(&ctx_dir)?;
        tracing::info!(
            "build {id}: sandbox {} context {} ({} KiB)",
            caller.as_str(),
            req.context.as_str(),
            tarball.len() / 1024
        );

        let meta = PodMeta {
            labels: pod_labels(caller, &id),
            annotations: req.annotations(),
            log_dir: Some(LOG_DIR.to_string()),
        };
        let opts = PodOpts {
            idle_nodes: &[],
            resources: &self.resources,
            cache: None,
            node: None,
            deadline_secs: self.deadline_secs,
            owner: self.owner.as_ref(),
            meta: Some(&meta),
        };
        let pod = BuilderPod::create_named(
            self.client.clone(),
            &self.namespace,
            self.backend,
            id.as_str(),
            &opts,
        )
        .await?;
        let ready = async {
            self.check_cap(caller, Some(&id)).await?;
            pod.wait_ready(POD_READY_TIMEOUT).await
        };
        if let Err(e) = ready.await {
            if let Err(del) = pod.delete().await {
                tracing::warn!("deleting builder pod {id}: {del:#}");
            }
            return Err(e);
        }

        let outcome = step_outcome(
            tokio::time::timeout(
                req.timeout + EXEC_SLACK,
                self.drive_build(&pod, &req, &tarball),
            )
            .await,
        );
        if let Some(line) = &outcome.error {
            self.note(&pod, self.backend, LogName::Build, line).await;
        }
        let state = if outcome.success() {
            BuildState::Built
        } else {
            BuildState::Failed
        };
        let labelled = pod.set_labels(&[(LABEL_STATE, state.label())]).await;
        if let Err(e) = &labelled {
            tracing::warn!("build {id}: {e:#}");
        }
        let log = self
            .collect_log(
                &pod,
                self.backend,
                LogName::Build,
                &id,
                &results,
                !outcome.success(),
            )
            .await?;
        self.publish(caller, &results).await?;
        Ok(BuildReply {
            runnable: outcome.success() && labelled.is_ok() && self.backend == Backend::Buildah,
            build_id: id.to_string(),
            exit: outcome.exit,
            timed_out: outcome.timed_out,
            log,
        })
    }

    async fn drive_build(
        &self,
        pod: &BuilderPod,
        req: &BuildRequest,
        tarball: &[u8],
    ) -> Result<ExecStatus> {
        let backend = self.backend;
        let started = Instant::now();
        pod.exec_capture(&backend.setup_command()).await?;
        pod.exec_with_stdin(&backend.untar_command(), tarball)
            .await?;
        let steps = backend.local_build_steps(
            &local_tag(pod.name.as_str()),
            req.dockerfile.as_str(),
            req.target.as_deref(),
            &req.build_args,
        );
        let mut last = None;
        for step in &steps {
            let left = req.timeout.saturating_sub(started.elapsed()).as_secs();
            let status = pod
                .exec_status(&step_argv(backend.shell(), LogName::Build, left, step))
                .await?;
            if !status.success() {
                return Ok(status);
            }
            last = Some(status);
        }
        last.ok_or_else(|| anyhow!("backend produced no build steps"))
    }

    pub async fn run(&self, caller: &SandboxName, params: RunParams) -> Result<RunReply> {
        if params.cmd.is_empty() {
            bail!("cmd must not be empty");
        }
        let id = BuildId::parse(&params.build_id)?;
        let paths = params
            .fetch_paths
            .iter()
            .map(|p| RelPath::parse_container(p))
            .collect::<Result<Vec<_>>>()?;
        let build = self.find(caller, &id).await?;
        if !build.runnable() {
            bail!(
                "build {id} is not runnable (state {}); see logs(which: \"build\")",
                build.state.map_or("unknown", BuildState::label)
            );
        }
        let limit = timeout(params.timeout_s, DEFAULT_RUN_TIMEOUT_S);
        let stage = tempfile::tempdir().context("creating staging dir")?;
        let results = stage.path().join(id.as_str());
        std::fs::create_dir_all(&results).context("creating results dir")?;

        let pod = &build.pod;
        let shell = build.backend.shell();
        let n = parse_run_number(&pod.exec_capture(&alloc_run_argv(shell)).await?)?;
        let log = LogName::Run(n);
        let ctr = format!("{id}-run{n}");
        let from = step_outcome(
            tokio::time::timeout(
                Duration::from_secs(FROM_TIMEOUT_S) + EXEC_SLACK,
                pod.exec_status(&step_argv(
                    shell,
                    log,
                    FROM_TIMEOUT_S,
                    &from_argv(&id.tag(), &ctr),
                )),
            )
            .await,
        );
        let outcome = if from.success() {
            step_outcome(
                tokio::time::timeout(
                    limit + EXEC_SLACK,
                    pod.exec_status(&step_argv(
                        shell,
                        log,
                        limit.as_secs(),
                        &run_argv(&ctr, &params.cmd),
                    )),
                )
                .await,
            )
        } else {
            from
        };
        if let Some(line) = &outcome.error {
            self.note(pod, build.backend, log, line).await;
        }

        let fetched = if paths.is_empty() || outcome.exit.is_none() {
            Ok(Fetched::default())
        } else {
            self.fetch(pod, &ctr, &paths, &results).await
        };
        if let Err(e) = pod.exec_capture(&rm_argv(&ctr)).await {
            tracing::warn!("removing container {ctr}: {e:#}");
        }
        let fetched = fetched?;
        let summary = self
            .collect_log(pod, build.backend, log, &id, &results, !outcome.success())
            .await?;
        self.publish(caller, &results).await?;
        Ok(RunReply {
            exit: outcome.exit,
            timed_out: outcome.timed_out,
            log: summary,
            fetched: fetched
                .files
                .iter()
                .map(|f| results_path(&build.id, f))
                .collect(),
            missing: fetched.missing,
            skipped: fetched.skipped,
        })
    }

    async fn fetch(
        &self,
        pod: &BuilderPod,
        ctr: &str,
        paths: &[RelPath],
        results: &Path,
    ) -> Result<Fetched> {
        let tar = pod
            .exec_capture_bytes(&fetch_argv(ctr, paths), FETCH_LIMIT)
            .await
            .context("fetching paths from the container")?;
        tokio::task::block_in_place(|| unpack_fetched(&tar, results, paths))
    }

    pub async fn logs(&self, caller: &SandboxName, p: LogsParams) -> Result<LogPage> {
        let id = BuildId::parse(&p.build_id)?;
        let which = LogSelector::parse(p.which.as_deref().unwrap_or("latest"))?;
        let query = LogQuery::new(QueryParams {
            from_line: p.from_line,
            to_line: p.to_line,
            head_lines: p.head_lines,
            tail_lines: p.tail_lines,
            grep: p.grep.as_deref(),
            context_lines: p.context_lines,
            max_bytes: p.max_bytes,
        })?;
        let build = self.find(caller, &id).await?;
        let shell = build.backend.shell();
        let available = parse_list(&build.pod.exec_capture(&list_argv(shell)).await?);
        let log = which.resolve(&available)?;
        let text = self.read_log(&build.pod, build.backend, log).await?;
        Ok(query.run(&text, log, &available))
    }

    pub async fn clean(&self, caller: &SandboxName, id: Option<&str>) -> Result<Vec<String>> {
        let id = id.map(BuildId::parse).transpose()?;
        let mut pairs = vec![
            (LABEL_MANAGED_BY, MANAGED_BY),
            (LABEL_SANDBOX, caller.as_str()),
        ];
        if let Some(id) = &id {
            pairs.push((LABEL_BUILD_ID, id.as_str()));
        }
        let mut deleted = Vec::new();
        for pod in self.list(&pairs).await? {
            if !owned_by(&pod, caller) || pod.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let name = pod_name(&pod);
            match self.pods().delete(name, &DeleteParams::default()).await {
                Ok(_) => deleted.push(label(&pod, LABEL_BUILD_ID).unwrap_or(name).to_string()),
                Err(e) => tracing::warn!("deleting builder pod {name}: {e:#}"),
            }
        }
        deleted.sort();
        Ok(deleted)
    }
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
        description = "Build a Dockerfile from a workspace subdirectory on a remote builder; pushes \
        nothing. Returns build_id, exit, runnable, and log: total_lines, the last 40 lines and, on \
        failure, the first error-looking lines (`N:text`). Query more with `logs`; a copy lands at \
        .buildit/<build_id>/build.log. The builder stays up for `run` and `logs` until `clean`."
    )]
    async fn build(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(params): Parameters<BuildParams>,
    ) -> Result<String, String> {
        let broker = &self.broker;
        reply(
            async {
                let caller = broker.caller(&parts)?;
                broker.build(&caller, params).await
            }
            .await,
        )
    }

    #[tool(
        description = "Run a command in a fresh container of a runnable build. Returns exit and a \
        log summary like build's, named run:<n> (copy at .buildit/<build_id>/run-<n>.log); \
        fetch_paths are copied out of the container into .buildit/<build_id>/<path>."
    )]
    async fn run(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(params): Parameters<RunParams>,
    ) -> Result<String, String> {
        let broker = &self.broker;
        reply(
            async {
                let caller = broker.caller(&parts)?;
                broker.run(&caller, params).await
            }
            .await,
        )
    }

    #[tool(
        description = "Read part of a build or run log without pulling all of it. Select lines with \
        from_line/to_line, then grep (regex, with context_lines), then head_lines or tail_lines; \
        output is capped at max_bytes. Lines come back as `N:text` (`N-text` for context, `--` \
        between gaps) with total_lines, matched, shown [first, last] and truncated. Without \
        head_lines or from_line the cut keeps the end."
    )]
    async fn logs(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(params): Parameters<LogsParams>,
    ) -> Result<String, String> {
        let broker = &self.broker;
        reply(
            async {
                let caller = broker.caller(&parts)?;
                broker.logs(&caller, params).await
            }
            .await,
        )
    }

    #[tool(description = "Delete a build's builder pod, or all of this sandbox's builds.")]
    async fn clean(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(params): Parameters<CleanParams>,
    ) -> Result<String, String> {
        let broker = &self.broker;
        reply(
            async {
                let caller = broker.caller(&parts)?;
                let deleted = broker.clean(&caller, params.build_id.as_deref()).await?;
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
    use std::time::Duration;

    use k8s_openapi::api::core::v1::Pod;
    use rmcp::ServiceExt;
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::sync::CancellationToken;

    use crate::backend::{Backend, Resources};
    use crate::mcp::{
        Broker, BuildId, BuildParams, BuildRequest, LABEL_BUILD_ID, MCP_PATH, SANDBOX_HEADER,
        alive, allowed_hosts, authorized, constant_time_eq, fetch_argv, from_argv, kube_client,
        owned_by, rm_argv, router, run_argv, selector, serve_until, unpack_fetched, within_cap,
    };
    use crate::sandbox::{RelPath, SandboxName, Workspace};

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
        assert_eq!(
            req.annotations(),
            [
                ("buildit.dev/context".to_string(), "svc".to_string()),
                (
                    "buildit.dev/dockerfile".to_string(),
                    "Dockerfile".to_string()
                ),
                ("buildit.dev/target".to_string(), "builder".to_string()),
            ]
        );
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
            from_argv("localhost/buildit/b1:latest", "b1-run1"),
            [
                "buildah",
                "from",
                "--pull-never",
                "--name",
                "b1-run1",
                "localhost/buildit/b1:latest"
            ]
        );
        let cmd = vec![
            "sh".to_string(),
            "-c".to_string(),
            "echo hi; exit 3".to_string(),
        ];
        assert_eq!(
            run_argv("b1-run1", &cmd),
            [
                "buildah",
                "run",
                "b1-run1",
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
        let fetch = fetch_argv("b1-run1", &paths);
        assert_eq!(fetch[..4], ["buildah", "unshare", "sh", "-c"]);
        assert_eq!(fetch[5..], ["sh", "b1-run1", "out/app", "etc/os-release"]);
        assert_eq!(rm_argv("b1-run1"), ["buildah", "rm", "b1-run1"]);
    }

    #[test]
    fn build_ids_cannot_smuggle_selectors() {
        let fresh = BuildId::fresh();
        assert_eq!(BuildId::parse(fresh.as_str()).unwrap(), fresh);
        assert!(BuildId::parse("buildit-0a1b2c3d").is_ok());
        for bad in [
            "",
            "Buildit-1",
            "-x",
            "x-",
            "a,buildit.dev/sandbox=other",
            "a=b",
            "a b",
            "a/b",
            "a.b",
            &"x".repeat(64),
        ] {
            assert!(BuildId::parse(bad).is_err(), "{bad:?}");
        }
        assert_eq!(selector(&[("a/b", "c"), ("d", "e")]), "a/b=c,d=e");
        assert!(Path::new(&crate::mcp::local_tag("buildit-1")).starts_with("localhost"));
    }

    fn pod(id: &str, sandbox: &str, created: &str, phase: Option<&str>) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {
                "name": id,
                "creationTimestamp": created,
                "labels": {
                    "buildit.dev/managed-by": "mcp",
                    "buildit.dev/sandbox": sandbox,
                    "buildit.dev/build-id": id,
                }
            },
            "status": { "phase": phase }
        }))
        .unwrap()
    }

    #[test]
    fn pods_belong_to_one_sandbox_and_rank_for_the_cap() {
        let a = SandboxName::parse("sb-a").unwrap();
        let b = SandboxName::parse("sb-b").unwrap();
        let p = pod("buildit-1", "sb-a", "2026-10-01T10:00:00Z", Some("Running"));
        assert!(owned_by(&p, &a));
        assert!(!owned_by(&p, &b));
        let mut unmanaged = p.clone();
        unmanaged
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("buildit.dev/managed-by".to_string(), "cli".to_string());
        assert!(!owned_by(&unmanaged, &a));
        let mut bare = p.clone();
        bare.metadata.labels = None;
        assert!(!owned_by(&bare, &a));

        assert!(alive(&p));
        assert!(alive(&pod(
            "x",
            "sb-a",
            "2026-10-01T10:00:00Z",
            Some("Pending")
        )));
        assert!(alive(&pod("x", "sb-a", "2026-10-01T10:00:00Z", None)));
        assert!(!alive(&pod(
            "x",
            "sb-a",
            "2026-10-01T10:00:00Z",
            Some("Failed")
        )));
        assert!(!alive(&pod(
            "x",
            "sb-a",
            "2026-10-01T10:00:00Z",
            Some("Succeeded")
        )));
        let mut going = p.clone();
        going.metadata.deletion_timestamp = going.metadata.creation_timestamp.clone();
        assert!(!alive(&going));

        let live = [
            pod("buildit-c", "sb-a", "2026-10-01T10:00:05Z", None),
            pod("buildit-b", "sb-a", "2026-10-01T10:00:00Z", None),
            pod("buildit-a", "sb-a", "2026-10-01T10:00:00Z", None),
        ];
        let id = |s: &str| BuildId::parse(s).unwrap();
        assert!(within_cap(&live, 2, &id("buildit-a")));
        assert!(within_cap(&live, 2, &id("buildit-b")));
        assert!(!within_cap(&live, 2, &id("buildit-c")));
        assert!(within_cap(&live, 3, &id("buildit-c")));
        assert!(!within_cap(&live, 3, &id("buildit-z")));
        assert_eq!(LABEL_BUILD_ID, "buildit.dev/build-id");
    }

    fn broker(workspace: Workspace) -> Arc<Broker> {
        let config = kube::Config::new("http://127.0.0.1:9".parse().unwrap());
        Arc::new(Broker {
            client: kube::Client::try_from(config).unwrap(),
            namespace: "builds".to_string(),
            workspace,
            backend: Backend::Buildah,
            resources: Resources::default(),
            deadline_secs: 600,
            max_builds: 2,
            owner: None,
        })
    }

    fn parts(sandbox: Option<&str>) -> axum::http::request::Parts {
        let mut req = axum::http::Request::builder();
        if let Some(s) = sandbox {
            req = req.header(SANDBOX_HEADER, s);
        }
        req.body(()).unwrap().into_parts().0
    }

    #[tokio::test]
    async fn caller_identity_comes_from_one_place() {
        let local = broker(Workspace::Local {
            dir: std::env::temp_dir(),
        });
        assert_eq!(local.caller(&parts(None)).unwrap().as_str(), "local");
        assert_eq!(local.caller(&parts(Some("sb-a"))).unwrap().as_str(), "sb-a");
        assert!(local.caller(&parts(Some("--x"))).is_err());
        let shell = broker(Workspace::Openshell {
            workdir: "/sandbox".to_string(),
        });
        let err = shell.caller(&parts(None)).unwrap_err().to_string();
        assert!(err.contains(SANDBOX_HEADER), "{err}");
        assert_eq!(shell.caller(&parts(Some("sb-a"))).unwrap().as_str(), "sb-a");
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

    struct Reply {
        status: u16,
        head: String,
        body: String,
    }

    async fn exchange(addr: &str, req: String) -> Reply {
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        tokio::time::timeout(Duration::from_secs(600), sock.read_to_end(&mut out))
            .await
            .expect("response did not finish")
            .unwrap();
        let out = String::from_utf8_lossy(&out).into_owned();
        let (head, body) = out.split_once("\r\n\r\n").unwrap_or((&out, ""));
        Reply {
            status: head.split_whitespace().nth(1).unwrap().parse().unwrap(),
            head: head.to_ascii_lowercase(),
            body: body.to_string(),
        }
    }

    fn http(method: &str, host: &str, auth: Option<&str>, extra: &str, body: &str) -> String {
        let auth = auth
            .map(|a| format!("Authorization: {a}\r\n"))
            .unwrap_or_default();
        format!(
            "{method} {MCP_PATH} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n{auth}{extra}Content-Length: {}\r\n\
             Connection: close\r\n\r\n{body}",
            body.len()
        )
    }

    const INIT: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}"#;

    #[tokio::test(flavor = "multi_thread")]
    async fn http_rejects_missing_or_wrong_token_and_foreign_hosts() {
        let addr = serve(Workspace::Local {
            dir: std::env::temp_dir(),
        })
        .await;
        let bearer = format!("Bearer {TOKEN}");
        let status = |host: &'static str, auth: Option<String>| {
            let addr = addr.clone();
            async move {
                exchange(&addr, http("POST", host, auth.as_deref(), "", INIT))
                    .await
                    .status
            }
        };
        assert_eq!(status("127.0.0.1", None).await, 401);
        assert_eq!(status("127.0.0.1", Some("Bearer nope".into())).await, 401);
        assert_eq!(status("127.0.0.1", Some(TOKEN.into())).await, 401);
        assert_eq!(status("evil.example", Some(bearer.clone())).await, 403);
        assert_eq!(
            status("host.openshell.internal:8849", Some(bearer.clone())).await,
            200
        );
        assert_eq!(
            status("host.containers.internal", Some(bearer.clone())).await,
            200
        );
        assert_eq!(status("127.0.0.1", Some(bearer)).await, 200);
    }

    fn jsonrpc(id: u32, method: &str, params: serde_json::Value) -> String {
        serde_json::json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
            .to_string()
    }

    fn tool_text(reply: &Reply) -> (bool, String) {
        let v: serde_json::Value = serde_json::from_str(&reply.body).unwrap();
        let result = &v["result"];
        (
            result["isError"].as_bool().unwrap_or(false),
            result["content"][0]["text"].as_str().unwrap().to_string(),
        )
    }

    // a tools/call with no initialize and no session header, one POST
    async fn raw_call(
        addr: &str,
        sandbox: &str,
        name: &str,
        args: serde_json::Value,
    ) -> (bool, String) {
        let body = jsonrpc(
            7,
            "tools/call",
            serde_json::json!({ "name": name, "arguments": args }),
        );
        let reply = exchange(
            addr,
            http(
                "POST",
                "127.0.0.1",
                Some(&format!("Bearer {TOKEN}")),
                &format!("{SANDBOX_HEADER}: {sandbox}\r\n"),
                &body,
            ),
        )
        .await;
        assert_eq!(reply.status, 200, "{}", reply.body);
        assert!(!reply.head.contains("mcp-session-id"), "{}", reply.head);
        assert!(
            reply.head.contains("content-type: application/json"),
            "{}",
            reply.head
        );
        tool_text(&reply)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stateless_posts_need_no_session_and_get_json() {
        let addr = serve(Workspace::Local {
            dir: std::env::temp_dir(),
        })
        .await;
        let bearer = format!("Bearer {TOKEN}");

        let init = exchange(&addr, http("POST", "127.0.0.1", Some(&bearer), "", INIT)).await;
        assert_eq!(init.status, 200);
        assert!(!init.head.contains("mcp-session-id"), "{}", init.head);
        assert!(
            init.head.contains("content-type: application/json"),
            "{}",
            init.head
        );
        assert!(init.body.contains("\"buildit\""), "{}", init.body);

        // two unrelated POSTs, neither initialized nor carrying a session
        for id in [2, 3] {
            let list = exchange(
                &addr,
                http(
                    "POST",
                    "127.0.0.1",
                    Some(&bearer),
                    "",
                    &jsonrpc(id, "tools/list", serde_json::json!({})),
                ),
            )
            .await;
            assert_eq!(list.status, 200, "{}", list.body);
            assert!(!list.head.contains("mcp-session-id"), "{}", list.head);
            assert!(!list.head.contains("text/event-stream"), "{}", list.head);
            let v: serde_json::Value = serde_json::from_str(&list.body).unwrap();
            assert_eq!(v["id"], id);
            let mut names: Vec<&str> = v["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap())
                .collect();
            names.sort();
            assert_eq!(names, ["build", "clean", "logs", "run"]);
        }

        let (is_error, text) = raw_call(
            &addr,
            "sb-a",
            "logs",
            serde_json::json!({ "build_id": "Nope,x=y" }),
        )
        .await;
        assert!(is_error);
        assert!(text.contains("invalid build_id"), "{text}");

        for method in ["GET", "DELETE"] {
            let r = exchange(&addr, http(method, "127.0.0.1", Some(&bearer), "", "")).await;
            assert_eq!(r.status, 405, "{method}: {}", r.body);
        }
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

    type Client = rmcp::service::RunningService<rmcp::RoleClient, ()>;

    async fn call_ok(c: &Client, name: &'static str, args: serde_json::Value) -> serde_json::Value {
        let r = c.call_tool(call(name, args)).await.unwrap();
        assert_ne!(r.is_error, Some(true), "{name}: {}", text(&r));
        serde_json::from_str(&text(&r)).unwrap()
    }

    async fn call_err(c: &Client, name: &'static str, args: serde_json::Value) -> String {
        let r = c.call_tool(call(name, args)).await.unwrap();
        assert_eq!(r.is_error, Some(true), "{name}: {}", text(&r));
        text(&r)
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
        assert_eq!(names, ["build", "clean", "logs", "run"]);
        let keys = |name: &str| {
            let t = tools.iter().find(|t| t.name == name).unwrap();
            let mut keys: Vec<_> = t.input_schema["properties"]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect();
            keys.sort();
            keys
        };
        assert_eq!(
            keys("build"),
            ["build_args", "context", "dockerfile", "target", "timeout_s"]
        );
        assert_eq!(
            keys("logs"),
            [
                "build_id",
                "context_lines",
                "from_line",
                "grep",
                "head_lines",
                "max_bytes",
                "tail_lines",
                "to_line",
                "which"
            ]
        );
        let schemas = serde_json::to_string(&tools).unwrap();
        assert!(!schemas.to_lowercase().contains("secret"), "{schemas}");

        let e = call_err(&c, "build", serde_json::json!({ "context": "../escape" })).await;
        assert!(e.contains("`..`"), "{e}");
        let e = call_err(&c, "build", serde_json::json!({ "context": "." })).await;
        assert!(e.contains("subdirectory"), "{e}");
        let e = call_err(
            &c,
            "run",
            serde_json::json!({ "build_id": "x,y", "cmd": ["true"] }),
        )
        .await;
        assert!(e.contains("invalid build_id"), "{e}");
        let e = call_err(
            &c,
            "run",
            serde_json::json!({ "build_id": "buildit-1", "cmd": [] }),
        )
        .await;
        assert!(e.contains("cmd must not be empty"), "{e}");
        let e = call_err(
            &c,
            "logs",
            serde_json::json!({ "build_id": "buildit-1", "which": "run:0" }),
        )
        .await;
        assert!(e.contains("which must be"), "{e}");
        let e = call_err(
            &c,
            "logs",
            serde_json::json!({ "build_id": "buildit-1", "head_lines": 1, "tail_lines": 1 }),
        )
        .await;
        assert!(e.contains("not both"), "{e}");
        let e = call_err(
            &c,
            "logs",
            serde_json::json!({ "build_id": "buildit-1", "grep": "(" }),
        )
        .await;
        assert!(e.contains("invalid grep"), "{e}");
        let e = call_err(&c, "clean", serde_json::json!({ "build_id": "A" })).await;
        assert!(e.contains("invalid build_id"), "{e}");
        c.cancel().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openshell_mode_refuses_requests_without_a_sandbox_header() {
        let addr = serve(Workspace::Openshell {
            workdir: "/sandbox/repo".to_string(),
        })
        .await;
        let c = client(&addr, TOKEN, None).await.unwrap();
        let e = call_err(&c, "build", serde_json::json!({ "context": "svc" })).await;
        assert!(e.contains(SANDBOX_HEADER), "{e}");
        c.cancel().await.unwrap();

        let c = client(&addr, TOKEN, Some("--gateway=evil")).await.unwrap();
        let e = call_err(&c, "clean", serde_json::json!({})).await;
        assert!(e.contains("invalid sandbox name"), "{e}");
        c.cancel().await.unwrap();
    }

    struct Server {
        addr: String,
        stop: tokio::sync::oneshot::Sender<()>,
        task: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    async fn start(broker: Arc<Broker>, grace: Duration) -> Server {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve_until(
            listener,
            broker,
            TOKEN.to_string(),
            allowed_hosts(None),
            async move {
                let _ = stopped.await;
            },
            grace,
        ));
        Server { addr, stop, task }
    }

    impl Server {
        async fn shut_down(self) {
            self.stop.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), self.task)
                .await
                .expect("server did not stop within 5s")
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_does_not_wait_on_idle_connections() {
        let server = start(
            broker(Workspace::Local {
                dir: std::env::temp_dir(),
            }),
            Duration::from_secs(60),
        )
        .await;
        // a keep-alive connection that already got its answer
        let mut idle = tokio::net::TcpStream::connect(&server.addr).await.unwrap();
        let req = http(
            "POST",
            "127.0.0.1",
            Some(&format!("Bearer {TOKEN}")),
            "",
            INIT,
        )
        .replace("Connection: close\r\n", "");
        idle.write_all(req.as_bytes()).await.unwrap();
        let mut buf = [0u8; 4096];
        let n = idle.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
        server.shut_down().await;
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), idle.read_to_end(&mut rest))
            .await
            .expect("idle connection stayed open")
            .unwrap();
    }

    #[cfg(unix)]
    fn tar_with(build: impl FnOnce(&mut tar::Builder<Vec<u8>>)) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        build(&mut b);
        b.into_inner().unwrap()
    }

    #[cfg(unix)]
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

    async fn cluster_broker(dir: &Path, max_builds: usize) -> Arc<Broker> {
        let ctx = std::env::var("BUILDIT_E2E_KUBECONTEXT")
            .expect("set BUILDIT_E2E_KUBECONTEXT to a disposable cluster's context");
        let ns = std::env::var("BUILDIT_E2E_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let (client, namespace, _) = kube_client(Some(&ctx), Some(&ns)).await.unwrap();
        Arc::new(Broker {
            client,
            namespace,
            workspace: Workspace::Local {
                dir: dir.to_path_buf(),
            },
            backend: Backend::Buildah,
            resources: Resources::default(),
            deadline_secs: 900,
            max_builds,
            owner: None,
        })
    }

    fn write(path: &Path, body: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    fn json(text: &str) -> serde_json::Value {
        serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a cluster: BUILDIT_E2E_KUBECONTEXT=kind-x cargo test -- --ignored"]
    async fn e2e_builds_survive_a_restart_and_stay_in_their_sandbox() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().canonicalize().unwrap();
        write(
            &dir.join("svc/Dockerfile"),
            "FROM docker.io/library/busybox:latest\n\
             RUN echo hello-from-build && mkdir -p /out && echo artifact > /out/a.txt\n",
        );
        write(
            &dir.join("bad/Dockerfile"),
            "FROM docker.io/library/busybox:latest\n\
             RUN echo 'main.c:3: undefined reference to frob' && exit 3\n",
        );

        let first = start(cluster_broker(&dir, 2).await, Duration::from_secs(5)).await;
        let a = client(&first.addr, TOKEN, Some("sb-a")).await.unwrap();
        let built = call_ok(&a, "build", serde_json::json!({ "context": "svc" })).await;
        assert_eq!(built["exit"], 0, "{built}");
        assert_eq!(built["runnable"], true, "{built}");
        assert_eq!(built["timed_out"], false);
        assert_eq!(built["log"]["log"], "build");
        assert!(built["log"].get("errors").is_none(), "{built}");
        assert!(
            built["log"]["tail"]
                .as_str()
                .unwrap()
                .contains("hello-from-build"),
            "{built}"
        );
        let id = built["build_id"].as_str().unwrap().to_string();
        assert_eq!(
            built["log"]["path"],
            format!(".buildit/{id}/build.log").as_str()
        );
        assert!(dir.join(format!(".buildit/{id}/build.log")).is_file());

        let broker = cluster_broker(&dir, 2).await;
        let pods = broker
            .list(&[("buildit.dev/build-id", id.as_str())])
            .await
            .unwrap();
        assert_eq!(pods.len(), 1);
        let labels = pods[0].metadata.labels.clone().unwrap();
        assert_eq!(labels["buildit.dev/sandbox"], "sb-a");
        assert_eq!(labels["buildit.dev/managed-by"], "mcp");
        assert_eq!(labels["buildit.dev/state"], "built");
        let notes = pods[0].metadata.annotations.clone().unwrap();
        assert_eq!(notes["buildit.dev/context"], "svc");
        assert_eq!(notes["buildit.dev/dockerfile"], "Dockerfile");

        a.cancel().await.unwrap();
        first.shut_down().await;

        // a new process with no memory of the build
        let second = start(broker, Duration::from_secs(5)).await;
        let (is_error, text) = raw_call(
            &second.addr,
            "sb-a",
            "run",
            serde_json::json!({
                "build_id": id,
                "cmd": ["sh", "-c", "echo out-1; echo error: on stderr >&2; exit 5"],
                "fetch_paths": ["/out/a.txt", "/nope"],
            }),
        )
        .await;
        assert!(!is_error, "{text}");
        let ran = json(&text);
        assert_eq!(ran["exit"], 5, "{ran}");
        assert_eq!(ran["log"]["log"], "run:1");
        let tail = ran["log"]["tail"].as_str().unwrap();
        assert!(
            tail.contains("out-1") && tail.contains("error: on stderr"),
            "{ran}"
        );
        assert!(
            ran["log"]["errors"]
                .as_str()
                .unwrap()
                .contains("error: on stderr"),
            "{ran}"
        );
        assert_eq!(
            ran["fetched"],
            serde_json::json!([format!(".buildit/{id}/out/a.txt")])
        );
        assert_eq!(ran["missing"], serde_json::json!(["nope"]));
        assert_eq!(
            std::fs::read_to_string(dir.join(format!(".buildit/{id}/out/a.txt"))).unwrap(),
            "artifact\n"
        );
        assert!(dir.join(format!(".buildit/{id}/run-1.log")).is_file());

        let a = client(&second.addr, TOKEN, Some("sb-a")).await.unwrap();
        let ran = call_ok(
            &a,
            "run",
            serde_json::json!({ "build_id": id, "cmd": ["true"] }),
        )
        .await;
        assert_eq!(ran["exit"], 0, "{ran}");
        assert_eq!(ran["log"]["log"], "run:2");

        let page = call_ok(
            &a,
            "logs",
            serde_json::json!({ "build_id": id, "which": "build", "grep": "^hello-from-build$", "context_lines": 1 }),
        )
        .await;
        assert_eq!(page["log"], "build");
        assert_eq!(page["matched"], 1, "{page}");
        assert!(
            page["lines"]
                .as_str()
                .unwrap()
                .contains(":hello-from-build\n"),
            "{page}"
        );
        assert_eq!(page["lines"].as_str().unwrap().lines().count(), 3, "{page}");
        assert_eq!(page["truncated"], false);

        let latest = call_ok(&a, "logs", serde_json::json!({ "build_id": id })).await;
        assert_eq!(latest["log"], "run:2");
        assert_eq!(
            latest["logs"],
            serde_json::json!(["build", "run:1", "run:2"])
        );

        let head = call_ok(
            &a,
            "logs",
            serde_json::json!({ "build_id": id, "which": "run:1", "head_lines": 1 }),
        )
        .await;
        assert!(
            head["lines"]
                .as_str()
                .unwrap()
                .starts_with("1:$ buildah from"),
            "{head}"
        );
        let total = head["total_lines"].as_u64().unwrap();
        let range = call_ok(
            &a,
            "logs",
            serde_json::json!({ "build_id": id, "which": "run:1", "from_line": 2, "to_line": total }),
        )
        .await;
        assert_eq!(range["shown"], serde_json::json!([2, total]), "{range}");
        let capped = call_ok(
            &a,
            "logs",
            serde_json::json!({ "build_id": id, "which": "build", "max_bytes": 2048, "from_line": 1 }),
        )
        .await;
        assert!(capped["lines"].as_str().unwrap().len() <= 2048);
        assert!(
            capped["lines"].as_str().unwrap().starts_with("1:"),
            "{capped}"
        );
        let e = call_err(
            &a,
            "logs",
            serde_json::json!({ "build_id": id, "which": "run:9" }),
        )
        .await;
        assert!(e.contains("no run:9 log"), "{e}");

        // another sandbox sees nothing of sb-a's build
        let b = client(&second.addr, TOKEN, Some("sb-b")).await.unwrap();
        for (tool, args) in [
            (
                "run",
                serde_json::json!({ "build_id": id, "cmd": ["true"] }),
            ),
            ("logs", serde_json::json!({ "build_id": id })),
        ] {
            let e = call_err(&b, tool, args).await;
            assert!(e.contains("no live build"), "{tool}: {e}");
        }
        let cleaned = call_ok(&b, "clean", serde_json::json!({ "build_id": id })).await;
        assert_eq!(cleaned["deleted"], serde_json::json!([]));
        let (is_error, text) = raw_call(
            &second.addr,
            "sb-b",
            "logs",
            serde_json::json!({ "build_id": id }),
        )
        .await;
        assert!(is_error && text.contains("no live build"), "{text}");

        let bad = call_ok(&a, "build", serde_json::json!({ "context": "bad" })).await;
        assert_ne!(bad["exit"], 0, "{bad}");
        assert_eq!(bad["runnable"], false);
        assert!(
            bad["log"]["errors"]
                .as_str()
                .unwrap()
                .contains("undefined reference to frob"),
            "{bad}"
        );
        let bad_id = bad["build_id"].as_str().unwrap().to_string();
        let e = call_err(
            &a,
            "run",
            serde_json::json!({ "build_id": bad_id, "cmd": ["true"] }),
        )
        .await;
        assert!(e.contains("not runnable"), "{e}");

        let e = call_err(&a, "build", serde_json::json!({ "context": "svc" })).await;
        assert!(e.contains("max 2"), "{e}");
        assert!(e.contains(&id) && e.contains(&bad_id), "{e}");
        let other = call_ok(&b, "build", serde_json::json!({ "context": "bad" })).await;
        let other_id = other["build_id"].as_str().unwrap().to_string();

        let cleaned = call_ok(&a, "clean", serde_json::json!({})).await;
        let mut want = vec![id.clone(), bad_id.clone()];
        want.sort();
        assert_eq!(cleaned["deleted"], serde_json::json!(want));
        let cleaned = call_ok(&b, "clean", serde_json::json!({ "build_id": other_id })).await;
        assert_eq!(cleaned["deleted"], serde_json::json!([other_id]));
        for sandbox in ["sb-a", "sb-b"] {
            let name = SandboxName::parse(sandbox).unwrap();
            let broker = cluster_broker(&dir, 2).await;
            assert!(broker.live_builds(&name).await.unwrap().is_empty());
        }
        let e = call_err(&a, "logs", serde_json::json!({ "build_id": id })).await;
        assert!(e.contains("no live build"), "{e}");

        a.cancel().await.unwrap();
        b.cancel().await.unwrap();
        second.shut_down().await;
    }
}
