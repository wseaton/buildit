use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use axum::extract::{Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use clap::Args;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{DeleteParams, ListParams};
use rmcp::handler::server::common::Extension;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ServerHandler, schemars, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};

use crate::backend::{Backend, PodOpts, Resources};
use crate::pod::BuilderPod;
use crate::sandbox::{RESULTS_DIR, RelPath, SandboxDir, SandboxName, Workspace};

pub const MCP_PATH: &str = "/mcp";

const SANDBOX_HOSTS: [&str; 2] = ["host.containers.internal", "host.openshell.internal"];

const LABEL_MANAGED_BY: &str = "buildit.dev/managed-by";
const MANAGED_BY: &str = "mcp";
const LABEL_SANDBOX: &str = "buildit.dev/sandbox";
const LABEL_BUILD_ID: &str = "buildit.dev/build-id";

const LOG_DIR: &str = "/tmp/buildit-logs";
const POD_READY: Duration = Duration::from_secs(180);
const DEFAULT_BUILD_TIMEOUT_S: u64 = 1800;
const DEFAULT_RUN_TIMEOUT_S: u64 = 600;
const FROM_TIMEOUT: Duration = Duration::from_secs(300);
const EXEC_SLACK: Duration = Duration::from_secs(60);
const FETCH_LIMIT: u64 = 512 * 1024 * 1024;
const SUMMARY_LINES: u64 = 40;
const SUMMARY_BYTES: u64 = 8 * 1024;
const DEFAULT_LOG_BYTES: u64 = 16 * 1024;
const MAX_LOG_BYTES: u64 = 64 * 1024;
const TAIL_READ: u64 = 1024 * 1024;

// appends `$ argv`, then the command's stdout and stderr, to the log
const STEP_SCRIPT: &str = r#"log="$1"; secs="$2"; shift 2; mkdir -p "${log%/*}"; printf '$ %s\n' "$*" >> "$log"; exec timeout "$secs" "$@" >> "$log" 2>&1"#;
const ALLOC_SCRIPT: &str = r#"set -C; n=1; until : > "$1/run-$n.log"; do n=$((n + 1)); [ "$n" -le 9999 ] || exit 1; done 2>/dev/null; echo "$n""#;
const FETCH_SCRIPT: &str = r#"ctr="$1"; shift; mnt=$(buildah mount "$ctr") || exit 1; cd "$mnt" || exit 1; exec tar -cf - --ignore-failed-read -- "$@""#;

#[derive(Args)]
pub struct McpArgs {
    /// Listen address for the streamable-http endpoint
    #[arg(long, env = "MCP_BIND", default_value = "0.0.0.0:8849")]
    pub bind: String,
    /// File of `<token> <sandbox> [<workdir>]` lines, re-read on every request
    #[arg(long, env = "MCP_TOKENS_FILE")]
    pub tokens_file: PathBuf,
    /// Sandbox workdir for token lines that name none
    #[arg(long, env = "BROKER_SANDBOX_WORKDIR")]
    pub sandbox_workdir: Option<String>,
    /// Host header values accepted besides loopback and the sandbox gateway names
    #[arg(
        long = "allowed-host",
        env = "BROKER_ALLOWED_HOSTS",
        value_delimiter = ','
    )]
    pub allowed_hosts: Vec<String>,
    /// Run off-cluster against this kubeconfig context
    #[arg(long)]
    pub kubecontext: Option<String>,
    /// Namespace for builder pods (default: the client's)
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// Builder pod lifetime in seconds (activeDeadlineSeconds)
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

pub async fn serve(client: kube::Client, args: McpArgs) -> Result<()> {
    let namespace = args
        .namespace
        .unwrap_or_else(|| client.default_namespace().to_string());
    let broker = Arc::new(Broker {
        client,
        namespace,
        tokens_file: args.tokens_file,
        default_workdir: args
            .sandbox_workdir
            .as_deref()
            .map(SandboxDir::parse)
            .transpose()?,
        local_workdir: None,
        resources: Resources {
            requests: args.requests,
            limits: args.limits,
        },
        deadline_secs: args.pod_deadline,
        max_builds: args.max_builds,
    });
    let listener = tokio::net::TcpListener::bind(&args.bind)
        .await
        .with_context(|| format!("binding {}", args.bind))?;
    tracing::info!(
        "buildit mcp listening on http://{}{MCP_PATH} (namespace {})",
        args.bind,
        broker.namespace
    );
    axum::serve(listener, router(broker, &args.allowed_hosts))
        .await
        .context("serving buildit mcp")
}

pub fn router(broker: Arc<Broker>, extra_hosts: &[String]) -> axum::Router {
    let mut hosts = StreamableHttpServerConfig::default().allowed_hosts;
    hosts.extend(SANDBOX_HOSTS.map(String::from));
    hosts.extend(extra_hosts.iter().map(|h| h.trim().to_string()));
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_sse_keep_alive(None)
        .with_sse_retry(None)
        .with_allowed_hosts(hosts);
    let served = broker.clone();
    let service: StreamableHttpService<BuilditMcp, NeverSessionManager> =
        StreamableHttpService::new(
            move || {
                Ok(BuilditMcp {
                    broker: served.clone(),
                })
            },
            Arc::new(NeverSessionManager::default()),
            config,
        );
    axum::Router::new()
        .nest_service(MCP_PATH, service)
        .layer(axum::middleware::from_fn_with_state(broker, authenticate))
}

async fn authenticate(State(broker): State<Arc<Broker>>, mut req: Request, next: Next) -> Response {
    match broker.caller(req.headers()).await {
        Ok(caller) => {
            req.extensions_mut().insert(caller);
            next.run(req).await
        }
        Err(status) => status.into_response(),
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|t| !t.is_empty())
}

#[derive(Debug, PartialEq, Eq)]
struct TokenLine {
    token: String,
    sandbox: SandboxName,
    workdir: Option<SandboxDir>,
}

fn parse_tokens(text: &str) -> Result<Vec<TokenLine>> {
    let mut lines = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let fields: Vec<&str> = raw.split_whitespace().collect();
        let (token, sandbox, workdir) = match fields[..] {
            [] => continue,
            [token, sandbox] => (token, sandbox, None),
            [token, sandbox, workdir] => (token, sandbox, Some(workdir)),
            _ => bail!(
                "token file line {} is not `<token> <sandbox> [<workdir>]`",
                i + 1
            ),
        };
        lines.push(TokenLine {
            token: token.to_string(),
            sandbox: SandboxName::parse(sandbox)?,
            workdir: workdir.map(SandboxDir::parse).transpose()?,
        });
    }
    Ok(lines)
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
        let ok = raw.len() <= 63
            && raw.starts_with(|c: char| c.is_ascii_alphanumeric())
            && raw.ends_with(|c: char| c.is_ascii_alphanumeric())
            && raw
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !ok {
            bail!("invalid build_id {raw:?}");
        }
        Ok(Self(raw.to_string()))
    }

    fn tag(&self) -> String {
        format!("localhost/buildit/{}:latest", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub sandbox: SandboxName,
    pub workspace: Workspace,
}

pub struct Broker {
    client: kube::Client,
    namespace: String,
    tokens_file: PathBuf,
    default_workdir: Option<SandboxDir>,
    local_workdir: Option<PathBuf>,
    resources: Resources,
    deadline_secs: i64,
    max_builds: usize,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BuildParams {
    /// Build context: a subdirectory of the workspace, e.g. "svc/api"
    pub context: String,
    /// Dockerfile path relative to the context (default "Dockerfile")
    pub dockerfile: Option<String>,
    /// Multi-stage target to stop at
    pub target: Option<String>,
    /// Build args, NAME -> value
    #[serde(default)]
    pub build_args: BTreeMap<String, String>,
    /// Build timeout in seconds (default 1800)
    pub timeout_s: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunParams {
    /// build_id returned by a successful build
    pub build_id: String,
    /// argv to run in a fresh container of the built image; use ["sh", "-c", "..."] for a shell
    pub cmd: Vec<String>,
    /// Run timeout in seconds (default 600)
    pub timeout_s: Option<u64>,
    /// Container paths to copy back into .buildit/<build_id>/<path> after the run
    #[serde(default)]
    pub fetch_paths: Vec<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct LogsParams {
    /// build_id from build
    pub build_id: String,
    /// "build" (default) or "run:<n>"
    pub which: Option<String>,
    /// Extended regex; returns matching lines as `N:text`
    pub grep: Option<String>,
    /// First line to return (1-based)
    pub from_line: Option<u64>,
    /// Last line to return
    pub to_line: Option<u64>,
    /// Without grep or a line range: the last N lines (default 100)
    pub tail_lines: Option<u64>,
    /// Output cap in bytes (default 16384, max 65536)
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CleanParams {
    /// Build to delete; omit to delete every build of this sandbox
    pub build_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BuildReply {
    pub build_id: String,
    pub exit: Option<i32>,
    pub timed_out: bool,
    pub log_tail: String,
}

#[derive(Debug, Serialize)]
pub struct RunReply {
    pub log: String,
    pub exit: Option<i32>,
    pub timed_out: bool,
    pub log_tail: String,
    pub fetched: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct LogsReply {
    pub lines: String,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct CleanReply {
    pub deleted: Vec<String>,
}

struct Outcome {
    exit: Option<i32>,
    timed_out: bool,
}

fn argv<const N: usize>(parts: [&str; N]) -> Vec<String> {
    parts.map(String::from).to_vec()
}

fn timeout(requested: Option<u64>, default: u64) -> Duration {
    Duration::from_secs(requested.unwrap_or(default).clamp(1, 7200))
}

fn alive(pod: &Pod) -> bool {
    pod.metadata.deletion_timestamp.is_none()
        && !matches!(
            pod.status.as_ref().and_then(|s| s.phase.as_deref()),
            Some("Succeeded" | "Failed")
        )
}

fn bud_argv(
    id: &BuildId,
    dockerfile: &RelPath,
    target: Option<&str>,
    build_args: &BTreeMap<String, String>,
) -> Result<Vec<String>> {
    let ws = Backend::Buildah.workspace();
    let mut out = argv(["buildah", "bud"]);
    if let Some(t) = target {
        out.extend(["--target".to_string(), t.to_string()]);
    }
    for (k, v) in build_args {
        if k.is_empty() || k.contains('=') {
            bail!("invalid build arg name {k:?}");
        }
        out.extend(["--build-arg".to_string(), format!("{k}={v}")]);
    }
    out.extend([
        "-f".to_string(),
        format!("{ws}/{}", dockerfile.as_str()),
        "-t".to_string(),
        id.tag(),
        ws.to_string(),
    ]);
    Ok(out)
}

async fn step(pod: &BuilderPod, log: &str, limit: Duration, cmd: &[String]) -> Result<Outcome> {
    let mut sh = argv(["sh", "-c", STEP_SCRIPT, "sh"]);
    sh.extend([format!("{LOG_DIR}/{log}"), limit.as_secs().to_string()]);
    sh.extend_from_slice(cmd);
    let started = Instant::now();
    let Ok(out) = tokio::time::timeout(limit + EXEC_SLACK, pod.exec_output(&sh, 4096)).await else {
        return Ok(Outcome {
            exit: None,
            timed_out: true,
        });
    };
    let code = out?
        .code
        .ok_or_else(|| anyhow!("step wrote more than expected to stdout"))?;
    Ok(Outcome {
        exit: Some(code),
        timed_out: code != 0 && started.elapsed() >= limit,
    })
}

// keep_end keeps the last max bytes, cut at a line start
async fn read_capped(
    pod: &BuilderPod,
    cmd: &[String],
    max: u64,
    keep_end: bool,
) -> Result<LogsReply> {
    let out = pod
        .exec_output(cmd, if keep_end { TAIL_READ } else { max })
        .await?;
    if let Some(code) = out.code
        && !(code == 0 || (code == 1 && out.stderr.is_empty()))
    {
        bail!(
            "{}: {}",
            cmd.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let mut bytes = out.stdout;
    let mut truncated = out.code.is_none();
    let max = usize::try_from(max).unwrap_or(usize::MAX);
    if keep_end && bytes.len() > max {
        let cut = bytes.len() - max;
        let start = bytes[cut..]
            .iter()
            .position(|b| *b == b'\n')
            .map_or(cut, |i| cut + i + 1);
        bytes.drain(..start);
        truncated = true;
    }
    Ok(LogsReply {
        lines: String::from_utf8_lossy(&bytes).into_owned(),
        truncated,
    })
}

async fn log_tail(pod: &BuilderPod, log: &str) -> Result<String> {
    let cmd = argv([
        "tail",
        "-n",
        &SUMMARY_LINES.to_string(),
        "--",
        &format!("{LOG_DIR}/{log}"),
    ]);
    Ok(read_capped(pod, &cmd, SUMMARY_BYTES, true).await?.lines)
}

// regular files and directories only; anything else, or outside dest, is dropped
fn unpack(tar: &[u8], dest: &Path) -> Result<Vec<String>> {
    use tar::EntryType;
    let mut files = Vec::new();
    for entry in tar::Archive::new(tar)
        .entries()
        .context("reading fetched tar")?
    {
        let mut entry = entry.context("reading fetched tar entry")?;
        let Ok(rel) = RelPath::parse(&entry.path()?.to_string_lossy()) else {
            continue;
        };
        match entry.header().entry_type() {
            EntryType::Regular | EntryType::Continuous => {
                if entry.unpack_in(dest)? {
                    files.push(rel.as_str().to_string());
                }
            }
            EntryType::Directory => std::fs::create_dir_all(dest.join(rel.as_str()))?,
            _ => continue,
        }
    }
    Ok(files)
}

impl Broker {
    async fn caller(&self, headers: &HeaderMap) -> Result<Caller, StatusCode> {
        let token = bearer(headers).ok_or(StatusCode::UNAUTHORIZED)?;
        let lines = tokio::fs::read_to_string(&self.tokens_file)
            .await
            .context("reading the token file")
            .and_then(|text| parse_tokens(&text))
            .map_err(|e| {
                tracing::error!("{}: {e:#}", self.tokens_file.display());
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        let line = lines
            .into_iter()
            .fold(None, |found, l| {
                if constant_time_eq(l.token.as_bytes(), token.as_bytes()) {
                    Some(l)
                } else {
                    found
                }
            })
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let workspace = match (
            &self.local_workdir,
            line.workdir.or(self.default_workdir.clone()),
        ) {
            (Some(dir), _) => Workspace::Local { dir: dir.clone() },
            (None, Some(workdir)) => Workspace::Openshell { workdir },
            (None, None) => {
                tracing::error!(
                    "sandbox {} has no workdir and BROKER_SANDBOX_WORKDIR is unset",
                    line.sandbox.as_str()
                );
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        };
        Ok(Caller {
            sandbox: line.sandbox,
            workspace,
        })
    }

    fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    async fn list(&self, sandbox: &SandboxName, id: Option<&BuildId>) -> Result<Vec<Pod>> {
        let mut sel = format!(
            "{LABEL_MANAGED_BY}={MANAGED_BY},{LABEL_SANDBOX}={}",
            sandbox.as_str()
        );
        if let Some(id) = id {
            sel.push_str(&format!(",{LABEL_BUILD_ID}={}", id.0));
        }
        Ok(self
            .pods()
            .list(&ListParams::default().labels(&sel))
            .await
            .with_context(|| format!("listing builder pods ({sel})"))?
            .items)
    }

    async fn find(&self, sandbox: &SandboxName, id: &BuildId) -> Result<BuilderPod> {
        if !self.list(sandbox, Some(id)).await?.iter().any(alive) {
            bail!("no live build {}; build again", id.0);
        }
        Ok(BuilderPod::existing(
            self.client.clone(),
            &self.namespace,
            &id.0,
        ))
    }

    fn builder_pod(&self, sandbox: &SandboxName, id: &BuildId) -> Result<Pod> {
        let opts = PodOpts {
            idle_nodes: &[],
            resources: &self.resources,
            cache: None,
            node: None,
        };
        let mut pod = Backend::Buildah.pod_spec(&id.0, &self.namespace, &opts)?;
        pod.metadata.labels.get_or_insert_default().extend(
            [
                (LABEL_MANAGED_BY, MANAGED_BY),
                (LABEL_SANDBOX, sandbox.as_str()),
                (LABEL_BUILD_ID, &id.0),
            ]
            .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        let spec = pod.spec.get_or_insert_default();
        spec.active_deadline_seconds = Some(self.deadline_secs);
        spec.automount_service_account_token = Some(false);
        spec.enable_service_links = Some(false);
        for c in &mut spec.containers {
            c.command = Some(argv(["sleep", &self.deadline_secs.to_string()]));
        }
        Ok(pod)
    }

    pub async fn build(&self, caller: &Caller, p: BuildParams) -> Result<BuildReply> {
        let context = RelPath::parse(&p.context)?;
        let dockerfile = RelPath::parse(p.dockerfile.as_deref().unwrap_or("Dockerfile"))?;
        let id = BuildId::fresh();
        let bud = bud_argv(&id, &dockerfile, p.target.as_deref(), &p.build_args)?;
        let live: Vec<Pod> = self
            .list(&caller.sandbox, None)
            .await?
            .into_iter()
            .filter(alive)
            .collect();
        if live.len() >= self.max_builds {
            let ids: Vec<&str> = live
                .iter()
                .filter_map(|p| p.metadata.name.as_deref())
                .collect();
            bail!(
                "this sandbox has {} live builds (max {}): {}; clean one first",
                live.len(),
                self.max_builds,
                ids.join(", ")
            );
        }
        let stage = tempfile::tempdir().context("creating staging dir")?;
        let ctx = stage.path().join("ctx");
        tokio::task::block_in_place(|| {
            caller
                .workspace
                .fetch_context(&caller.sandbox, &context, &ctx)
        })?;
        let tarball = crate::context::tarball(&ctx)?;
        tracing::info!(
            "build {}: sandbox {} context {} ({} KiB)",
            id.0,
            caller.sandbox.as_str(),
            context.as_str(),
            tarball.len() / 1024
        );
        let pod = BuilderPod::create_from(
            self.client.clone(),
            &self.namespace,
            &self.builder_pod(&caller.sandbox, &id)?,
        )
        .await?;
        let built = async {
            pod.wait_ready(POD_READY).await?;
            pod.exec_capture(&Backend::Buildah.setup_command()).await?;
            pod.exec_with_stdin(&Backend::Buildah.untar_command(), &tarball)
                .await?;
            let limit = timeout(p.timeout_s, DEFAULT_BUILD_TIMEOUT_S);
            let outcome = step(&pod, "build.log", limit, &bud).await?;
            Ok(BuildReply {
                build_id: id.0.clone(),
                exit: outcome.exit,
                timed_out: outcome.timed_out,
                log_tail: log_tail(&pod, "build.log").await?,
            })
        }
        .await;
        if built.is_err()
            && let Err(e) = pod.delete().await
        {
            tracing::warn!("deleting builder pod {}: {e:#}", id.0);
        }
        built
    }

    pub async fn run(&self, caller: &Caller, p: RunParams) -> Result<RunReply> {
        if p.cmd.is_empty() {
            bail!("cmd must not be empty");
        }
        let id = BuildId::parse(&p.build_id)?;
        let paths = p
            .fetch_paths
            .iter()
            .map(|f| RelPath::parse(f.trim_start_matches('/')))
            .collect::<Result<Vec<_>>>()?;
        let pod = self.find(&caller.sandbox, &id).await?;
        let alloc = pod
            .exec_capture(&argv(["sh", "-c", ALLOC_SCRIPT, "sh", LOG_DIR]))
            .await?;
        let n: u32 = alloc
            .trim()
            .parse()
            .with_context(|| format!("bad run number {alloc:?}"))?;
        let log = format!("run-{n}.log");
        let ctr = format!("{}-run{n}", id.0);
        let from = argv(["buildah", "from", "--pull-never", "--name", &ctr, &id.tag()]);
        let mut outcome = step(&pod, &log, FROM_TIMEOUT, &from).await?;
        let mut fetched = Ok(Vec::new());
        if outcome.exit == Some(0) {
            let mut cmd = argv(["buildah", "run", &ctr, "--"]);
            cmd.extend(p.cmd);
            let limit = timeout(p.timeout_s, DEFAULT_RUN_TIMEOUT_S);
            outcome = step(&pod, &log, limit, &cmd).await?;
            if !paths.is_empty() {
                fetched = self.fetch(caller, &pod, &ctr, &id, &paths).await;
            }
            if let Err(e) = pod.exec_capture(&argv(["buildah", "rm", &ctr])).await {
                tracing::warn!("removing container {ctr}: {e:#}");
            }
        }
        let fetched = fetched?;
        Ok(RunReply {
            log: format!("run:{n}"),
            exit: outcome.exit,
            timed_out: outcome.timed_out,
            log_tail: log_tail(&pod, &log).await?,
            fetched: fetched
                .iter()
                .map(|f| format!("{RESULTS_DIR}/{}/{f}", id.0))
                .collect(),
        })
    }

    async fn fetch(
        &self,
        caller: &Caller,
        pod: &BuilderPod,
        ctr: &str,
        id: &BuildId,
        paths: &[RelPath],
    ) -> Result<Vec<String>> {
        let mut cmd = argv(["buildah", "unshare", "sh", "-c", FETCH_SCRIPT, "sh", ctr]);
        cmd.extend(paths.iter().map(|p| p.as_str().to_string()));
        let out = pod.exec_output(&cmd, FETCH_LIMIT).await?;
        match out.code {
            None => bail!("fetch_paths add up to more than {FETCH_LIMIT} bytes"),
            Some(0) => {}
            Some(_) => bail!(
                "fetching paths failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ),
        }
        let stage = tempfile::tempdir().context("creating staging dir")?;
        let results = stage.path().join(&id.0);
        std::fs::create_dir(&results).context("creating results dir")?;
        let fetched = unpack(&out.stdout, &results)?;
        tokio::task::block_in_place(|| caller.workspace.publish(&caller.sandbox, &results))?;
        Ok(fetched)
    }

    pub async fn logs(&self, sandbox: &SandboxName, p: LogsParams) -> Result<LogsReply> {
        let id = BuildId::parse(&p.build_id)?;
        let file = match p.which.as_deref().unwrap_or("build") {
            "build" => "build.log".to_string(),
            which => match which
                .strip_prefix("run:")
                .and_then(|n| n.parse::<u32>().ok())
            {
                Some(n) => format!("run-{n}.log"),
                None => bail!("which must be build or run:<n>, got {which:?}"),
            },
        };
        let max = p
            .max_bytes
            .unwrap_or(DEFAULT_LOG_BYTES)
            .clamp(1024, MAX_LOG_BYTES);
        let pod = self.find(sandbox, &id).await?;
        let path = format!("{LOG_DIR}/{file}");
        if let Some(pattern) = &p.grep {
            let cmd = argv(["grep", "-n", "-E", "-e", pattern, "--", &path]);
            return read_capped(&pod, &cmd, max, false).await;
        }
        if p.from_line.is_some() || p.to_line.is_some() {
            let from = p.from_line.unwrap_or(1).max(1);
            let to = p.to_line.map_or("$".to_string(), |t| t.to_string());
            let cmd = argv(["sed", "-n", &format!("{from},{to}p"), "--", &path]);
            return read_capped(&pod, &cmd, max, false).await;
        }
        let n = p.tail_lines.unwrap_or(100).to_string();
        read_capped(&pod, &argv(["tail", "-n", &n, "--", &path]), max, true).await
    }

    pub async fn clean(&self, sandbox: &SandboxName, id: Option<&str>) -> Result<CleanReply> {
        let id = id.map(BuildId::parse).transpose()?;
        let mut deleted = Vec::new();
        for pod in self.list(sandbox, id.as_ref()).await? {
            if pod.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let name = pod.metadata.name.unwrap_or_default();
            self.pods()
                .delete(&name, &DeleteParams::default())
                .await
                .with_context(|| format!("deleting builder pod {name}"))?;
            deleted.push(name);
        }
        deleted.sort();
        Ok(CleanReply { deleted })
    }
}

#[derive(Clone)]
pub struct BuilditMcp {
    broker: Arc<Broker>,
}

fn caller(parts: &Parts) -> Result<Caller> {
    parts
        .extensions
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| anyhow!("request reached a tool without an authenticated caller"))
}

fn reply<T: Serialize>(result: Result<T>) -> Result<String, String> {
    result
        .and_then(|r| serde_json::to_string(&r).context("serializing reply"))
        .map_err(|e| format!("{e:#}"))
}

#[tool_router]
impl BuilditMcp {
    #[tool(
        description = "Build a Dockerfile from a workspace subdirectory on a remote builder; pushes \
        nothing. Returns build_id, exit and the log's last lines. The builder stays up \
        for `run` and `logs` until `clean`."
    )]
    async fn build(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<BuildParams>,
    ) -> Result<String, String> {
        reply(async { self.broker.build(&caller(&parts)?, p).await }.await)
    }

    #[tool(
        description = "Run a command in a fresh container of a successful build. Returns exit, the \
        log name (run:<n>) and its last lines; fetch_paths are copied out of the container into \
        .buildit/<build_id>/<path>."
    )]
    async fn run(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<RunParams>,
    ) -> Result<String, String> {
        reply(async { self.broker.run(&caller(&parts)?, p).await }.await)
    }

    #[tool(
        description = "Read part of a build or run log: grep (numbered matches), else a \
        from_line/to_line range, else the last tail_lines. Output is capped at max_bytes."
    )]
    async fn logs(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<LogsParams>,
    ) -> Result<String, String> {
        reply(async { self.broker.logs(&caller(&parts)?.sandbox, p).await }.await)
    }

    #[tool(description = "Delete a build's builder pod, or all of this sandbox's builds.")]
    async fn clean(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<CleanParams>,
    ) -> Result<String, String> {
        reply(
            async {
                self.broker
                    .clean(&caller(&parts)?.sandbox, p.build_id.as_deref())
                    .await
            }
            .await,
        )
    }
}

#[tool_handler(name = "buildit")]
impl ServerHandler for BuilditMcp {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::backend::Resources;
    use crate::mcp::{
        Broker, BuildId, Caller, MCP_PATH, TokenLine, bud_argv, parse_tokens, router, unpack,
    };
    use crate::sandbox::{RelPath, SandboxDir, SandboxName, Workspace};

    fn broker(client: kube::Client, tokens: &Path, local: Option<&Path>) -> Arc<Broker> {
        Arc::new(Broker {
            namespace: client.default_namespace().to_string(),
            client,
            tokens_file: tokens.to_path_buf(),
            default_workdir: Some(SandboxDir::parse("/sandbox/default").unwrap()),
            local_workdir: local.map(Path::to_path_buf),
            resources: Resources::default(),
            deadline_secs: 900,
            max_builds: 4,
        })
    }

    fn offline_client() -> kube::Client {
        let _ = rustls::crypto::ring::default_provider().install_default();
        kube::Client::try_from(kube::Config::new("http://127.0.0.1:9".parse().unwrap())).unwrap()
    }

    fn tokens(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("tokens");
        std::fs::write(&path, body).unwrap();
        path
    }

    async fn serve(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let app = router(broker, &["builds.svc".to_string()]);
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (addr, task)
    }

    #[test]
    fn token_lines_take_an_optional_workdir() {
        let lines = parse_tokens("tok-a sb-a /sandbox/task-a\n\n  tok-b  sb-b \n").unwrap();
        assert_eq!(
            lines,
            [
                TokenLine {
                    token: "tok-a".to_string(),
                    sandbox: SandboxName::parse("sb-a").unwrap(),
                    workdir: Some(SandboxDir::parse("/sandbox/task-a").unwrap()),
                },
                TokenLine {
                    token: "tok-b".to_string(),
                    sandbox: SandboxName::parse("sb-b").unwrap(),
                    workdir: None,
                },
            ]
        );
        for bad in [
            "tok-a\n",
            "tok-a sb-a /w x\n",
            "tok-a --flag\n",
            "tok-a sb-a rel\n",
        ] {
            assert!(parse_tokens(bad).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn callers_come_from_the_bearer_token_alone() {
        let dir = tempfile::tempdir().unwrap();
        let file = tokens(dir.path(), "tok-a sb-a /sandbox/a\ntok-b sb-b\n");
        let broker = broker(offline_client(), &file, None);
        let caller = |sandbox: &str, workdir: &str| Caller {
            sandbox: SandboxName::parse(sandbox).unwrap(),
            workspace: Workspace::Openshell {
                workdir: SandboxDir::parse(workdir).unwrap(),
            },
        };
        let a = [
            ("authorization", "Bearer tok-a"),
            ("x-crucible-sandbox", "sb-b"),
        ];
        assert_eq!(
            broker.caller(&headers(&a)).await,
            Ok(caller("sb-a", "/sandbox/a"))
        );
        assert_eq!(
            broker
                .caller(&headers(&[("authorization", "Bearer tok-b")]))
                .await,
            Ok(caller("sb-b", "/sandbox/default"))
        );
        for bad in [
            &[("authorization", "Bearer tok-")][..],
            &[("authorization", "Bearer sb-a")],
            &[("authorization", "tok-a")],
            &[("x-crucible-sandbox", "sb-a")],
        ] {
            assert_eq!(
                broker.caller(&headers(bad)).await,
                Err(axum::http::StatusCode::UNAUTHORIZED)
            );
        }
        std::fs::write(&file, "tok-c sb-c /sandbox/c\n").unwrap();
        assert_eq!(
            broker
                .caller(&headers(&[("authorization", "Bearer tok-a")]))
                .await,
            Err(axum::http::StatusCode::UNAUTHORIZED)
        );
    }

    fn headers(pairs: &[(&'static str, &'static str)]) -> axum::http::HeaderMap {
        pairs
            .iter()
            .map(|(k, v)| (axum::http::HeaderName::from_static(k), v.parse().unwrap()))
            .collect()
    }

    async fn post(addr: &str, host: &str, auth: &str, body: &str) -> (u16, String) {
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST {MCP_PATH} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n{auth}Content-Length: {}\r\n\
             Connection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(req.as_bytes()).await.unwrap();
        let mut out = String::new();
        sock.read_to_string(&mut out).await.unwrap();
        let status = out.split_whitespace().nth(1).unwrap().parse().unwrap();
        (status, out)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_is_stateless_json_behind_the_token_and_host_checks() {
        let dir = tempfile::tempdir().unwrap();
        let file = tokens(dir.path(), "tok-a sb-a /sandbox/a\n");
        let (addr, _task) = serve(broker(offline_client(), &file, None)).await;
        let list = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#;
        let auth = "Authorization: Bearer tok-a\r\n";
        let (status, reply) = post(&addr, "127.0.0.1", auth, list).await;
        let reply = reply.to_ascii_lowercase();
        assert_eq!(status, 200, "{reply}");
        assert!(reply.contains("content-type: application/json"), "{reply}");
        assert!(!reply.contains("mcp-session-id"), "{reply}");
        for tool in ["\"build\"", "\"run\"", "\"logs\"", "\"clean\""] {
            assert!(reply.contains(tool), "{tool}: {reply}");
        }
        for host in ["host.openshell.internal:8849", "builds.svc"] {
            assert_eq!(post(&addr, host, auth, list).await.0, 200, "{host}");
        }
        assert_eq!(post(&addr, "evil.example", auth, list).await.0, 403);
        assert_eq!(post(&addr, "127.0.0.1", "", list).await.0, 401);
        let wrong = "Authorization: Bearer nope\r\n";
        assert_eq!(post(&addr, "127.0.0.1", wrong, list).await.0, 401);
    }

    #[test]
    fn build_ids_cannot_smuggle_selectors() {
        assert!(BuildId::parse(&BuildId::fresh().0).is_ok());
        for bad in [
            "",
            "A",
            "-x",
            "x-",
            "a,buildit.dev/sandbox=b",
            "a=b",
            "a b",
            "a.b",
        ] {
            assert!(BuildId::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn builder_pods_carry_no_credentials_and_end_at_the_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let broker = broker(offline_client(), &tokens(dir.path(), ""), None);
        let id = BuildId::parse("buildit-1").unwrap();
        let pod = broker
            .builder_pod(&SandboxName::parse("sb-a").unwrap(), &id)
            .unwrap();
        let labels = pod.metadata.labels.unwrap();
        assert_eq!(labels["buildit.dev/sandbox"], "sb-a");
        assert_eq!(labels["buildit.dev/build-id"], "buildit-1");
        let spec = pod.spec.unwrap();
        assert_eq!(spec.automount_service_account_token, Some(false));
        assert_eq!(spec.enable_service_links, Some(false));
        assert_eq!(spec.active_deadline_seconds, Some(900));
        let c = &spec.containers[0];
        assert_eq!(c.command.as_deref().unwrap(), ["sleep", "900"]);
        assert!(c.env_from.is_none());
        assert!(c.env.iter().flatten().all(|e| e.value_from.is_none()));

        let args = BTreeMap::from([("A".to_string(), "x=y z".to_string())]);
        let df = RelPath::parse("docker/Dockerfile").unwrap();
        let bud = bud_argv(&id, &df, Some("base"), &args).unwrap().join(" ");
        assert_eq!(
            bud,
            "buildah bud --target base --build-arg A=x=y z -f /home/build/workspace/docker/Dockerfile \
             -t localhost/buildit/buildit-1:latest /home/build/workspace"
        );
        let bad = BTreeMap::from([("A=B".to_string(), "v".to_string())]);
        assert!(bud_argv(&id, &df, None, &bad).is_err());
    }

    #[test]
    fn unpack_keeps_files_and_dirs_inside_dest_only() {
        let mut b = tar::Builder::new(Vec::new());
        let mut file = |path: &str, body: &[u8]| {
            let mut h = tar::Header::new_gnu();
            h.set_size(body.len() as u64);
            h.set_mode(0o644);
            h.set_entry_type(tar::EntryType::Regular);
            b.append_data(&mut h, path, body).unwrap();
        };
        file("out/app", b"bin");
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Symlink);
        h.set_size(0);
        b.append_link(&mut h, "out/passwd", "/etc/passwd").unwrap();
        let mut raw = b.into_inner().unwrap();
        let mut evil = tar::Header::new_old();
        evil.as_old_mut().name[..8].copy_from_slice(b"../evil\0");
        evil.set_size(1);
        evil.set_entry_type(tar::EntryType::Regular);
        evil.set_cksum();
        raw.truncate(raw.len() - 1024);
        raw.extend_from_slice(evil.as_bytes());
        raw.extend_from_slice(&[b'x'; 512]);
        raw.extend_from_slice(&[0; 1024]);

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("b1");
        std::fs::create_dir(&dest).unwrap();
        assert_eq!(unpack(&raw, &dest).unwrap(), ["out/app"]);
        assert_eq!(std::fs::read(dest.join("out/app")).unwrap(), b"bin");
        assert!(std::fs::symlink_metadata(dest.join("out/passwd")).is_err());
        assert!(!dir.path().join("evil").exists());
    }

    // a tools/call POST with no initialize and no session
    async fn call(
        addr: &str,
        token: &str,
        name: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": name, "arguments": args },
        });
        let auth = format!("Authorization: Bearer {token}\r\n");
        let (status, reply) = post(addr, "127.0.0.1", &auth, &body.to_string()).await;
        assert_eq!(status, 200, "{reply}");
        let (_, body) = reply.split_once("\r\n\r\n").unwrap();
        let reply: serde_json::Value = serde_json::from_str(body).unwrap();
        let text = reply["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string();
        match reply["result"]["isError"].as_bool() {
            Some(true) => Err(text),
            _ => Ok(serde_json::from_str(&text).unwrap()),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a cluster: BUILDIT_E2E_KUBECONTEXT=kind-x cargo test -- --ignored"]
    async fn e2e_build_restart_run_fetch_logs_and_clean() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let context = std::env::var("BUILDIT_E2E_KUBECONTEXT").unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(work.join("svc")).unwrap();
        std::fs::write(
            work.join("svc/Dockerfile"),
            "FROM docker.io/library/busybox:latest AS base\n\
             RUN echo hello-from-build && mkdir -p /out && echo artifact > /out/a.txt\n\
             FROM base AS broken\n\
             RUN exit 1\n",
        )
        .unwrap();
        let file = tokens(&work, "tok-a sb-a\ntok-b sb-b\n");
        let cluster = || async {
            let client = crate::client_for(Some(&context)).await.unwrap();
            broker(client, &file, Some(&work))
        };

        let (addr, first) = serve(cluster().await).await;
        let args = serde_json::json!({ "context": "svc", "target": "base" });
        let built = call(&addr, "tok-a", "build", args).await.unwrap();
        assert_eq!(built["exit"], 0, "{built}");
        let tail = built["log_tail"].as_str().unwrap();
        assert!(tail.contains("hello-from-build"), "{built}");
        let id = built["build_id"].as_str().unwrap().to_string();
        first.abort();

        // a new server process with no memory of the build
        let (addr, _second) = serve(cluster().await).await;
        let a = |tool, args| call(&addr, "tok-a", tool, args);
        let b = |tool, args| call(&addr, "tok-b", tool, args);
        let args = serde_json::json!({
            "build_id": id,
            "cmd": ["sh", "-c", "echo out-1; exit 5"],
            "fetch_paths": ["/out/a.txt", "/nope"],
        });
        let ran = a("run", args).await.unwrap();
        assert_eq!(ran["exit"], 5, "{ran}");
        assert_eq!(ran["log"], "run:1");
        assert!(ran["log_tail"].as_str().unwrap().contains("out-1"), "{ran}");
        let fetched = format!(".buildit/{id}/out/a.txt");
        assert_eq!(ran["fetched"], serde_json::json!([fetched]));
        let body = std::fs::read_to_string(work.join(&fetched)).unwrap();
        assert_eq!(body, "artifact\n");

        let args = serde_json::json!({ "build_id": id, "grep": "^hello-from-build$" });
        let grep = a("logs", args).await.unwrap();
        assert!(
            grep["lines"]
                .as_str()
                .unwrap()
                .ends_with(":hello-from-build\n")
        );
        assert_eq!(grep["lines"].as_str().unwrap().lines().count(), 1);
        let args = serde_json::json!({ "build_id": id, "which": "run:1", "to_line": 1 });
        let line = a("logs", args).await.unwrap();
        assert!(
            line["lines"]
                .as_str()
                .unwrap()
                .starts_with("$ buildah from")
        );

        let only_id = serde_json::json!({ "build_id": id });
        let run = serde_json::json!({ "build_id": id, "cmd": ["true"] });
        for (tool, args) in [("run", run), ("logs", only_id.clone())] {
            let e = b(tool, args).await.unwrap_err();
            assert!(e.contains("no live build"), "{tool}: {e}");
        }
        let cleaned = b("clean", only_id.clone()).await.unwrap();
        assert_eq!(cleaned["deleted"], serde_json::json!([]));
        let cleaned = a("clean", serde_json::json!({})).await.unwrap();
        assert_eq!(cleaned["deleted"], serde_json::json!([id]));
        let e = a("logs", only_id).await.unwrap_err();
        assert!(e.contains("no live build"), "{e}");
    }
}
