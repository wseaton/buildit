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
use crate::logs::{LOG_DIR, Log, LogName, MAX_READ, Page, Query};
use crate::pod::BuilderPod;
use crate::sandbox::{RESULTS_DIR, RelPath, SandboxDir, SandboxName, Workspace};

pub const MCP_PATH: &str = "/mcp";

const SANDBOX_HOSTS: [&str; 2] = ["host.containers.internal", "host.openshell.internal"];

const LABEL_MANAGED_BY: &str = "buildit.dev/managed-by";
const MANAGED_BY: &str = "mcp";
const LABEL_SANDBOX: &str = "buildit.dev/sandbox";
const LABEL_BUILD_ID: &str = "buildit.dev/build-id";

const POD_READY: Duration = Duration::from_secs(180);
const DEFAULT_BUILD_TIMEOUT_S: u64 = 1800;
const DEFAULT_RUN_TIMEOUT_S: u64 = 600;
const FROM_TIMEOUT: Duration = Duration::from_secs(300);
const EXEC_SLACK: Duration = Duration::from_secs(60);
const FETCH_LIMIT: u64 = 512 * 1024 * 1024;
const SUMMARY_LINES: usize = 40;
const SUMMARY_BYTES: usize = 8 * 1024;
const SUMMARY_WINDOW: u64 = 256 * 1024;
const DEADLINE_MARGIN: Duration = Duration::from_secs(10);
const DEADLINE_EXCEEDED: &str = "DeadlineExceeded";
const CONTAINER_STDOUT: &str = "/proc/1/fd/1";
const TERMINATION_LOG: &str = "/dev/termination-log";
const DEFAULT_LOG_BYTES: u64 = 16 * 1024;
const MAX_LOG_BYTES: u64 = 64 * 1024;

// appends `$ argv`, then the command's stdout and stderr, to the log
const STEP_SCRIPT: &str = r#"log="$1"; secs="$2"; shift 2; mkdir -p "${log%/*}"; printf '$ %s\n' "$*" >> "$log"; exec timeout "$secs" "$@" >> "$log" 2>&1"#;
// STEP_SCRIPT in the background: writes its pid, then `<exit code>[ timeout]` to the
// exit file and the termination log, and mirrors the log to the container's stdout
const LAUNCH_SCRIPT: &str = r#"log="$1"; exit_file="$2"; pid_file="$3"; secs="$4"; mirror="$5"; term="$6"; shift 6
mkdir -p "${log%/*}"; printf '$ %s\n' "$*" >> "$log"
{
  start=$(date +%s)
  timeout "$secs" "$@" >> "$log" 2>&1
  code=$?
  if [ "$code" -ne 0 ] && [ $(( $(date +%s) - start )) -ge "$secs" ]; then code="$code timeout"; fi
  echo "$code" > "$exit_file.tmp"
  cat "$exit_file.tmp" > "$term"
  mv "$exit_file.tmp" "$exit_file"
} < /dev/null > /dev/null 2>&1 &
pid=$!
echo "$pid" > "$pid_file"
tail -n +1 -f --pid="$pid" "$log" < /dev/null 2> /dev/null > "$mirror" &"#;
// prints running, lost, or `exit <record>`; zombies count as lost
const STATE_SCRIPT: &str = r#"if [ -f "$1" ]; then echo "exit $(cat "$1")"; exit; fi
pid=$(cat "$2" 2>/dev/null)
case "$pid" in ""|*[!0-9]*) stat="" ;; *) stat=$(cat "/proc/$pid/stat" 2>/dev/null) ;; esac
stat=${stat##*) }
case "${stat%% *}" in
  ""|Z|X) if [ -f "$1" ]; then echo "exit $(cat "$1")"; else echo lost; fi ;;
  *) echo running ;;
esac"#;
const ALLOC_SCRIPT: &str = r#"set -C; n=1; until true > "$1/run-$n.log"; do n=$((n + 1)); [ "$n" -le 9999 ] || exit 1; done 2>/dev/null; echo "$n""#;
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
    /// Schedule builder pods only on nodes of this architecture (kubernetes.io/arch)
    #[arg(long)]
    pub node_arch: Option<String>,
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
        node_arch: args.node_arch,
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
    node_arch: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BuildParams {
    /// Build context: a subdirectory of the workspace, e.g. "svc/api"
    pub context: String,
    /// Dockerfile path relative to the context (default "Dockerfile")
    pub dockerfile: Option<String>,
    /// Multi-stage target to stop at
    #[serde(alias = "build_target")]
    pub target: Option<String>,
    /// Build args, NAME -> value
    #[serde(default)]
    pub build_args: BTreeMap<String, String>,
    /// Build timeout in seconds (default 1800)
    pub timeout_s: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StatusParams {
    /// build_id from build
    pub build_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunParams {
    /// build_id of a build whose status is succeeded
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
    /// Regex (Rust syntax); returns matches as `N:text`, context as `N-text`, gaps as `--`
    pub grep: Option<String>,
    /// Lines of context around each grep match (max 20)
    pub context: Option<u64>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BuildStatus {
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildState {
    Running,
    Exited { code: i32, timed_out: bool },
    Lost,
    Expired,
}

impl BuildState {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "running" => Ok(Self::Running),
            "lost" => Ok(Self::Lost),
            other => other
                .strip_prefix("exit ")
                .and_then(Self::record)
                .ok_or_else(|| anyhow!("unexpected build state {other:?}")),
        }
    }

    // `<exit code>[ timeout]`, as LAUNCH_SCRIPT writes it
    fn record(raw: &str) -> Option<Self> {
        let (code, timed_out) = match raw.split_whitespace().collect::<Vec<_>>()[..] {
            [code] => (code, false),
            [code, "timeout"] => (code, true),
            _ => return None,
        };
        Some(Self::Exited {
            code: code.parse().ok()?,
            timed_out,
        })
    }

    fn status(self) -> BuildStatus {
        match self {
            Self::Running => BuildStatus::Running,
            Self::Exited { code: 0, .. } => BuildStatus::Succeeded,
            Self::Exited { .. } | Self::Lost | Self::Expired => BuildStatus::Failed,
        }
    }

    fn exit(self) -> Option<i32> {
        match self {
            Self::Exited { code, .. } => Some(code),
            Self::Running | Self::Lost | Self::Expired => None,
        }
    }

    fn timed_out(self) -> bool {
        match self {
            Self::Exited { timed_out, .. } => timed_out,
            Self::Expired => true,
            Self::Running | Self::Lost => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PodView {
    Live,
    Ended(BuildState),
}

impl PodView {
    // the build's pod, unless it is gone or being deleted
    fn of(pods: &[Pod]) -> Option<Self> {
        let pod = pods
            .iter()
            .find(|p| p.metadata.deletion_timestamp.is_none())?;
        if alive(pod) {
            return Some(Self::Live);
        }
        let status = pod.status.as_ref();
        let record = status
            .and_then(|s| s.container_statuses.as_deref())
            .unwrap_or_default()
            .iter()
            .filter_map(|c| c.state.as_ref()?.terminated.as_ref()?.message.as_deref())
            .find_map(BuildState::record);
        Some(Self::Ended(match record {
            Some(state) => state,
            None if status.and_then(|s| s.reason.as_deref()) == Some(DEADLINE_EXCEEDED) => {
                BuildState::Expired
            }
            None => BuildState::Lost,
        }))
    }
}

enum Found {
    Live(BuilderPod),
    Ended(BuilderPod, BuildState),
}

#[derive(Debug, Serialize)]
pub struct BuildReply {
    pub build_id: String,
    pub status: BuildStatus,
}

#[derive(Debug, Serialize)]
pub struct StatusReply {
    pub build_id: String,
    pub status: BuildStatus,
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

fn build_limit(requested: Duration, deadline_secs: i64, used: Duration) -> Result<Duration> {
    let deadline = Duration::from_secs(u64::try_from(deadline_secs).unwrap_or(0));
    let left = deadline
        .saturating_sub(used)
        .saturating_sub(DEADLINE_MARGIN);
    if left < Duration::from_secs(1) {
        bail!(
            "the {deadline_secs}s pod deadline leaves no time to build after {}s of setup",
            used.as_secs()
        );
    }
    Ok(requested.min(left))
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

async fn step(pod: &BuilderPod, log: LogName, limit: Duration, cmd: &[String]) -> Result<Outcome> {
    let mut sh = argv(["sh", "-c", STEP_SCRIPT, "sh"]);
    sh.extend([log.path(), limit.as_secs().to_string()]);
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

fn build_file(name: &str) -> String {
    format!("{LOG_DIR}/build.{name}")
}

async fn launch(pod: &BuilderPod, limit: Duration, cmd: &[String]) -> Result<()> {
    let mut sh = argv(["sh", "-c", LAUNCH_SCRIPT, "sh"]);
    sh.extend([
        LogName::Build.path(),
        build_file("exit"),
        build_file("pid"),
        limit.as_secs().to_string(),
        CONTAINER_STDOUT.to_string(),
        TERMINATION_LOG.to_string(),
    ]);
    sh.extend_from_slice(cmd);
    pod.exec_capture(&sh).await?;
    Ok(())
}

async fn build_state(pod: &BuilderPod) -> Result<BuildState> {
    let mut sh = argv(["sh", "-c", STATE_SCRIPT, "sh"]);
    sh.extend([build_file("exit"), build_file("pid")]);
    BuildState::parse(&pod.exec_capture(&sh).await?)
}

fn summary(log: &Log) -> String {
    let tail = Query::Tail {
        lines: SUMMARY_LINES,
    };
    log.query(&tail, SUMMARY_BYTES).lines
}

async fn log_tail(pod: &BuilderPod, log: LogName) -> Result<String> {
    Ok(summary(&Log::fetch(pod, log, SUMMARY_WINDOW).await?))
}

fn usize_of(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

impl LogsParams {
    fn query(&self) -> Result<Query> {
        match (&self.grep, self.from_line, self.to_line, self.tail_lines) {
            (Some(p), None, None, None) => Query::grep(p, usize_of(self.context.unwrap_or(0))),
            _ if self.context.is_some() => bail!("context needs grep"),
            (None, None, None, n) => Ok(Query::Tail {
                lines: usize_of(n.unwrap_or(100)),
            }),
            (None, from, to, None) => Ok(Query::Range {
                from: usize_of(from.unwrap_or(1)),
                to: to.map(usize_of),
            }),
            _ => bail!("grep, from_line/to_line and tail_lines are mutually exclusive"),
        }
    }
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

    async fn lookup(&self, sandbox: &SandboxName, id: &BuildId) -> Result<Found> {
        let view = PodView::of(&self.list(sandbox, Some(id)).await?)
            .ok_or_else(|| anyhow!("no live build {}; build again", id.0))?;
        let pod = BuilderPod::existing(self.client.clone(), &self.namespace, &id.0);
        Ok(match view {
            PodView::Live => Found::Live(pod),
            PodView::Ended(state) => Found::Ended(pod, state),
        })
    }

    async fn find(&self, sandbox: &SandboxName, id: &BuildId) -> Result<BuilderPod> {
        match self.lookup(sandbox, id).await? {
            Found::Live(pod) => Ok(pod),
            Found::Ended(..) => bail!("build {}'s pod has ended; build again", id.0),
        }
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
        if let Some(arch) = &self.node_arch {
            spec.node_selector
                .get_or_insert_default()
                .insert("kubernetes.io/arch".to_string(), arch.clone());
        }
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
        let created = Instant::now();
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
            let requested = timeout(p.timeout_s, DEFAULT_BUILD_TIMEOUT_S);
            let limit = build_limit(requested, self.deadline_secs, created.elapsed())?;
            launch(&pod, limit, &bud).await?;
            Ok(BuildReply {
                build_id: id.0.clone(),
                status: BuildStatus::Running,
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

    pub async fn status(&self, sandbox: &SandboxName, p: StatusParams) -> Result<StatusReply> {
        let id = BuildId::parse(&p.build_id)?;
        let (state, log_tail) = match self.lookup(sandbox, &id).await? {
            Found::Live(pod) => (
                build_state(&pod).await?,
                log_tail(&pod, LogName::Build).await?,
            ),
            Found::Ended(pod, state) => match Log::from_container(&pod, SUMMARY_WINDOW).await {
                Ok(log) => (state, summary(&log)),
                Err(e) => {
                    tracing::warn!("reading the ended build {}'s log: {e:#}", id.0);
                    (state, String::new())
                }
            },
        };
        Ok(StatusReply {
            build_id: id.0,
            status: state.status(),
            exit: state.exit(),
            timed_out: state.timed_out(),
            log_tail,
        })
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
        match build_state(&pod).await? {
            BuildState::Exited { code: 0, .. } => {}
            BuildState::Running => bail!("build {} is still building; poll status", id.0),
            state => bail!(
                "build {} failed (exit {}); read its logs, then clean",
                id.0,
                state.exit().map_or("none".to_string(), |c| c.to_string())
            ),
        }
        let alloc = pod
            .exec_capture(&argv(["sh", "-c", ALLOC_SCRIPT, "sh", LOG_DIR]))
            .await?;
        let n: u32 = alloc
            .trim()
            .parse()
            .with_context(|| format!("bad run number {alloc:?}"))?;
        let log = LogName::Run(n);
        let ctr = format!("{}-run{n}", id.0);
        let from = argv(["buildah", "from", "--pull-never", "--name", &ctr, &id.tag()]);
        let mut outcome = step(&pod, log, FROM_TIMEOUT, &from).await?;
        let mut fetched = Ok(Vec::new());
        if outcome.exit == Some(0) {
            let mut cmd = argv(["buildah", "run", &ctr, "--"]);
            cmd.extend(p.cmd);
            let limit = timeout(p.timeout_s, DEFAULT_RUN_TIMEOUT_S);
            outcome = step(&pod, log, limit, &cmd).await?;
            if !paths.is_empty() {
                fetched = self.fetch(caller, &pod, &ctr, &id, &paths).await;
            }
            if let Err(e) = pod.exec_capture(&argv(["buildah", "rm", &ctr])).await {
                tracing::warn!("removing container {ctr}: {e:#}");
            }
        }
        let fetched = fetched?;
        Ok(RunReply {
            log: log.to_string(),
            exit: outcome.exit,
            timed_out: outcome.timed_out,
            log_tail: log_tail(&pod, log).await?,
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

    pub async fn logs(&self, sandbox: &SandboxName, p: LogsParams) -> Result<Page> {
        let id = BuildId::parse(&p.build_id)?;
        let name = LogName::parse(p.which.as_deref().unwrap_or("build"))?;
        let query = p.query()?;
        let max = p
            .max_bytes
            .unwrap_or(DEFAULT_LOG_BYTES)
            .clamp(1024, MAX_LOG_BYTES);
        let log = match self.lookup(sandbox, &id).await? {
            Found::Live(pod) => Log::fetch(&pod, name, MAX_READ).await?,
            Found::Ended(pod, _) if name == LogName::Build => {
                Log::from_container(&pod, MAX_READ).await?
            }
            Found::Ended(..) => bail!("build {}'s pod has ended; only its build log is kept", id.0),
        };
        Ok(log.query(&query, usize_of(max)))
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
        description = "Start building a Dockerfile (params: context, dockerfile, target, build_args, \
        timeout_s); pushes nothing. Returns build_id with status running at once. Poll `status` \
        until succeeded or failed, then `run`/`logs` as needed, and always `clean` the build_id."
    )]
    async fn build(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<BuildParams>,
    ) -> Result<String, String> {
        reply(async { self.broker.build(&caller(&parts)?, p).await }.await)
    }

    #[tool(
        description = "Report a build's status (params: build_id): running, succeeded or failed, \
        with exit, timed_out and the build log's last lines. Poll it after `build`; `clean` when done."
    )]
    async fn status(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<StatusParams>,
    ) -> Result<String, String> {
        reply(async { self.broker.status(&caller(&parts)?.sandbox, p).await }.await)
    }

    #[tool(
        description = "Run a command (params: build_id, cmd, timeout_s, fetch_paths) in a fresh \
        container of a build whose status is succeeded. Returns exit, the \
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
        description = "Read part of a build or run log (params: build_id, which, grep, context, \
        from_line, to_line, tail_lines, max_bytes), as numbered lines: grep with optional context, a from_line/to_line range, or the last tail_lines (default 100). Reports \
        total_lines and matched; output is capped at max_bytes and long lines are clipped. \
        When output overflows max_bytes, tail and grep keep the newest lines (the end of the \
        log) and range keeps the first; truncated is set. \
        Only the newest 4 MiB is read; when truncated_head is set, line 1 is the first whole \
        line of that window."
    )]
    async fn logs(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<LogsParams>,
    ) -> Result<String, String> {
        reply(async { self.broker.logs(&caller(&parts)?.sandbox, p).await }.await)
    }

    #[tool(
        description = "Delete a build's builder pod (params: build_id), or all of this sandbox's \
        builds when build_id is omitted. Call it once done with a build, succeeded or not."
    )]
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
    use std::time::Duration;

    use k8s_openapi::api::core::v1::Pod;

    use crate::mcp::{
        Broker, BuildId, BuildParams, BuildState, BuildStatus, Caller, LogsParams, MCP_PATH,
        PodView, TokenLine, bud_argv, build_limit, parse_tokens, router, unpack,
    };
    use crate::sandbox::{RelPath, SandboxDir, SandboxName, Workspace};

    fn broker(client: kube::Client, tokens: &Path, local: Option<&Path>) -> Arc<Broker> {
        broker_with_deadline(client, tokens, local, 900)
    }

    fn broker_with_deadline(
        client: kube::Client,
        tokens: &Path,
        local: Option<&Path>,
        deadline_secs: i64,
    ) -> Arc<Broker> {
        Arc::new(Broker {
            namespace: client.default_namespace().to_string(),
            client,
            tokens_file: tokens.to_path_buf(),
            default_workdir: Some(SandboxDir::parse("/sandbox/default").unwrap()),
            local_workdir: local.map(Path::to_path_buf),
            resources: Resources::default(),
            deadline_secs,
            max_builds: 4,
            node_arch: None,
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
        for tool in [
            "\"build\"",
            "\"status\"",
            "\"run\"",
            "\"logs\"",
            "\"clean\"",
        ] {
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
    fn log_queries_take_one_mode() {
        let params = |v: serde_json::Value| -> LogsParams {
            let mut v = v;
            v["build_id"] = "b".into();
            serde_json::from_value(v).unwrap()
        };
        let ok = [
            serde_json::json!({}),
            serde_json::json!({ "tail_lines": 5 }),
            serde_json::json!({ "from_line": 2 }),
            serde_json::json!({ "grep": "x", "context": 2 }),
        ];
        for v in ok {
            assert!(params(v.clone()).query().is_ok(), "{v}");
        }
        let bad = [
            serde_json::json!({ "grep": "x", "tail_lines": 5 }),
            serde_json::json!({ "grep": "x", "to_line": 5 }),
            serde_json::json!({ "from_line": 1, "tail_lines": 5 }),
            serde_json::json!({ "context": 1 }),
        ];
        for v in bad {
            assert!(params(v.clone()).query().is_err(), "{v}");
        }
    }

    #[test]
    fn build_target_is_an_alias_of_target() {
        let parse = |v: serde_json::Value| serde_json::from_value::<BuildParams>(v);
        for key in ["target", "build_target"] {
            let p = parse(serde_json::json!({ "context": "svc", key: "base" })).unwrap();
            assert_eq!(p.target.as_deref(), Some("base"), "{key}");
        }
        let p = parse(serde_json::json!({ "context": "svc" })).unwrap();
        assert_eq!(p.target, None);
        let both = serde_json::json!({ "context": "svc", "target": "a", "build_target": "b" });
        assert!(parse(both).is_err());
        let schema = serde_json::to_value(rmcp::schemars::schema_for!(BuildParams)).unwrap();
        assert!(schema["properties"]["target"].is_object(), "{schema}");
        assert_ne!(schema["additionalProperties"], false, "{schema}");
    }

    #[test]
    fn build_states_parse_from_the_state_script() {
        let exited = |code, timed_out| BuildState::Exited { code, timed_out };
        let cases = [
            (
                "running\n",
                BuildState::Running,
                BuildStatus::Running,
                None,
                false,
            ),
            (
                "exit 0\n",
                exited(0, false),
                BuildStatus::Succeeded,
                Some(0),
                false,
            ),
            (
                "exit 1",
                exited(1, false),
                BuildStatus::Failed,
                Some(1),
                false,
            ),
            (
                "exit 124\n",
                exited(124, false),
                BuildStatus::Failed,
                Some(124),
                false,
            ),
            (
                "exit 124 timeout\n",
                exited(124, true),
                BuildStatus::Failed,
                Some(124),
                true,
            ),
            (
                "exit 137 timeout",
                exited(137, true),
                BuildStatus::Failed,
                Some(137),
                true,
            ),
            ("lost\n", BuildState::Lost, BuildStatus::Failed, None, false),
        ];
        for (raw, state, status, exit, timed_out) in cases {
            let parsed = BuildState::parse(raw).unwrap();
            assert_eq!(parsed, state, "{raw:?}");
            assert_eq!(parsed.status(), status, "{raw:?}");
            assert_eq!(parsed.exit(), exit, "{raw:?}");
            assert_eq!(parsed.timed_out(), timed_out, "{raw:?}");
        }
        for bad in [
            "",
            "exit ",
            "exit x",
            "exit 1 2",
            "exit 1 timeout x",
            "exit timeout",
            "Running",
            "done",
        ] {
            assert!(BuildState::parse(bad).is_err(), "{bad:?}");
        }
        assert_eq!(BuildState::Expired.status(), BuildStatus::Failed);
        assert_eq!(BuildState::Expired.exit(), None);
        assert!(BuildState::Expired.timed_out());
        assert_eq!(
            serde_json::to_value([
                BuildStatus::Running,
                BuildStatus::Succeeded,
                BuildStatus::Failed
            ])
            .unwrap(),
            serde_json::json!(["running", "succeeded", "failed"])
        );
    }

    fn pod(status: serde_json::Value, deleting: bool) -> Pod {
        let mut meta = serde_json::json!({ "name": "buildit-1" });
        if deleting {
            meta["deletionTimestamp"] = "2026-10-02T00:00:00Z".into();
        }
        serde_json::from_value(serde_json::json!({ "metadata": meta, "status": status })).unwrap()
    }

    fn terminated(message: Option<&str>) -> serde_json::Value {
        let mut state = serde_json::json!({ "exitCode": 137 });
        if let Some(m) = message {
            state["message"] = m.into();
        }
        serde_json::json!([{
            "name": "builder", "image": "buildah", "imageID": "", "ready": false,
            "restartCount": 0, "state": { "terminated": state }
        }])
    }

    #[test]
    fn ended_pods_still_report_their_build() {
        let view = |status: serde_json::Value| PodView::of(&[pod(status, false)]);
        let expired = serde_json::json!({ "phase": "Failed", "reason": "DeadlineExceeded" });
        assert_eq!(PodView::of(&[]), None);
        assert_eq!(PodView::of(&[pod(expired.clone(), true)]), None);
        assert_eq!(
            view(serde_json::json!({ "phase": "Running" })),
            Some(PodView::Live)
        );
        assert_eq!(
            view(serde_json::json!({ "phase": "Pending" })),
            Some(PodView::Live)
        );
        assert_eq!(view(serde_json::json!({})), Some(PodView::Live));

        let cases = [
            (expired.clone(), BuildState::Expired),
            (
                serde_json::json!({
                    "phase": "Failed", "reason": "DeadlineExceeded",
                    "containerStatuses": terminated(None),
                }),
                BuildState::Expired,
            ),
            (
                serde_json::json!({
                    "phase": "Failed", "reason": "DeadlineExceeded",
                    "containerStatuses": terminated(Some("garbage")),
                }),
                BuildState::Expired,
            ),
            (
                serde_json::json!({
                    "phase": "Failed", "reason": "DeadlineExceeded",
                    "containerStatuses": terminated(Some("124 timeout\n")),
                }),
                BuildState::Exited {
                    code: 124,
                    timed_out: true,
                },
            ),
            (
                serde_json::json!({
                    "phase": "Failed", "reason": "DeadlineExceeded",
                    "containerStatuses": terminated(Some("0\n")),
                }),
                BuildState::Exited {
                    code: 0,
                    timed_out: false,
                },
            ),
            (
                serde_json::json!({ "phase": "Succeeded" }),
                BuildState::Lost,
            ),
            (
                serde_json::json!({ "phase": "Failed", "reason": "Evicted" }),
                BuildState::Lost,
            ),
            (
                serde_json::json!({
                    "phase": "Succeeded", "containerStatuses": terminated(Some("2")),
                }),
                BuildState::Exited {
                    code: 2,
                    timed_out: false,
                },
            ),
        ];
        for (status, want) in cases {
            assert_eq!(view(status.clone()), Some(PodView::Ended(want)), "{status}");
        }

        let Some(PodView::Ended(state)) = view(expired) else {
            panic!("expired pod is not ended");
        };
        assert_eq!(
            (state.status(), state.exit(), state.timed_out()),
            (BuildStatus::Failed, None, true)
        );
        let Some(PodView::Ended(state)) = view(serde_json::json!({ "phase": "Succeeded" })) else {
            panic!("finished pod is not ended");
        };
        assert_eq!(
            (state.status(), state.exit(), state.timed_out()),
            (BuildStatus::Failed, None, false)
        );
    }

    #[test]
    fn build_timeouts_fire_before_the_pod_deadline() {
        let s = Duration::from_secs;
        assert_eq!(build_limit(s(1800), 7200, s(0)).unwrap(), s(1800));
        assert_eq!(build_limit(s(1800), 900, s(0)).unwrap(), s(890));
        assert_eq!(build_limit(s(1800), 900, s(100)).unwrap(), s(790));
        assert_eq!(build_limit(s(60), 900, s(100)).unwrap(), s(60));
        assert_eq!(build_limit(s(1800), 900, s(889)).unwrap(), s(1));
        let left = build_limit(s(1800), 900, Duration::from_millis(100_500)).unwrap();
        assert_eq!(left.as_secs(), 789);
        for (deadline, used) in [(900, 890), (900, 2000), (10, 0), (0, 0), (-5, 0)] {
            assert!(
                build_limit(s(1800), deadline, s(used)).is_err(),
                "{deadline} {used}"
            );
        }
    }

    #[cfg(target_os = "linux")]
    fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
        for _ in 0..200 {
            if done() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("timed out waiting for {what}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn launch_records_exits_timeouts_and_mirrors_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let launch = |name: &str, mirror: Option<&Path>, secs: &str, cmd: &[&str]| {
            let d = dir.path().join(name);
            std::fs::create_dir_all(&d).unwrap();
            let (log, exit, term) = (d.join("logs/build.log"), d.join("exit"), d.join("term"));
            let mirror = mirror.map_or(d.join("mirror"), Path::to_path_buf);
            let status = std::process::Command::new("sh")
                .args(["-c", crate::mcp::LAUNCH_SCRIPT, "sh"])
                .args([&log, &exit, &d.join("pid"), Path::new(secs), &mirror, &term])
                .args(cmd)
                .status()
                .unwrap();
            assert!(status.success(), "{name}");
            wait_for(name, || exit.exists());
            let read = |p: &Path| std::fs::read_to_string(p).unwrap_or_default();
            let log_text = read(&log);
            wait_for(name, || read(&mirror) == log_text);
            assert_eq!(read(&term), read(&exit), "{name}");
            (read(&exit), log_text)
        };

        let (exit, log) = launch("ok", None, "30", &["sh", "-c", "echo hi; exit 3"]);
        assert_eq!(exit, "3\n");
        assert_eq!(log, "$ sh -c echo hi; exit 3\nhi\n");
        let (exit, _) = launch("own-124", None, "30", &["sh", "-c", "exit 124"]);
        assert_eq!(exit, "124\n");
        let (exit, _) = launch("slow", None, "1", &["sleep", "30"]);
        assert_eq!(exit, "124 timeout\n");
        assert_eq!(
            BuildState::parse(&format!("exit {exit}")).unwrap(),
            BuildState::Exited {
                code: 124,
                timed_out: true
            }
        );

        let nowhere = dir.path().join("missing/mirror");
        let d = dir.path().join("no-mirror");
        std::fs::create_dir_all(&d).unwrap();
        let status = std::process::Command::new("sh")
            .args(["-c", crate::mcp::LAUNCH_SCRIPT, "sh"])
            .args([&d.join("build.log"), &d.join("exit"), &d.join("pid")])
            .args([
                Path::new("30"),
                &nowhere,
                &dir.path().join("missing/term"),
                Path::new("true"),
            ])
            .status()
            .unwrap();
        assert!(status.success());
        wait_for("no-mirror", || d.join("exit").exists());
        assert_eq!(std::fs::read_to_string(d.join("exit")).unwrap(), "0\n");
    }

    #[test]
    fn run_logs_allocate_in_sequence_under_a_posix_shell() {
        let dir = tempfile::tempdir().unwrap();
        let alloc = || {
            let out = std::process::Command::new("bash")
                .args(["--posix", "-c", crate::mcp::ALLOC_SCRIPT, "sh"])
                .arg(dir.path())
                .output()
                .unwrap();
            assert!(out.status.success(), "{out:?}");
            String::from_utf8(out.stdout).unwrap()
        };

        assert_eq!(alloc(), "1\n");
        assert_eq!(alloc(), "2\n");
        std::fs::remove_file(dir.path().join("run-1.log")).unwrap();
        assert_eq!(alloc(), "1\n");
        assert_eq!(alloc(), "3\n");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn state_script_reports_zombies_as_lost() {
        let dir = tempfile::tempdir().unwrap();
        let (exit, pid) = (dir.path().join("exit"), dir.path().join("pid"));
        let state = || {
            let out = std::process::Command::new("sh")
                .args(["-c", crate::mcp::STATE_SCRIPT, "sh"])
                .args([&exit, &pid])
                .output()
                .unwrap();
            assert!(out.status.success());
            BuildState::parse(&String::from_utf8(out.stdout).unwrap()).unwrap()
        };

        assert_eq!(state(), BuildState::Lost);
        std::fs::write(&pid, "not-a-pid\n").unwrap();
        assert_eq!(state(), BuildState::Lost);

        let mut sleeper = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        std::fs::write(&pid, format!("{}\n", sleeper.id())).unwrap();
        assert_eq!(state(), BuildState::Running);
        sleeper.kill().unwrap();
        sleeper.wait().unwrap();

        let mut zombie = std::process::Command::new("true").spawn().unwrap();
        let stat = format!("/proc/{}/stat", zombie.id());
        wait_for("a zombie", || {
            std::fs::read_to_string(&stat)
                .is_ok_and(|s| s.rsplit_once(") ").is_some_and(|(_, r)| r.starts_with('Z')))
        });
        std::fs::write(&pid, format!("{}\n", zombie.id())).unwrap();
        assert_eq!(state(), BuildState::Lost);

        std::fs::write(&exit, "124 timeout\n").unwrap();
        assert_eq!(
            state(),
            BuildState::Exited {
                code: 124,
                timed_out: true
            }
        );
        zombie.wait().unwrap();
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

    #[tokio::test]
    async fn builder_pods_pin_the_node_arch_only_when_asked() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = SandboxName::parse("sb-a").unwrap();
        let id = BuildId::parse("buildit-1").unwrap();
        let unpinned = broker(offline_client(), &tokens(dir.path(), ""), None)
            .builder_pod(&sandbox, &id)
            .unwrap();
        let arch = |pod: Pod| {
            pod.spec
                .unwrap()
                .node_selector
                .and_then(|s| s.get("kubernetes.io/arch").cloned())
        };
        assert_eq!(arch(unpinned), None);

        let pinned = Broker {
            node_arch: Some("amd64".to_string()),
            ..Arc::into_inner(broker(offline_client(), &tokens(dir.path(), ""), None)).unwrap()
        };
        assert_eq!(
            arch(pinned.builder_pod(&sandbox, &id).unwrap()).as_deref(),
            Some("amd64")
        );
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
    async fn e2e_build_poll_restart_run_fetch_logs_and_clean() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let context = std::env::var("BUILDIT_E2E_KUBECONTEXT").unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(work.join("svc")).unwrap();
        std::fs::write(
            work.join("svc/Dockerfile"),
            "FROM docker.io/library/busybox:latest AS base\n\
             RUN echo hello-from-build && sleep 20 && mkdir -p /out && echo artifact > /out/a.txt\n\
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
        let args = serde_json::json!({ "context": "svc", "build_target": "base" });
        let built = call(&addr, "tok-a", "build", args).await.unwrap();
        assert_eq!(built["status"], "running", "{built}");
        let id = built["build_id"].as_str().unwrap().to_string();
        let args = serde_json::json!({ "context": "svc", "target": "broken" });
        let broken = call(&addr, "tok-a", "build", args).await.unwrap();
        assert_eq!(broken["status"], "running", "{broken}");
        let broken = broken["build_id"].as_str().unwrap().to_string();

        let only_id = serde_json::json!({ "build_id": id });
        let status = call(&addr, "tok-a", "status", only_id.clone())
            .await
            .unwrap();
        assert_eq!(status["status"], "running", "{status}");
        assert_eq!(status["exit"], serde_json::Value::Null, "{status}");
        let run = serde_json::json!({ "build_id": id, "cmd": ["true"] });
        let e = call(&addr, "tok-a", "run", run).await.unwrap_err();
        assert!(e.contains("still building"), "{e}");
        first.abort();

        // a new server process with no memory of the builds
        let (addr, _second) = serve(cluster().await).await;
        let a = |tool, args| call(&addr, "tok-a", tool, args);
        let b = |tool, args| call(&addr, "tok-b", tool, args);
        let finished = |build_id: String| {
            let addr = addr.clone();
            async move {
                let args = serde_json::json!({ "build_id": build_id });
                for _ in 0..150 {
                    let status = call(&addr, "tok-a", "status", args.clone()).await.unwrap();
                    if status["status"] != "running" {
                        return status;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                panic!("build {build_id} still running after 300s");
            }
        };
        let done = finished(id.clone()).await;
        assert_eq!(done["status"], "succeeded", "{done}");
        assert_eq!(done["exit"], 0, "{done}");
        assert_eq!(done["timed_out"], false, "{done}");
        let tail = done["log_tail"].as_str().unwrap();
        assert!(tail.contains("hello-from-build"), "{done}");
        let failed = finished(broken.clone()).await;
        assert_eq!(failed["status"], "failed", "{failed}");
        assert_ne!(failed["exit"], 0, "{failed}");
        assert!(failed["exit"].is_i64(), "{failed}");
        let run = serde_json::json!({ "build_id": broken, "cmd": ["true"] });
        let e = a("run", run).await.unwrap_err();
        assert!(e.contains("failed (exit"), "{e}");
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
        assert_eq!(grep["matched"], 1, "{grep}");
        assert_eq!(grep["truncated_head"], false, "{grep}");
        let args = serde_json::json!({ "build_id": id, "which": "run:1", "to_line": 1 });
        let line = a("logs", args).await.unwrap();
        assert!(
            line["lines"]
                .as_str()
                .unwrap()
                .starts_with("1:$ buildah from"),
            "{line}"
        );
        let args = serde_json::json!({ "build_id": id, "which": "run:1", "grep": "^out-1$", "context": 1 });
        let ctx = a("logs", args).await.unwrap();
        assert_eq!(ctx["matched"], 1, "{ctx}");
        let ctx = ctx["lines"].as_str().unwrap();
        assert!(
            ctx.contains(":out-1\n") && ctx.contains("-$ buildah run"),
            "{ctx}"
        );
        let args = serde_json::json!({ "build_id": id, "which": "run:2" });
        assert!(a("logs", args.clone()).await.is_err());
        let again = serde_json::json!({ "build_id": id, "cmd": ["sh", "-c", "echo out-2"] });
        let ran = a("run", again).await.unwrap();
        assert_eq!(ran["exit"], 0, "{ran}");
        assert_eq!(ran["log"], "run:2");
        let second = a("logs", args).await.unwrap();
        assert!(
            second["lines"].as_str().unwrap().contains(":out-2\n"),
            "{second}"
        );

        let run = serde_json::json!({ "build_id": id, "cmd": ["true"] });
        for (tool, args) in [
            ("run", run),
            ("logs", only_id.clone()),
            ("status", only_id.clone()),
        ] {
            let e = b(tool, args).await.unwrap_err();
            assert!(e.contains("no live build"), "{tool}: {e}");
        }
        let cleaned = b("clean", only_id.clone()).await.unwrap();
        assert_eq!(cleaned["deleted"], serde_json::json!([]));
        let cleaned = a("clean", serde_json::json!({ "build_id": broken }))
            .await
            .unwrap();
        assert_eq!(cleaned["deleted"], serde_json::json!([broken]));
        let cleaned = a("clean", serde_json::json!({})).await.unwrap();
        assert_eq!(cleaned["deleted"], serde_json::json!([id]));
        for tool in ["logs", "status"] {
            let e = a(tool, only_id.clone()).await.unwrap_err();
            assert!(e.contains("no live build"), "{tool}: {e}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a cluster: BUILDIT_E2E_KUBECONTEXT=kind-x cargo test -- --ignored"]
    async fn e2e_pod_deadline_reports_failed_timed_out() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let context = std::env::var("BUILDIT_E2E_KUBECONTEXT").unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let work = tmp.path().canonicalize().unwrap();
        std::fs::create_dir_all(work.join("svc")).unwrap();
        std::fs::write(
            work.join("svc/Dockerfile"),
            "FROM docker.io/library/busybox:latest\nRUN echo deadline-build && sleep 600\n",
        )
        .unwrap();
        let file = tokens(&work, "tok-a sb-a\n");
        let client = crate::client_for(Some(&context)).await.unwrap();
        let deadline = 120;
        let broker = broker_with_deadline(client.clone(), &file, Some(&work), deadline);
        let pods: kube::Api<Pod> = kube::Api::namespaced(client, &broker.namespace);
        let (addr, _task) = serve(broker).await;
        let a = |tool, args| call(&addr, "tok-a", tool, args);

        let built = a("build", serde_json::json!({ "context": "svc" }))
            .await
            .unwrap();
        let id = built["build_id"].as_str().unwrap().to_string();
        let only_id = serde_json::json!({ "build_id": id });

        let mut status = serde_json::Value::Null;
        for _ in 0..deadline {
            status = a("status", only_id.clone()).await.unwrap();
            if status["status"] != "running" {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert_eq!(status["status"], "failed", "{status}");
        assert_eq!(status["timed_out"], true, "{status}");
        assert_eq!(status["exit"], 124, "{status}");
        let phase = pods.get(&id).await.unwrap().status.unwrap().phase;
        assert_eq!(
            phase.as_deref(),
            Some("Running"),
            "the build timeout fired first"
        );

        let mut expired = false;
        for _ in 0..120 {
            let pod = pods.get(&id).await.unwrap();
            if pod.status.and_then(|s| s.reason).as_deref() == Some("DeadlineExceeded") {
                expired = true;
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        assert!(expired, "pod {id} never hit its deadline");

        let ended = a("status", only_id.clone()).await.unwrap();
        assert_eq!(ended["status"], "failed", "{ended}");
        assert_eq!(ended["timed_out"], true, "{ended}");
        assert_eq!(ended["exit"], 124, "{ended}");
        let tail = ended["log_tail"].as_str().unwrap();
        assert!(tail.contains(":$ buildah bud"), "{ended}");
        assert!(tail.contains("deadline-build"), "{ended}");

        let grep = serde_json::json!({ "build_id": id, "grep": "^deadline-build$" });
        let grep = a("logs", grep).await.unwrap();
        assert_eq!(grep["matched"], 1, "{grep}");
        let run = serde_json::json!({ "build_id": id, "which": "run:1" });
        let e = a("logs", run).await.unwrap_err();
        assert!(e.contains("only its build log"), "{e}");
        let run = serde_json::json!({ "build_id": id, "cmd": ["true"] });
        let e = a("run", run).await.unwrap_err();
        assert!(e.contains("has ended"), "{e}");

        let cleaned = a("clean", only_id.clone()).await.unwrap();
        assert_eq!(cleaned["deleted"], serde_json::json!([id]));
        let e = a("status", only_id).await.unwrap_err();
        assert!(e.contains("no live build"), "{e}");
    }
}
