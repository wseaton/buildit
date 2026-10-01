use std::collections::BTreeMap;
use std::fmt;
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
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::Api;
use kube::api::{ApiResource, DeleteParams, DynamicObject, ListParams};
use kube::core::GroupVersion;
use rmcp::handler::server::common::Extension;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ServerHandler, schemars, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::backend::{BACKEND_LABEL, Backend, PodMeta, PodOpts, Resources};
use crate::build::POD_READY_TIMEOUT;
use crate::buildlog::{
    LOG_DIR, LogName, LogPage, LogQuery, LogSelector, LogSummary, LogText, QueryParams, READ_LIMIT,
    alloc_run_argv, list_argv, note_argv, parse_list, parse_run_number, read_argv, step_argv,
    summarize,
};
use crate::pod::{BuilderPod, ExecStatus};
use crate::sandbox::{RESULTS_DIR, RelPath, SandboxDir, SandboxName, Workspace};

pub const MCP_PATH: &str = "/mcp";

const SANDBOX_HOSTS: [&str; 2] = ["host.containers.internal", "host.openshell.internal"];

const DEFAULT_BIND: &str = "0.0.0.0:8849";
const DEFAULT_NAME: &str = "buildit";

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
const MAX_OWNER_DEPTH: usize = 4;
const DELETE_GRACE_S: u32 = 1;
const CALL_DRAIN: Duration = Duration::from_secs(20);

#[derive(Args)]
pub struct McpArgs {
    /// Listen address for the streamable-http endpoint [env: MCP_BIND, BROKER_BIND]
    #[arg(long)]
    pub bind: Option<String>,
    /// Token file of `<token> <sandbox> [<workdir>]` lines, re-read on every request
    #[arg(long, env = "MCP_TOKENS_FILE", conflicts_with = "dev_sandbox")]
    pub tokens_file: Option<PathBuf>,
    /// Sandbox path holding the agent's tree, for token lines that name no workdir
    #[arg(long, env = "BROKER_SANDBOX_WORKDIR")]
    pub sandbox_workdir: Option<String>,
    /// Dev: use a local directory in place of the openshell sandbox
    #[arg(long, value_name = "DIR")]
    pub local_workdir: Option<PathBuf>,
    /// Dev: the sandbox a BROKER_TOKEN caller acts as
    #[arg(long, value_name = "NAME", requires = "local_workdir")]
    pub dev_sandbox: Option<String>,
    /// Server name reported to clients
    #[arg(long, env = "MCP_NAME", default_value = DEFAULT_NAME)]
    pub name: String,
    /// Tools to serve, comma-separated (default: all)
    #[arg(long, env = "MCP_TOOLS", value_delimiter = ',')]
    pub tools: Vec<String>,
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

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn identity(
    tokens_file: Option<PathBuf>,
    dev_sandbox: Option<&str>,
    dev_token: Option<String>,
) -> Result<Identity> {
    if let Some(file) = tokens_file {
        if dev_token.is_some() {
            tracing::info!(
                "ignoring BROKER_TOKEN; callers come from {}",
                file.display()
            );
        }
        return Ok(Identity::TokenFile(file));
    }
    let Some(sandbox) = dev_sandbox else {
        bail!(
            "set MCP_TOKENS_FILE (or, for local dev, --local-workdir with --dev-sandbox and \
             BROKER_TOKEN)"
        );
    };
    let token = dev_token.ok_or_else(|| {
        anyhow!("--dev-sandbox needs BROKER_TOKEN; refusing to serve unauthenticated")
    })?;
    Ok(Identity::Dev {
        token,
        sandbox: SandboxName::parse(sandbox)?,
    })
}

pub async fn serve(args: McpArgs) -> Result<()> {
    let identity = identity(
        args.tokens_file,
        args.dev_sandbox.as_deref(),
        env_nonempty("BROKER_TOKEN"),
    )?;
    let workspace = match &args.local_workdir {
        Some(dir) => WorkspaceKind::Local {
            dir: dir
                .canonicalize()
                .with_context(|| format!("resolving {}", dir.display()))?,
        },
        None => WorkspaceKind::Openshell {
            default_workdir: args
                .sandbox_workdir
                .as_deref()
                .map(SandboxDir::parse)
                .transpose()?,
        },
    };
    let tools = Tools::select(&args.name, &args.tools)?;
    let bind = args
        .bind
        .or_else(|| env_nonempty("MCP_BIND"))
        .or_else(|| env_nonempty("BROKER_BIND"))
        .unwrap_or_else(|| DEFAULT_BIND.to_string());
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
        identity,
        workspace,
        backend: args.backend,
        resources: Resources {
            requests: args.requests,
            limits: args.limits,
        },
        deadline_secs: args.pod_deadline,
        max_builds: args.max_builds.max(1),
        owner,
        calls: TaskTracker::new(),
    });

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(
        "{} mcp listening on http://{bind}{MCP_PATH} (namespace {}, backend {:?}, tools {})",
        tools.name,
        broker.namespace,
        broker.backend,
        tools.names().join(",")
    );
    let hosts = allowed_hosts(env_hosts().as_deref());
    serve_until(
        listener,
        broker,
        tools,
        hosts,
        shutdown_signal()?,
        SHUTDOWN_GRACE,
    )
    .await
}

async fn serve_until(
    listener: tokio::net::TcpListener,
    broker: Arc<Broker>,
    tools: Tools,
    hosts: Vec<String>,
    shutdown: impl Future<Output = ()>,
    grace: Duration,
) -> Result<()> {
    let ct = CancellationToken::new();
    let calls = broker.calls.clone();
    let app = router(broker, tools, hosts, ct.clone());
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
    calls.close();
    if tokio::time::timeout(CALL_DRAIN, calls.wait())
        .await
        .is_err()
    {
        tracing::warn!("{} calls still running after {CALL_DRAIN:?}", calls.len());
    }
    served.context("serving buildit mcp")
}

pub fn router(
    broker: Arc<Broker>,
    tools: Tools,
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
    let served = broker.clone();
    let service: StreamableHttpService<BuilditMcp, NeverSessionManager> =
        StreamableHttpService::new(
            move || Ok(BuilditMcp::new(served.clone(), tools.clone())),
            Arc::new(NeverSessionManager::default()),
            config,
        );
    axum::Router::new()
        .nest_service(MCP_PATH, service)
        .layer(axum::middleware::from_fn_with_state(broker, authenticate))
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
    match workload_owner(client, namespace, &name).await {
        Ok(owner) => {
            tracing::info!("builder pods are owned by {} {}", owner.kind, owner.name);
            Some(owner)
        }
        Err(e) => {
            tracing::warn!("cannot read own pod {name} ({e:#}); builder pods get no owner");
            None
        }
    }
}

// the top of the pod's controller chain, e.g. Pod -> ReplicaSet -> Deployment
async fn workload_owner(
    client: kube::Client,
    namespace: &str,
    pod: &str,
) -> Result<OwnerReference> {
    let me = Api::<Pod>::namespaced(client.clone(), namespace)
        .get(pod)
        .await
        .with_context(|| format!("reading pod {pod}"))?;
    let mut owner = OwnerReference {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        name: pod.to_string(),
        uid: me
            .metadata
            .uid
            .ok_or_else(|| anyhow!("pod {pod} has no uid"))?,
        ..Default::default()
    };
    let mut refs = me.metadata.owner_references.unwrap_or_default();
    for _ in 0..MAX_OWNER_DEPTH {
        let Some(up) = refs.into_iter().find(|r| r.controller == Some(true)) else {
            break;
        };
        owner = OwnerReference {
            api_version: up.api_version,
            kind: up.kind,
            name: up.name,
            uid: up.uid,
            ..Default::default()
        };
        refs = match controller_refs(client.clone(), namespace, &owner).await {
            Ok(refs) => refs,
            Err(e) => {
                tracing::warn!(
                    "cannot read {} {} ({e:#}); owning builder pods by it",
                    owner.kind,
                    owner.name
                );
                break;
            }
        };
    }
    Ok(owner)
}

async fn controller_refs(
    client: kube::Client,
    namespace: &str,
    of: &OwnerReference,
) -> Result<Vec<OwnerReference>> {
    let gvk = of
        .api_version
        .parse::<GroupVersion>()
        .with_context(|| format!("apiVersion {:?}", of.api_version))?
        .with_kind(&of.kind);
    let api: Api<DynamicObject> =
        Api::namespaced_with(client, namespace, &ApiResource::from_gvk(&gvk));
    let obj = api.get(&of.name).await?;
    if obj.metadata.uid.as_deref() != Some(of.uid.as_str()) {
        bail!("uid changed");
    }
    Ok(obj.metadata.owner_references.unwrap_or_default())
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

async fn authenticate(State(broker): State<Arc<Broker>>, mut req: Request, next: Next) -> Response {
    match broker.caller(req.headers()).await {
        Ok(caller) => {
            req.extensions_mut().insert(caller);
            next.run(req).await
        }
        Err(Denied::Unauthorized) => (
            StatusCode::UNAUTHORIZED,
            [(header::CONTENT_TYPE, "application/json")],
            r#"{"error":"missing or unknown bearer token"}"#,
        )
            .into_response(),
        Err(Denied::Unavailable(e)) => {
            tracing::error!("cannot identify the caller: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "application/json")],
                r#"{"error":"the server cannot identify callers right now"}"#,
            )
                .into_response()
        }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct TokenLine {
    token: String,
    sandbox: SandboxName,
    workdir: Option<SandboxDir>,
}

// `<token> <sandbox> [<workdir>]` per line; one bad line refuses the whole file
fn parse_tokens(text: &str) -> Result<Vec<TokenLine>> {
    let mut lines: Vec<TokenLine> = Vec::new();
    for (index, raw) in text.lines().enumerate() {
        let n = index + 1;
        let mut fields = raw.split_whitespace();
        let (token, sandbox, workdir) =
            match (fields.next(), fields.next(), fields.next(), fields.next()) {
                (None, ..) => continue,
                (Some(token), Some(sandbox), workdir, None) => (token, sandbox, workdir),
                _ => bail!("token file line {n} is not `<token> <sandbox> [<workdir>]`"),
            };
        if lines.iter().any(|l| l.token == token) {
            bail!("token file line {n} repeats a token");
        }
        lines.push(TokenLine {
            token: token.to_string(),
            sandbox: SandboxName::parse(sandbox).with_context(|| format!("token file line {n}"))?,
            workdir: workdir
                .map(SandboxDir::parse)
                .transpose()
                .with_context(|| format!("token file line {n}"))?,
        });
    }
    Ok(lines)
}

// compares every line in constant time, so timing leaks neither which line matched nor a prefix
fn match_token<'a>(lines: &'a [TokenLine], token: &str) -> Option<&'a TokenLine> {
    let mut found = None;
    for line in lines {
        if constant_time_eq(line.token.as_bytes(), token.as_bytes()) {
            found = Some(line);
        }
    }
    found
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

// pods created in the same second as `id` count as older
fn within_cap(live: &[Pod], cap: usize, id: &BuildId) -> bool {
    let created = |p: &Pod| p.metadata.creation_timestamp.as_ref().map(|t| t.0);
    let Some(me) = live
        .iter()
        .find(|p| label(p, LABEL_BUILD_ID) == Some(id.as_str()))
    else {
        return false;
    };
    let mine = created(me);
    let older = live
        .iter()
        .filter(|p| label(p, LABEL_BUILD_ID) != Some(id.as_str()) && created(p) <= mine)
        .count();
    older < cap
}

fn delete_params() -> DeleteParams {
    DeleteParams {
        grace_period_seconds: Some(DELETE_GRACE_S),
        ..DeleteParams::default()
    }
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

pub enum Identity {
    TokenFile(PathBuf),
    Dev { token: String, sandbox: SandboxName },
}

pub enum WorkspaceKind {
    Openshell { default_workdir: Option<SandboxDir> },
    Local { dir: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub sandbox: SandboxName,
    pub workspace: Workspace,
}

#[derive(Debug)]
pub enum Denied {
    Unauthorized,
    Unavailable(anyhow::Error),
}

pub struct Broker {
    client: kube::Client,
    namespace: String,
    identity: Identity,
    workspace: WorkspaceKind,
    backend: Backend,
    resources: Resources,
    deadline_secs: i64,
    max_builds: usize,
    owner: Option<OwnerReference>,
    // in-flight tool calls, drained on shutdown
    calls: TaskTracker,
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

// a step that fails at or past its limit timed out
fn step_outcome(
    status: Result<Result<ExecStatus>, tokio::time::error::Elapsed>,
    limit: Duration,
    elapsed: Duration,
) -> StepOutcome {
    match status {
        Ok(Ok(s)) => {
            let timed_out = !s.success() && elapsed >= limit;
            StepOutcome {
                exit: s.code,
                timed_out,
                error: timed_out.then(|| format!("buildit: timed out after {}s", limit.as_secs())),
            }
        }
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
    pub async fn caller(&self, headers: &HeaderMap) -> Result<Caller, Denied> {
        let token = bearer(headers).ok_or(Denied::Unauthorized)?;
        let (sandbox, workdir) = match &self.identity {
            Identity::Dev {
                token: want,
                sandbox,
            } => {
                if !constant_time_eq(token.as_bytes(), want.as_bytes()) {
                    return Err(Denied::Unauthorized);
                }
                (sandbox.clone(), None)
            }
            Identity::TokenFile(path) => {
                let text = tokio::fs::read_to_string(path)
                    .await
                    .with_context(|| format!("reading {}", path.display()))
                    .map_err(Denied::Unavailable)?;
                let lines = parse_tokens(&text)
                    .with_context(|| path.display().to_string())
                    .map_err(Denied::Unavailable)?;
                let line = match_token(&lines, token).ok_or(Denied::Unauthorized)?;
                (line.sandbox.clone(), line.workdir.clone())
            }
        };
        let workspace = match &self.workspace {
            WorkspaceKind::Local { dir } => Workspace::Local { dir: dir.clone() },
            WorkspaceKind::Openshell { default_workdir } => Workspace::Openshell {
                workdir: workdir.or_else(|| default_workdir.clone()).ok_or_else(|| {
                    Denied::Unavailable(anyhow!(
                        "sandbox {} has no workdir in the token file and \
                             BROKER_SANDBOX_WORKDIR is unset",
                        sandbox.as_str()
                    ))
                })?,
            },
        };
        Ok(Caller { sandbox, workspace })
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

    // deletes every sandbox's ended builder pods
    async fn sweep_ended(&self) {
        let pods = match self.list(&[(LABEL_MANAGED_BY, MANAGED_BY)]).await {
            Ok(pods) => pods,
            Err(e) => {
                tracing::warn!("sweeping ended builder pods: {e:#}");
                return;
            }
        };
        for pod in pods
            .iter()
            .filter(|p| !alive(p) && p.metadata.deletion_timestamp.is_none())
        {
            let name = pod_name(pod);
            if let Err(e) = self.pods().delete(name, &delete_params()).await {
                tracing::warn!("deleting ended builder pod {name}: {e:#}");
            }
        }
    }

    // pods of other sandboxes look exactly like missing ones
    async fn find(&self, caller: &SandboxName, id: &BuildId) -> Result<LiveBuild> {
        let gone = || anyhow!("no live build {id}; build again");
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
            let why = pod
                .status
                .as_ref()
                .and_then(|s| s.reason.as_deref())
                .unwrap_or("pod ended");
            bail!("build {id} is gone ({why}); build again");
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

    async fn publish(&self, caller: &Caller, results: &Path) -> Result<()> {
        tokio::task::block_in_place(|| caller.workspace.publish(&caller.sandbox, results))
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
        std::fs::write(results.join(&file), text.copy())
            .with_context(|| format!("writing {file}"))?;
        summarize(&text, log, results_path(id, &file), failed)
    }

    pub async fn build(
        &self,
        caller: &Caller,
        params: BuildParams,
        ct: &CancellationToken,
    ) -> Result<BuildReply> {
        let req = BuildRequest::parse(params)?;
        self.sweep_ended().await;
        self.check_cap(&caller.sandbox, None).await?;
        let id = BuildId::fresh();
        let stage = tempfile::tempdir().context("creating staging dir")?;
        let ctx_dir = stage.path().join("ctx");
        let results = stage.path().join(id.as_str());
        std::fs::create_dir_all(&results).context("creating results dir")?;

        tokio::task::block_in_place(|| {
            caller
                .workspace
                .fetch_context(&caller.sandbox, &req.context, &ctx_dir)
        })?;
        let tarball = crate::context::tarball(&ctx_dir)?;
        tracing::info!(
            "build {id}: sandbox {} context {} ({} KiB)",
            caller.sandbox.as_str(),
            req.context.as_str(),
            tarball.len() / 1024
        );

        let meta = PodMeta {
            labels: pod_labels(&caller.sandbox, &id),
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
        let reply = tokio::select! {
            biased;
            () = ct.cancelled() => Err(anyhow!("call cancelled")),
            reply = self.finish_build(caller, &pod, &id, &req, &tarball, &results) => reply,
        };
        if reply.is_err()
            && let Err(e) = self.pods().delete(&pod.name, &delete_params()).await
        {
            tracing::warn!("deleting builder pod {id}: {e:#}");
        }
        reply
    }

    async fn finish_build(
        &self,
        caller: &Caller,
        pod: &BuilderPod,
        id: &BuildId,
        req: &BuildRequest,
        tarball: &[u8],
        results: &Path,
    ) -> Result<BuildReply> {
        self.check_cap(&caller.sandbox, Some(id)).await?;
        pod.wait_ready(POD_READY_TIMEOUT).await?;

        let started = Instant::now();
        let outcome = step_outcome(
            tokio::time::timeout(
                req.timeout + EXEC_SLACK,
                self.drive_build(pod, req, tarball),
            )
            .await,
            req.timeout,
            started.elapsed(),
        );
        if let Some(line) = &outcome.error {
            self.note(pod, self.backend, LogName::Build, line).await;
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
                pod,
                self.backend,
                LogName::Build,
                id,
                results,
                !outcome.success(),
            )
            .await?;
        self.publish(caller, results).await?;
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

    pub async fn run(&self, caller: &Caller, params: RunParams) -> Result<RunReply> {
        if params.cmd.is_empty() {
            bail!("cmd must not be empty");
        }
        let id = BuildId::parse(&params.build_id)?;
        let paths = params
            .fetch_paths
            .iter()
            .map(|p| RelPath::parse_container(p))
            .collect::<Result<Vec<_>>>()?;
        let build = self.find(&caller.sandbox, &id).await?;
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
        let from_limit = Duration::from_secs(FROM_TIMEOUT_S);
        let started = Instant::now();
        let from = step_outcome(
            tokio::time::timeout(
                from_limit + EXEC_SLACK,
                pod.exec_status(&step_argv(
                    shell,
                    log,
                    FROM_TIMEOUT_S,
                    &from_argv(&id.tag(), &ctr),
                )),
            )
            .await,
            from_limit,
            started.elapsed(),
        );
        let outcome = if from.success() {
            let started = Instant::now();
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
                limit,
                started.elapsed(),
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
            match self.pods().delete(name, &delete_params()).await {
                Ok(_) => deleted.push(label(&pod, LABEL_BUILD_ID).unwrap_or(name).to_string()),
                Err(e) => tracing::warn!("deleting builder pod {name}: {e:#}"),
            }
        }
        deleted.sort();
        Ok(deleted)
    }
}

#[derive(Clone)]
pub struct Tools {
    name: Arc<str>,
    router: Arc<ToolRouter<BuilditMcp>>,
}

impl Tools {
    // an empty selection serves every tool
    pub fn select(name: &str, wanted: &[String]) -> Result<Self> {
        let mut router = BuilditMcp::tool_router();
        let wanted: Vec<&str> = wanted
            .iter()
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .collect();
        if let Some(unknown) = wanted.iter().find(|t| !router.has_route(t)) {
            bail!("unknown tool {unknown:?} in MCP_TOOLS");
        }
        if !wanted.is_empty() {
            for tool in router.list_all() {
                if !wanted.contains(&tool.name.as_ref()) {
                    router.remove_route(&tool.name);
                }
            }
        }
        Ok(Self {
            name: name.into(),
            router: Arc::new(router),
        })
    }

    fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .router
            .list_all()
            .into_iter()
            .map(|t| t.name.to_string())
            .collect();
        names.sort();
        names
    }
}

#[derive(Clone)]
pub struct BuilditMcp {
    broker: Arc<Broker>,
    tools: Tools,
}

fn authenticated(parts: &Parts) -> Result<Caller> {
    parts
        .extensions
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| anyhow!("request reached a tool without an authenticated caller"))
}

async fn cancellable<T>(
    ct: &CancellationToken,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        biased;
        () = ct.cancelled() => Err(anyhow!("call cancelled")),
        result = work => result,
    }
}

fn reply<T: Serialize>(result: Result<T>) -> Result<String, String> {
    result
        .and_then(|r| serde_json::to_string(&r).context("serializing reply"))
        .map_err(|e| format!("{e:#}"))
}

#[tool_router]
impl BuilditMcp {
    pub fn new(broker: Arc<Broker>, tools: Tools) -> Self {
        Self { broker, tools }
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
        ct: CancellationToken,
        Parameters(params): Parameters<BuildParams>,
    ) -> Result<String, String> {
        let _call = self.broker.calls.token();
        let broker = &self.broker;
        reply(
            async {
                let caller = authenticated(&parts)?;
                broker.build(&caller, params, &ct).await
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
        ct: CancellationToken,
        Parameters(params): Parameters<RunParams>,
    ) -> Result<String, String> {
        let _call = self.broker.calls.token();
        let broker = &self.broker;
        reply(
            cancellable(&ct, async {
                let caller = authenticated(&parts)?;
                broker.run(&caller, params).await
            })
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
        ct: CancellationToken,
        Parameters(params): Parameters<LogsParams>,
    ) -> Result<String, String> {
        let _call = self.broker.calls.token();
        let broker = &self.broker;
        reply(
            cancellable(&ct, async {
                let caller = authenticated(&parts)?;
                broker.logs(&caller.sandbox, params).await
            })
            .await,
        )
    }

    #[tool(description = "Delete a build's builder pod, or all of this sandbox's builds.")]
    async fn clean(
        &self,
        Extension(parts): Extension<Parts>,
        ct: CancellationToken,
        Parameters(params): Parameters<CleanParams>,
    ) -> Result<String, String> {
        let _call = self.broker.calls.token();
        let broker = &self.broker;
        reply(
            cancellable(&ct, async {
                let caller = authenticated(&parts)?;
                let deleted = broker
                    .clean(&caller.sandbox, params.build_id.as_deref())
                    .await?;
                Ok(CleanReply { deleted })
            })
            .await,
        )
    }
}

#[tool_handler(router = self.tools.router)]
impl ServerHandler for BuilditMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new(self.tools.name.as_ref(), env!("CARGO_PKG_VERSION")),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
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
        Broker, BuildId, BuildParams, BuildRequest, Caller, Denied, Identity, LABEL_BUILD_ID,
        MCP_PATH, McpArgs, TokenLine, Tools, WorkspaceKind, alive, allowed_hosts, bearer,
        constant_time_eq, fetch_argv, from_argv, identity, kube_client, match_token, owned_by,
        parse_tokens, rm_argv, router, run_argv, selector, serve_until, step_outcome,
        unpack_fetched, within_cap, workload_owner,
    };
    use crate::sandbox::{RelPath, SandboxDir, SandboxName, Workspace};

    const SANDBOXES: [&str; 7] = [
        "ci-1-abc", "sb-a", "sb-b", "sb-down", "sb-gone", "sb-new", "sb-old",
    ];

    fn token_for(sandbox: &str) -> String {
        format!("tok-{sandbox}")
    }

    struct Tokens {
        dir: tempfile::TempDir,
        path: PathBuf,
    }

    impl Tokens {
        fn with(body: &str) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tokens");
            let tokens = Self { dir, path };
            tokens.write(body);
            tokens
        }

        fn all() -> Self {
            Self::with(
                &SANDBOXES
                    .iter()
                    .map(|s| format!("{} {s}\n", token_for(s)))
                    .collect::<String>(),
            )
        }

        // replaced by rename, the way crucible writes it
        fn write(&self, body: &str) {
            let tmp = self.dir.path().join("tokens.tmp");
            std::fs::write(&tmp, body).unwrap();
            std::fs::rename(&tmp, &self.path).unwrap();
        }

        fn identity(&self) -> Identity {
            Identity::TokenFile(self.path.clone())
        }
    }

    fn params(context: &str) -> BuildParams {
        BuildParams {
            context: context.to_string(),
            dockerfile: None,
            target: None,
            build_args: BTreeMap::new(),
            timeout_s: None,
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> axum::http::HeaderMap {
        let mut map = axum::http::HeaderMap::new();
        for (k, v) in pairs {
            map.append(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        map
    }

    #[test]
    fn bearer_takes_only_the_bearer_scheme() {
        let got = |v: &str| bearer(&headers(&[("authorization", v)])).map(str::to_string);
        assert_eq!(got("Bearer s3cr3t").as_deref(), Some("s3cr3t"));
        assert_eq!(got("s3cr3t"), None);
        assert_eq!(got("Bearer "), None);
        assert_eq!(got("bearer s3cr3t"), None);
        assert_eq!(got("Basic czNjcjN0"), None);
        assert_eq!(bearer(&headers(&[])), None);
        assert_eq!(bearer(&headers(&[("x-crucible-sandbox", "sb-a")])), None);
        assert!(constant_time_eq(b"", b""));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }

    #[test]
    fn token_lines_take_an_optional_workdir() {
        let lines =
            parse_tokens("\n  tok-a sb-a /sandbox/task-0123abcd\n\ttok-b   sb-b  \n\n").unwrap();
        assert_eq!(
            lines,
            [
                TokenLine {
                    token: "tok-a".to_string(),
                    sandbox: SandboxName::parse("sb-a").unwrap(),
                    workdir: Some(SandboxDir::parse("/sandbox/task-0123abcd").unwrap()),
                },
                TokenLine {
                    token: "tok-b".to_string(),
                    sandbox: SandboxName::parse("sb-b").unwrap(),
                    workdir: None,
                },
            ]
        );
        assert_eq!(parse_tokens("").unwrap(), []);
        for (body, want) in [
            ("tok-a\n", "line 1 is not"),
            ("tok-a sb-a /sandbox x\n", "line 1 is not"),
            ("tok-a sb-a\ntok-a sb-b\n", "line 2 repeats a token"),
            ("tok-a --gateway=x\n", "invalid sandbox name"),
            ("tok-a sb-a\ntok-b sb-b sandbox/rel\n", "must be absolute"),
            ("tok-a sb-a /sandbox/../etc\n", "invalid sandbox workdir"),
        ] {
            let err = format!("{:#}", parse_tokens(body).unwrap_err());
            assert!(err.contains(want), "{body:?}: {err}");
        }
    }

    #[test]
    fn a_token_matches_only_its_own_line() {
        let lines = parse_tokens("tok-a sb-a\ntok-b sb-b\n").unwrap();
        let sandbox = |t: &str| match_token(&lines, t).map(|l| l.sandbox.as_str().to_string());
        assert_eq!(sandbox("tok-a").as_deref(), Some("sb-a"));
        assert_eq!(sandbox("tok-b").as_deref(), Some("sb-b"));
        assert_eq!(sandbox("tok-"), None, "a prefix is not a match");
        assert_eq!(sandbox("tok-ab"), None);
        assert_eq!(sandbox("sb-a"), None, "a sandbox name is not a token");
        assert_eq!(sandbox(""), None);
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

        // two builds racing in one second: neither may count on being first
        let racing = [
            pod("buildit-y", "sb-a", "2026-10-01T10:00:07Z", None),
            pod("buildit-x", "sb-a", "2026-10-01T10:00:07Z", None),
        ];
        assert!(!within_cap(&racing, 1, &id("buildit-x")));
        assert!(!within_cap(&racing, 1, &id("buildit-y")));
        assert!(within_cap(&racing, 2, &id("buildit-y")));
        // the one that listed before the other existed goes ahead
        assert!(within_cap(&racing[1..], 1, &id("buildit-x")));
        let earlier = [
            pod("buildit-y", "sb-a", "2026-10-01T10:00:08Z", None),
            pod("buildit-x", "sb-a", "2026-10-01T10:00:07Z", None),
        ];
        assert!(within_cap(&earlier, 1, &id("buildit-x")));
        assert!(!within_cap(&earlier, 1, &id("buildit-y")));
    }

    #[tokio::test]
    async fn timeouts_come_from_the_clock_not_the_exit_code() {
        let limit = Duration::from_secs(60);
        let exited = |code: i32| {
            Ok(Ok(crate::pod::ExecStatus {
                code: Some(code),
                message: String::new(),
            }))
        };
        let at = Duration::from_secs;

        let quick_124 = step_outcome(exited(124), limit, at(3));
        assert_eq!((quick_124.exit, quick_124.timed_out), (Some(124), false));
        assert_eq!(quick_124.error, None);
        for code in [124, 143, 137] {
            let killed = step_outcome(exited(code), limit, at(60));
            assert_eq!((killed.exit, killed.timed_out), (Some(code), true));
            assert_eq!(
                killed.error.as_deref(),
                Some("buildit: timed out after 60s")
            );
        }
        let ok_late = step_outcome(exited(0), limit, at(61));
        assert!(ok_late.success() && !ok_late.timed_out && ok_late.error.is_none());

        let failed = step_outcome(Ok(Err(anyhow::anyhow!("exec broke"))), limit, at(1));
        assert!(!failed.timed_out && failed.exit.is_none());
        assert_eq!(failed.error.as_deref(), Some("buildit: exec broke"));

        let hung = tokio::time::timeout(
            Duration::ZERO,
            std::future::pending::<anyhow::Result<crate::pod::ExecStatus>>(),
        )
        .await;
        let hung = step_outcome(hung, limit, at(120));
        assert!(hung.timed_out && hung.exit.is_none());
        assert_eq!(
            hung.error.as_deref(),
            Some("buildit: timed out waiting for the builder")
        );
    }

    fn broker(identity: Identity, workspace: WorkspaceKind) -> Arc<Broker> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = kube::Config::new("http://127.0.0.1:9".parse().unwrap());
        Arc::new(Broker {
            client: kube::Client::try_from(config).unwrap(),
            namespace: "builds".to_string(),
            identity,
            workspace,
            backend: Backend::Buildah,
            resources: Resources::default(),
            deadline_secs: 600,
            max_builds: 2,
            owner: None,
            calls: tokio_util::task::TaskTracker::new(),
        })
    }

    fn local(dir: &Path) -> WorkspaceKind {
        WorkspaceKind::Local {
            dir: dir.to_path_buf(),
        }
    }

    fn openshell(default_workdir: Option<&str>) -> WorkspaceKind {
        WorkspaceKind::Openshell {
            default_workdir: default_workdir.map(|d| SandboxDir::parse(d).unwrap()),
        }
    }

    fn openshell_caller(sandbox: &str, workdir: &str) -> Caller {
        Caller {
            sandbox: SandboxName::parse(sandbox).unwrap(),
            workspace: Workspace::Openshell {
                workdir: SandboxDir::parse(workdir).unwrap(),
            },
        }
    }

    async fn caller_as(broker: &Broker, pairs: &[(&str, &str)]) -> Result<Caller, String> {
        broker.caller(&headers(pairs)).await.map_err(|e| match e {
            Denied::Unauthorized => "unauthorized".to_string(),
            Denied::Unavailable(e) => format!("unavailable: {e:#}"),
        })
    }

    #[tokio::test]
    async fn token_a_cannot_act_as_sandbox_b() {
        let tokens = Tokens::with("tok-a sb-a /sandbox/task-aaaa\ntok-b sb-b /sandbox/task-bbbb\n");
        let broker = broker(tokens.identity(), openshell(None));
        let a = openshell_caller("sb-a", "/sandbox/task-aaaa");
        assert_eq!(
            caller_as(&broker, &[("authorization", "Bearer tok-a")]).await,
            Ok(a.clone())
        );
        assert_eq!(
            caller_as(
                &broker,
                &[
                    ("authorization", "Bearer tok-a"),
                    ("x-crucible-sandbox", "sb-b"),
                ]
            )
            .await,
            Ok(a),
            "a sandbox header does not override the token"
        );
        for pairs in [
            &[("x-crucible-sandbox", "sb-b")][..],
            &[("authorization", "Bearer sb-b")],
            &[("authorization", "Bearer tok-")],
            &[("authorization", "Bearer tok-a tok-b")],
            &[("authorization", "tok-a")],
            &[],
        ] {
            assert_eq!(
                caller_as(&broker, pairs).await,
                Err("unauthorized".to_string()),
                "{pairs:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_token_file_rewrite_takes_effect_on_the_next_request() {
        let tokens = Tokens::with("tok-a sb-a /sandbox/one\n");
        let broker = broker(tokens.identity(), openshell(None));
        let a = [("authorization", "Bearer tok-a")];
        let c = [("authorization", "Bearer tok-c")];
        assert_eq!(
            caller_as(&broker, &a).await,
            Ok(openshell_caller("sb-a", "/sandbox/one"))
        );
        assert_eq!(
            caller_as(&broker, &c).await,
            Err("unauthorized".to_string())
        );

        tokens.write("tok-a sb-a /sandbox/two\ntok-c sb-c /sandbox/three\n");
        assert_eq!(
            caller_as(&broker, &a).await,
            Ok(openshell_caller("sb-a", "/sandbox/two"))
        );
        assert_eq!(
            caller_as(&broker, &c).await,
            Ok(openshell_caller("sb-c", "/sandbox/three"))
        );

        tokens.write("tok-c sb-c /sandbox/three\n");
        assert_eq!(
            caller_as(&broker, &a).await,
            Err("unauthorized".to_string())
        );

        tokens.write("tok-c sb-c /sandbox/three\ntok-c sb-d /sandbox/four\n");
        let err = caller_as(&broker, &c).await.unwrap_err();
        assert!(err.contains("repeats a token"), "{err}");

        std::fs::remove_file(&tokens.path).unwrap();
        let err = caller_as(&broker, &c).await.unwrap_err();
        assert!(err.starts_with("unavailable: reading"), "{err}");
    }

    #[tokio::test]
    async fn a_two_field_line_uses_the_default_workdir() {
        let tokens = Tokens::with("tok-a sb-a\ntok-b sb-b /sandbox/task-bbbb\n");
        let a = [("authorization", "Bearer tok-a")];
        let b = [("authorization", "Bearer tok-b")];
        let with_default = broker(tokens.identity(), openshell(Some("/sandbox/repo/")));
        assert_eq!(
            caller_as(&with_default, &a).await,
            Ok(openshell_caller("sb-a", "/sandbox/repo"))
        );
        assert_eq!(
            caller_as(&with_default, &b).await,
            Ok(openshell_caller("sb-b", "/sandbox/task-bbbb")),
            "a line's own workdir beats the default"
        );
        let without = broker(tokens.identity(), openshell(None));
        let err = caller_as(&without, &a).await.unwrap_err();
        assert!(
            err.contains("sb-a has no workdir") && err.contains("BROKER_SANDBOX_WORKDIR"),
            "{err}"
        );
        assert_eq!(
            caller_as(&without, &b).await,
            Ok(openshell_caller("sb-b", "/sandbox/task-bbbb"))
        );
    }

    #[tokio::test]
    async fn workspace_paths_follow_the_callers_workdir() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let tokens = Tokens::with(&format!(
            "tok-a sb-a /sandbox/task-{sha}\ntok-b sb-b /sandbox/task-ffff\n"
        ));
        let broker = broker(tokens.identity(), openshell(Some("/sandbox/repo")));
        let caller = caller_as(&broker, &[("authorization", "Bearer tok-a")])
            .await
            .unwrap();
        let Workspace::Openshell { workdir } = &caller.workspace else {
            panic!("{caller:?}");
        };
        let ctx = RelPath::parse("svc/api").unwrap();
        assert_eq!(workdir.join(&ctx), format!("/sandbox/task-{sha}/svc/api"));
        assert_eq!(workdir.results(), format!("/sandbox/task-{sha}/.buildit/"));

        let dir = tempfile::tempdir().unwrap();
        let broker = Broker {
            workspace: local(dir.path()),
            ..Arc::into_inner(broker).unwrap()
        };
        assert_eq!(
            caller_as(&broker, &[("authorization", "Bearer tok-b")]).await,
            Ok(Caller {
                sandbox: SandboxName::parse("sb-b").unwrap(),
                workspace: Workspace::Local {
                    dir: dir.path().to_path_buf()
                },
            }),
            "a local workspace ignores the line's workdir"
        );
    }

    #[tokio::test]
    async fn the_dev_token_names_one_sandbox() {
        let dir = tempfile::tempdir().unwrap();
        let broker = broker(
            Identity::Dev {
                token: "dev-token".to_string(),
                sandbox: SandboxName::parse("me").unwrap(),
            },
            local(dir.path()),
        );
        let me = Caller {
            sandbox: SandboxName::parse("me").unwrap(),
            workspace: Workspace::Local {
                dir: dir.path().to_path_buf(),
            },
        };
        assert_eq!(
            caller_as(
                &broker,
                &[
                    ("authorization", "Bearer dev-token"),
                    ("x-crucible-sandbox", "sb-b")
                ]
            )
            .await,
            Ok(me)
        );
        for token in ["Bearer dev-toke", "Bearer dev-token2", "dev-token"] {
            assert_eq!(
                caller_as(&broker, &[("authorization", token)]).await,
                Err("unauthorized".to_string()),
                "{token}"
            );
        }
    }

    #[test]
    fn broker_token_is_only_a_dev_identity() {
        let file = PathBuf::from("/run/tokens");
        assert!(matches!(
            identity(Some(file.clone()), None, Some("dev".to_string())),
            Ok(Identity::TokenFile(p)) if p == file
        ));
        let err = identity(None, None, Some("dev".to_string()))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("MCP_TOKENS_FILE"), "{err}");
        let err = identity(None, Some("me"), None).err().unwrap().to_string();
        assert!(err.contains("BROKER_TOKEN"), "{err}");
        assert!(identity(None, Some("--x"), Some("dev".to_string())).is_err());
        assert!(matches!(
            identity(None, Some("me"), Some("dev".to_string())),
            Ok(Identity::Dev { token, sandbox }) if token == "dev" && sandbox.as_str() == "me"
        ));
    }

    #[derive(clap::Parser)]
    struct McpCli {
        #[command(flatten)]
        args: McpArgs,
    }

    #[test]
    fn dev_flags_need_the_local_workdir_and_exclude_the_token_file() {
        use clap::Parser;
        let parse = |argv: &[&str]| {
            McpCli::try_parse_from(std::iter::once("mcp").chain(argv.iter().copied()))
        };
        assert!(parse(&["--dev-sandbox", "me"]).is_err());
        assert!(
            parse(&[
                "--dev-sandbox",
                "me",
                "--local-workdir",
                ".",
                "--tokens-file",
                "/run/tokens"
            ])
            .is_err()
        );
        let ok = parse(&["--dev-sandbox", "me", "--local-workdir", "."]).unwrap();
        assert_eq!(ok.args.dev_sandbox.as_deref(), Some("me"));
        let ok = parse(&["--tokens-file", "/run/tokens", "--tools", "build,logs"]).unwrap();
        assert_eq!(ok.args.tools, ["build", "logs"]);
        assert_eq!(ok.args.name, "buildit");
    }

    #[test]
    fn mcp_tools_selects_the_served_tools() {
        let all = ["build", "clean", "logs", "run"];
        assert_eq!(Tools::select("buildit", &[]).unwrap().names(), all);
        assert_eq!(
            Tools::select("buildit", &[String::new()]).unwrap().names(),
            all
        );
        let some = Tools::select("buildit", &["logs".to_string(), " build ".to_string()]).unwrap();
        assert_eq!(some.names(), ["build", "logs"]);
        let err = Tools::select("buildit", &["build".to_string(), "deploy".to_string()])
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("\"deploy\""), "{err}");
    }

    async fn serve_with(broker: Arc<Broker>, tools: Tools) -> String {
        let app = router(broker, tools, allowed_hosts(None), CancellationToken::new());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr.to_string()
    }

    async fn serve(broker: Arc<Broker>) -> String {
        serve_with(broker, Tools::select("buildit", &[]).unwrap()).await
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
        let tokens = Tokens::all();
        let addr = serve(broker(tokens.identity(), local(&std::env::temp_dir()))).await;
        let bearer = format!("Bearer {}", token_for("sb-a"));
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
        assert_eq!(status("127.0.0.1", Some(token_for("sb-a"))).await, 401);
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
        let reply = raw_call_with(
            addr,
            &format!("Bearer {}", token_for(sandbox)),
            "",
            name,
            args,
        )
        .await;
        assert_eq!(reply.status, 200, "{}", reply.body);
        tool_text(&reply)
    }

    async fn raw_call_with(
        addr: &str,
        auth: &str,
        extra: &str,
        name: &str,
        args: serde_json::Value,
    ) -> Reply {
        let body = jsonrpc(
            7,
            "tools/call",
            serde_json::json!({ "name": name, "arguments": args }),
        );
        let reply = exchange(addr, http("POST", "127.0.0.1", Some(auth), extra, &body)).await;
        if reply.status == 200 {
            assert!(!reply.head.contains("mcp-session-id"), "{}", reply.head);
            assert!(
                reply.head.contains("content-type: application/json"),
                "{}",
                reply.head
            );
        }
        reply
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stateless_posts_need_no_session_and_get_json() {
        let tokens = Tokens::all();
        let addr = serve(broker(tokens.identity(), local(&std::env::temp_dir()))).await;
        let bearer = format!("Bearer {}", token_for("sb-a"));

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
    ) -> Result<
        rmcp::service::RunningService<rmcp::RoleClient, ()>,
        Box<rmcp::service::ClientInitializeError>,
    > {
        let config =
            StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}{MCP_PATH}"))
                .auth_header(token);
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
        let tokens = Tokens::all();
        let addr = serve(broker(tokens.identity(), local(dir.path()))).await;

        assert!(
            client(&addr, "wrong").await.is_err(),
            "initialize must fail without the right token"
        );

        let c = client(&addr, &token_for("ci-1-abc")).await.unwrap();
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

    fn sandbox_of(text: &str) -> &str {
        let at = text
            .find("buildit.dev/sandbox=")
            .unwrap_or_else(|| panic!("no sandbox selector in {text}"));
        let rest = &text[at + "buildit.dev/sandbox=".len()..];
        &rest[..rest.find(')').unwrap_or(rest.len())]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_callers_come_from_the_token_file_not_headers() {
        let tokens = Tokens::with("tok-a sb-a /sandbox/task-aaaa\ntok-b sb-b /sandbox/task-bbbb\n");
        let addr = serve(broker(tokens.identity(), openshell(None))).await;
        let clean = |auth: &'static str, extra: &'static str| {
            let addr = addr.clone();
            async move { raw_call_with(&addr, auth, extra, "clean", serde_json::json!({})).await }
        };

        // the kube API is unreachable, so clean fails naming the selector it listed with
        let reply = clean("Bearer tok-a", "X-Crucible-Sandbox: sb-b\r\n").await;
        assert_eq!(reply.status, 200, "{}", reply.body);
        let (is_error, text) = tool_text(&reply);
        assert!(is_error, "{text}");
        assert_eq!(sandbox_of(&text), "sb-a", "{text}");
        let (_, text) = tool_text(&clean("Bearer tok-b", "").await);
        assert_eq!(sandbox_of(&text), "sb-b", "{text}");

        for (auth, extra) in [
            ("", "X-Crucible-Sandbox: sb-a\r\n"),
            ("Bearer nope", "X-Crucible-Sandbox: sb-a\r\n"),
            ("Bearer sb-a", ""),
        ] {
            let reply = clean(auth, extra).await;
            assert_eq!(reply.status, 401, "{auth:?} {extra:?}: {}", reply.body);
        }
        let probe = exchange(&addr, http("POST", "127.0.0.1", None, "", INIT)).await;
        assert_eq!(probe.status, 401, "crucible's unauthenticated probe");

        tokens.write("tok-c sb-c /sandbox/task-cccc\n");
        assert_eq!(clean("Bearer tok-a", "").await.status, 401);
        let (_, text) = tool_text(&clean("Bearer tok-c", "").await);
        assert_eq!(sandbox_of(&text), "sb-c", "{text}");

        tokens.write("tok-c\n");
        let reply = clean("Bearer tok-c", "").await;
        assert_eq!(reply.status, 500, "{}", reply.body);
        assert!(!reply.body.contains("tok-c"), "{}", reply.body);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mcp_tools_and_name_shape_what_clients_see() {
        let tokens = Tokens::all();
        let tools = Tools::select("builds", &["logs".to_string(), "clean".to_string()]).unwrap();
        let addr = serve_with(
            broker(tokens.identity(), openshell(Some("/sandbox/repo"))),
            tools,
        )
        .await;
        let c = client(&addr, &token_for("sb-a")).await.unwrap();
        let info = c.peer_info().unwrap();
        assert_eq!(info.server_info.as_ref().unwrap().name, "builds");
        let mut names: Vec<_> = c
            .list_all_tools()
            .await
            .unwrap()
            .iter()
            .map(|t| t.name.to_string())
            .collect();
        names.sort();
        assert_eq!(names, ["clean", "logs"]);
        assert!(
            c.call_tool(call("build", serde_json::json!({ "context": "svc" })))
                .await
                .is_err(),
            "an unselected tool is not callable"
        );
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
            Tools::select("buildit", &[]).unwrap(),
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
        let tokens = Tokens::all();
        let server = start(
            broker(tokens.identity(), local(&std::env::temp_dir())),
            Duration::from_secs(60),
        )
        .await;
        // a keep-alive connection that already got its answer
        let mut idle = tokio::net::TcpStream::connect(&server.addr).await.unwrap();
        let req = http(
            "POST",
            "127.0.0.1",
            Some(&format!("Bearer {}", token_for("sb-a"))),
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

    async fn cluster_broker(dir: &Path, tokens: &Tokens, max_builds: usize) -> Arc<Broker> {
        cluster_broker_with_deadline(dir, tokens, max_builds, 900).await
    }

    async fn cluster_broker_with_deadline(
        dir: &Path,
        tokens: &Tokens,
        max_builds: usize,
        deadline_secs: i64,
    ) -> Arc<Broker> {
        let ctx = std::env::var("BUILDIT_E2E_KUBECONTEXT")
            .expect("set BUILDIT_E2E_KUBECONTEXT to a disposable cluster's context");
        let ns = std::env::var("BUILDIT_E2E_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let (client, namespace, _) = kube_client(Some(&ctx), Some(&ns)).await.unwrap();
        Arc::new(Broker {
            client,
            namespace,
            identity: tokens.identity(),
            workspace: local(dir),
            backend: Backend::Buildah,
            resources: Resources::default(),
            deadline_secs,
            max_builds,
            owner: None,
            calls: tokio_util::task::TaskTracker::new(),
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
        let tokens = Tokens::all();
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

        let first = start(
            cluster_broker(&dir, &tokens, 2).await,
            Duration::from_secs(5),
        )
        .await;
        let a = client(&first.addr, &token_for("sb-a")).await.unwrap();
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

        let broker = cluster_broker(&dir, &tokens, 2).await;
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

        let a = client(&second.addr, &token_for("sb-a")).await.unwrap();
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
        let b = client(&second.addr, &token_for("sb-b")).await.unwrap();
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
            let broker = cluster_broker(&dir, &tokens, 2).await;
            assert!(broker.live_builds(&name).await.unwrap().is_empty());
        }
        let e = call_err(&a, "logs", serde_json::json!({ "build_id": id })).await;
        assert_eq!(e, format!("no live build {id}; build again"));

        a.cancel().await.unwrap();
        b.cancel().await.unwrap();
        second.shut_down().await;
    }

    async fn sandbox_pods(broker: &Broker, sandbox: &str) -> Vec<Pod> {
        broker
            .list(&[("buildit.dev/sandbox", sandbox)])
            .await
            .unwrap()
    }

    async fn wait_until(what: &str, secs: u64, mut done: impl AsyncFnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        while !done().await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn running(pods: &[Pod]) -> bool {
        pods.iter().any(|p| {
            p.metadata.deletion_timestamp.is_none()
                && p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running")
        })
    }

    fn all_deleting(pods: &[Pod]) -> bool {
        pods.iter().all(|p| p.metadata.deletion_timestamp.is_some())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a cluster: BUILDIT_E2E_KUBECONTEXT=kind-x cargo test -- --ignored"]
    async fn e2e_a_build_nobody_hears_back_from_deletes_its_pod() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().canonicalize().unwrap();
        let tokens = Tokens::all();
        write(
            &dir.join("slow/Dockerfile"),
            "FROM docker.io/library/busybox:latest\nRUN sleep 600\n",
        );
        let probe = cluster_broker(&dir, &tokens, 4).await;
        let build = serde_json::json!({ "context": "slow" });

        // the client goes away mid-build
        let server = start(
            cluster_broker(&dir, &tokens, 4).await,
            Duration::from_secs(5),
        )
        .await;
        let addr = server.addr.clone();
        let args = build.clone();
        let call = tokio::spawn(async move { raw_call(&addr, "sb-gone", "build", args).await });
        wait_until("a running builder for sb-gone", 180, async || {
            running(&sandbox_pods(&probe, "sb-gone").await)
        })
        .await;
        call.abort();
        let _ = call.await;
        wait_until("sb-gone's builder to be deleted", 30, async || {
            all_deleting(&sandbox_pods(&probe, "sb-gone").await)
        })
        .await;

        // the server shuts down mid-build: it waits for the pod delete
        let addr = server.addr.clone();
        let req = http(
            "POST",
            "127.0.0.1",
            Some(&format!("Bearer {}", token_for("sb-down"))),
            "",
            &jsonrpc(
                7,
                "tools/call",
                serde_json::json!({ "name": "build", "arguments": build }),
            ),
        );
        let call = tokio::spawn(async move { exchange(&addr, req).await });
        wait_until("a running builder for sb-down", 180, async || {
            running(&sandbox_pods(&probe, "sb-down").await)
        })
        .await;
        server.shut_down().await;
        assert!(all_deleting(&sandbox_pods(&probe, "sb-down").await));
        let reply = call.await.unwrap();
        assert_eq!(reply.status, 500, "{}", reply.body);

        for sandbox in ["sb-gone", "sb-down"] {
            wait_until("builder pods to go", 60, async || {
                sandbox_pods(&probe, sandbox).await.is_empty()
            })
            .await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a cluster: BUILDIT_E2E_KUBECONTEXT=kind-x cargo test -- --ignored"]
    async fn e2e_builder_pods_outlive_a_restart_of_the_serving_pod() {
        use k8s_openapi::api::apps::v1::Deployment;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
        use kube::Api;
        use kube::api::{DeleteParams, ListParams, PostParams};

        let _ = rustls::crypto::ring::default_provider().install_default();
        let ctx = std::env::var("BUILDIT_E2E_KUBECONTEXT").unwrap();
        let ns = std::env::var("BUILDIT_E2E_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let (client, ns, _) = kube_client(Some(&ctx), Some(&ns)).await.unwrap();
        let deployments: Api<Deployment> = Api::namespaced(client.clone(), &ns);
        let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
        let name = BuildId::fresh()
            .to_string()
            .replace("buildit-", "owner-e2e-");
        let sleeper = |name: &str, owner: Option<&OwnerReference>| -> Pod {
            serde_json::from_value(serde_json::json!({
                "metadata": { "name": name, "ownerReferences": owner.map(|o| vec![o]) },
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{
                        "name": "c",
                        "image": "docker.io/library/busybox:latest",
                        "command": ["sleep", "3600"]
                    }]
                }
            }))
            .unwrap()
        };
        let deployment: Deployment = serde_json::from_value(serde_json::json!({
            "metadata": { "name": name },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": { "app": name } },
                "template": {
                    "metadata": { "labels": { "app": name } },
                    "spec": sleeper(&name, None).spec
                }
            }
        }))
        .unwrap();
        let deployment = deployments
            .create(&PostParams::default(), &deployment)
            .await
            .unwrap();
        let by_app = ListParams::default().labels(&format!("app={name}"));
        let live_server = async || {
            pods.list(&by_app)
                .await
                .unwrap()
                .items
                .into_iter()
                .find(|p| p.metadata.deletion_timestamp.is_none())
                .and_then(|p| p.metadata.name)
        };
        let mut first = None;
        wait_until("the deployment's pod", 60, async || {
            first = live_server().await;
            first.is_some()
        })
        .await;
        let first = first.unwrap();

        let owner = workload_owner(client.clone(), &ns, &first).await.unwrap();
        assert_eq!(owner.kind, "Deployment");
        assert_eq!(owner.api_version, "apps/v1");
        assert_eq!(owner.name, name);
        assert_eq!(Some(&owner.uid), deployment.metadata.uid.as_ref());
        assert_eq!(owner.controller, None);

        let pod_owner = OwnerReference {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            name: first.clone(),
            uid: pods.get(&first).await.unwrap().metadata.uid.unwrap(),
            ..Default::default()
        };
        let kept = format!("{name}-kept");
        let lost = format!("{name}-lost");
        pods.create(&PostParams::default(), &sleeper(&kept, Some(&owner)))
            .await
            .unwrap();
        pods.create(&PostParams::default(), &sleeper(&lost, Some(&pod_owner)))
            .await
            .unwrap();
        let bare = workload_owner(client.clone(), &ns, &kept).await.unwrap();
        assert_eq!(
            (bare.kind.as_str(), bare.name.as_str()),
            ("Pod", kept.as_str())
        );

        // a restart of the serving pod: the deployment replaces it
        pods.delete(&first, &DeleteParams::default()).await.unwrap();
        wait_until("the pod-owned builder to be collected", 60, async || {
            pods.get_opt(&lost)
                .await
                .unwrap()
                .is_none_or(|p| p.metadata.deletion_timestamp.is_some())
        })
        .await;
        wait_until("a replacement serving pod", 60, async || {
            live_server().await.is_some_and(|n| n != first)
        })
        .await;
        let survivor = pods.get(&kept).await.unwrap();
        assert!(survivor.metadata.deletion_timestamp.is_none());

        // deleting the workload takes the builder with it
        deployments
            .delete(&name, &DeleteParams::background())
            .await
            .unwrap();
        wait_until(
            "the deployment-owned builder to be collected",
            90,
            async || {
                pods.get_opt(&kept)
                    .await
                    .unwrap()
                    .is_none_or(|p| p.metadata.deletion_timestamp.is_some())
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a cluster: BUILDIT_E2E_KUBECONTEXT=kind-x cargo test -- --ignored"]
    async fn e2e_builds_past_their_deadline_say_so_and_get_swept() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().canonicalize().unwrap();
        let tokens = Tokens::all();
        write(
            &dir.join("svc/Dockerfile"),
            "FROM docker.io/library/busybox:latest\nRUN echo hi\n",
        );
        let server = start(
            cluster_broker_with_deadline(&dir, &tokens, 2, 45).await,
            Duration::from_secs(5),
        )
        .await;
        let probe = cluster_broker(&dir, &tokens, 2).await;
        let a = client(&server.addr, &token_for("sb-old")).await.unwrap();
        let built = call_ok(&a, "build", serde_json::json!({ "context": "svc" })).await;
        let id = built["build_id"].as_str().unwrap().to_string();
        wait_until("the builder to pass its deadline", 120, async || {
            sandbox_pods(&probe, "sb-old")
                .await
                .iter()
                .any(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Failed"))
        })
        .await;
        let e = call_err(&a, "logs", serde_json::json!({ "build_id": id })).await;
        assert_eq!(
            e,
            format!("build {id} is gone (DeadlineExceeded); build again")
        );
        let cleaned = call_ok(&a, "clean", serde_json::json!({})).await;
        assert_eq!(cleaned["deleted"], serde_json::json!([id]));
        let built = call_ok(&a, "build", serde_json::json!({ "context": "svc" })).await;
        let second = built["build_id"].as_str().unwrap().to_string();
        wait_until("the second builder to pass its deadline", 120, async || {
            sandbox_pods(&probe, "sb-old").await.iter().any(|p| {
                p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Failed")
                    && p.metadata.deletion_timestamp.is_none()
            })
        })
        .await;

        // any sandbox's build sweeps ended builder pods
        let b = client(&server.addr, &token_for("sb-new")).await.unwrap();
        let fresh = call_ok(&b, "build", serde_json::json!({ "context": "svc" })).await;
        assert_eq!(fresh["exit"], 0, "{fresh}");
        assert!(all_deleting(&sandbox_pods(&probe, "sb-old").await));
        let e = call_err(&a, "logs", serde_json::json!({ "build_id": second })).await;
        assert_eq!(e, format!("no live build {second}; build again"));

        call_ok(&b, "clean", serde_json::json!({})).await;
        a.cancel().await.unwrap();
        b.cancel().await.unwrap();
        server.shut_down().await;
    }
}
