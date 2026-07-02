mod auth;
mod backend;
mod build;
mod context;
mod job;
mod oci;
mod pod;
mod schedule;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};

use crate::backend::Backend;

#[derive(Parser)]
#[command(
    name = "buildit",
    version,
    about = "Remote container builds on a Kubernetes cluster"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Build and push an image via an unprivileged builder pod
    Build(BuildArgs),
    /// Wait for a detached build Job and print its digest-pinned ref
    Wait {
        /// Job name printed by `build --detach`
        job: String,
        #[arg(short, long, default_value = "default", env = "BUILDIT_NAMESPACE")]
        namespace: String,
        /// Kubeconfig context to use (defaults to the current context)
        #[arg(long)]
        kubecontext: Option<String>,
    },
    /// Delete leftover buildit pods, jobs, and secrets (label app=buildit)
    Clean {
        #[arg(short, long, default_value = "default", env = "BUILDIT_NAMESPACE")]
        namespace: String,
        /// Kubeconfig context to use (defaults to the current context)
        #[arg(long)]
        kubecontext: Option<String>,
    },
}

#[derive(Args)]
pub struct BuildArgs {
    /// Fully-qualified image reference to build and push, e.g. quay.io/acme/foo:tag
    pub image: String,
    #[arg(short = 'f', long, default_value = "Dockerfile")]
    pub dockerfile: String,
    /// Build context directory
    #[arg(short, long, default_value = ".")]
    pub context: PathBuf,
    #[arg(short, long, default_value = "default", env = "BUILDIT_NAMESPACE")]
    pub namespace: String,
    /// Builder backend. All three run unprivileged; buildkit and buildah are
    /// rootless (uid 1000), kaniko is the legacy (unmaintained) fallback.
    #[arg(long, value_enum, default_value_t = Backend::Buildkit)]
    pub backend: Backend,
    /// Skip the idle-node scout and let the scheduler place the pod anywhere
    #[arg(long)]
    pub any_node: bool,
    /// Run the build as a Kubernetes Job and return immediately. The context
    /// is pushed to the registry so the build survives client disconnect;
    /// follow up with `buildit wait <job>`.
    #[arg(long)]
    pub detach: bool,
    /// Build args, repeatable: --build-arg KEY=VALUE
    #[arg(long = "build-arg", value_name = "KEY=VALUE")]
    pub build_args: Vec<String>,
    /// Kubeconfig context to use (defaults to the current context)
    #[arg(long)]
    pub kubecontext: Option<String>,
}

async fn client_for(kubecontext: Option<&str>) -> Result<kube::Client> {
    let config = match kubecontext {
        Some(ctx) => kube::Config::from_kubeconfig(&kube::config::KubeConfigOptions {
            context: Some(ctx.to_string()),
            ..Default::default()
        })
        .await
        .with_context(|| format!("loading kubeconfig context {ctx}"))?,
        None => {
            if let Ok(kc) = kube::config::Kubeconfig::read()
                && let Some(current) = &kc.current_context
            {
                tracing::info!("using current kubecontext: {current}");
            }
            kube::Config::infer()
                .await
                .context("inferring kube config")?
        }
    };
    kube::Client::try_from(config).context("building kube client")
}

#[tokio::main]
async fn main() -> Result<()> {
    // kube wants ring, oci-client's deps drag in aws-lc-rs; with both in the
    // tree rustls refuses to guess, so pick ring explicitly
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("installing rustls crypto provider"))?;
    // stderr for logs, stdout stays clean for the digest-pinned ref
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Build(args) => {
            let client = client_for(args.kubecontext.as_deref()).await?;
            build::run(client, &args).await
        }
        Cmd::Wait {
            job,
            namespace,
            kubecontext,
        } => {
            let client = client_for(kubecontext.as_deref()).await?;
            job::wait(client, &namespace, &job).await
        }
        Cmd::Clean {
            namespace,
            kubecontext,
        } => {
            let client = client_for(kubecontext.as_deref()).await?;
            let deleted = pod::clean(client.clone(), &namespace).await?
                + job::clean(client, &namespace).await?;
            if deleted == 0 {
                tracing::info!("nothing to clean in {namespace}");
            }
            Ok(())
        }
    }
}
