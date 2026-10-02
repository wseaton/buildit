use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use crate::pod::BuilderPod;
use crate::{BuildArgs, Mode, Schedule};
use crate::{auth, backend, context, job, oci, pod, schedule};

// quay.io/foo:tag -> quay.io/foo@sha256:...
pub(crate) fn pinned_ref(image: &str, digest: &str) -> String {
    format!("{}@{digest}", oci::repo_of(image))
}

pub async fn run(client: kube::Client, args: &BuildArgs) -> Result<()> {
    let registry = auth::registry_of(&args.image)?;
    let authfile = auth::minimal_authfile(&registry)?;

    tracing::info!(
        "context: {}  dockerfile: {}  ->  {}  (ns={}, backend={:?}, mode={:?})",
        args.context.display(),
        args.dockerfile,
        args.image,
        args.namespace,
        args.backend,
        args.mode
    );

    let idle_nodes = match (args.node.as_deref(), args.schedule) {
        // explicit pin makes the scout pointless
        (Some(_), _) | (None, Schedule::Any) => Vec::new(),
        (None, Schedule::Idle) => {
            let nodes = schedule::idle_nodes(client.clone()).await;
            tracing::info!(
                "{} idle node(s) without GPU workloads, preferring them (best effort)",
                nodes.len()
            );
            nodes
        }
    };

    if let Some(backend::CacheVolume::Pvc(name)) = args.cache() {
        pod::ensure_pvc(client.clone(), &args.namespace, name, &args.cache_size).await?;
    }

    if args.mode == Mode::Job {
        return detach(client, args, &registry, &authfile, &idle_nodes).await;
    }

    let tarball = context::tarball(&args.context)?;
    tracing::info!("context tarball: {} KiB", tarball.len() / 1024);

    let resources = args.resources();
    let opts = backend::PodOpts {
        idle_nodes: &idle_nodes,
        resources: &resources,
        cache: args.cache(),
        node: args.node.as_deref(),
    };
    let pod = BuilderPod::create(client, &args.namespace, args.backend, &opts).await?;
    tracing::info!("builder pod {} created, waiting for ready", pod.name);

    let outcome = tokio::select! {
        result = drive(&pod, args, &tarball, &authfile) => result,
        _ = tokio::signal::ctrl_c() => Err(anyhow!("interrupted, cleaning up")),
    };

    if let Err(err) = pod.delete().await {
        tracing::warn!("failed to delete pod {}: {err:#}", pod.name);
        tracing::warn!("run `buildit clean -n {}` to remove it", args.namespace);
    } else {
        tracing::info!("builder pod {} deleted", pod.name);
    }

    let digest = outcome?;
    tracing::info!("pushed {}", args.image);
    println!("{}", pinned_ref(&args.image, &digest));
    Ok(())
}

// push context to the registry, hand the build to a Job, print the handle
async fn detach(
    client: kube::Client,
    args: &BuildArgs,
    registry: &str,
    authfile: &[u8],
    idle_nodes: &[String],
) -> Result<()> {
    let tar = context::tar_bytes(&args.context)?;
    tracing::info!("context tar: {} KiB", tar.len() / 1024);
    let ctx_ref = oci::context_reference(&args.image, &tar);
    let (user, pass) = auth::basic_credentials(registry)?;
    let reg_auth = oci_client::secrets::RegistryAuth::Basic(user, pass);
    let ctx_labels = if args.context_labels.is_empty() {
        let defaults = oci::default_context_labels(registry);
        for (k, v) in &defaults {
            tracing::info!("context label default for {registry}: {k}={v}");
        }
        defaults
    } else {
        args.context_labels.clone()
    };
    oci::push_context(&ctx_ref, tar, &reg_auth, &ctx_labels).await?;

    let job = job::create(
        client,
        &args.namespace,
        args.backend,
        &job::DetachArgs {
            image: &args.image,
            dockerfile: &args.dockerfile,
            build_args: &args.build_args,
            ctx_ref: &ctx_ref,
            authfile,
            idle_nodes,
            resources: &args.resources(),
            labels: &args.labels,
            cache: args.cache(),
            node: args.node.as_deref(),
        },
    )
    .await?;
    tracing::info!(
        "job {job} created; follow with `buildit wait {job} -n {}`",
        args.namespace
    );
    println!("{job}");
    Ok(())
}

// --output render: print manifests as YAML, touch nothing
pub fn render(args: &BuildArgs) -> Result<()> {
    let name = crate::pod::unique_name();
    let resources = args.resources();
    match args.mode {
        Mode::Pod => {
            let opts = backend::PodOpts {
                idle_nodes: &[],
                resources: &resources,
                cache: args.cache(),
                node: args.node.as_deref(),
            };
            let pod = args.backend.pod_spec(&name, &args.namespace, &opts)?;
            print!("{}", serde_norway::to_string(&pod)?);
        }
        Mode::Job => {
            let tar = context::tar_bytes(&args.context)?;
            let ctx_ref = oci::context_reference(&args.image, &tar);
            let spec = args.backend.job_spec(
                &name,
                &args.namespace,
                &job::DetachArgs {
                    image: &args.image,
                    dockerfile: &args.dockerfile,
                    build_args: &args.build_args,
                    ctx_ref: &ctx_ref,
                    authfile: b"",
                    idle_nodes: &[],
                    resources: &resources,
                    labels: &args.labels,
                    cache: args.cache(),
                    node: args.node.as_deref(),
                },
            )?;
            tracing::info!("secret data redacted; a real run ships your registry token");
            tracing::info!("a real run pushes the context to {ctx_ref} first");
            let secret = job::secret_manifest(&name, &args.namespace, b"<redacted>", None);
            print!(
                "{}---\n{}",
                serde_norway::to_string(&spec)?,
                serde_norway::to_string(&secret)?
            );
        }
    }
    Ok(())
}

async fn drive(
    pod: &BuilderPod,
    args: &BuildArgs,
    tarball: &[u8],
    authfile: &[u8],
) -> Result<String> {
    let backend = args.backend;
    pod.wait_ready(Duration::from_secs(180)).await?;

    tracing::info!("staging source + auth");
    pod.exec_stream(&backend.setup_command()).await?;
    pod.exec_with_stdin(&backend.untar_command(), tarball)
        .await?;
    pod.exec_with_stdin(&backend.auth_upload_command(), authfile)
        .await?;

    tracing::info!("building + pushing (this can take several minutes)");
    for step in backend.build_steps(
        &args.image,
        &args.dockerfile,
        &args.build_args,
        &args.labels,
    ) {
        pod.exec_stream(&step).await?;
    }

    let raw = pod
        .exec_capture(&backend.digest_command())
        .await
        .context("reading image digest")?;
    backend.digest_from(&raw)
}

#[cfg(test)]
mod tests {
    use crate::build::pinned_ref;

    #[test]
    fn pin_swaps_tag_for_digest() {
        assert_eq!(
            pinned_ref("quay.io/acme/foo:tag", "sha256:abc"),
            "quay.io/acme/foo@sha256:abc"
        );
        assert_eq!(
            pinned_ref("localhost:5000/foo", "sha256:abc"),
            "localhost:5000/foo@sha256:abc"
        );
        assert_eq!(
            pinned_ref("quay.io/acme/foo", "sha256:abc"),
            "quay.io/acme/foo@sha256:abc"
        );
    }
}
