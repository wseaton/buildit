use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use crate::BuildArgs;
use crate::pod::BuilderPod;
use crate::{auth, context, job, oci, schedule};

// quay.io/foo:tag -> quay.io/foo@sha256:...
pub(crate) fn pinned_ref(image: &str, digest: &str) -> String {
    format!("{}@{digest}", oci::repo_of(image))
}

pub async fn run(client: kube::Client, args: &BuildArgs) -> Result<()> {
    let registry = auth::registry_of(&args.image)?;
    let authfile = auth::minimal_authfile(&registry)?;

    tracing::info!(
        "context: {}  dockerfile: {}  ->  {}  (ns={}, backend={:?}, detach={})",
        args.context.display(),
        args.dockerfile,
        args.image,
        args.namespace,
        args.backend,
        args.detach
    );

    let idle_nodes = if args.any_node {
        Vec::new()
    } else {
        let nodes = schedule::idle_nodes(client.clone()).await;
        tracing::info!(
            "{} idle node(s) without GPU workloads, preferring them (best effort)",
            nodes.len()
        );
        nodes
    };

    if args.detach {
        return detach(client, args, &registry, &authfile, &idle_nodes).await;
    }

    let tarball = context::tarball(&args.context)?;
    tracing::info!("context tarball: {} KiB", tarball.len() / 1024);

    let pod = BuilderPod::create(client, &args.namespace, args.backend, &idle_nodes).await?;
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
    oci::push_context(&ctx_ref, tar, &reg_auth).await?;

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
    for step in backend.build_steps(&args.image, &args.dockerfile, &args.build_args) {
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
