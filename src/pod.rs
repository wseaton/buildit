use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, AttachParams, AttachedProcess, DeleteParams, ListParams, PostParams};
use kube::runtime::wait::{await_condition, conditions};
use tokio::io::AsyncWriteExt;

use crate::backend::Backend;

pub struct BuilderPod {
    pods: Api<Pod>,
    pub name: String,
}

pub(crate) fn unique_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "buildit-{:08x}",
        (nanos ^ u128::from(std::process::id())) as u32
    )
}

impl BuilderPod {
    pub async fn create(
        client: kube::Client,
        namespace: &str,
        backend: Backend,
        opts: &crate::backend::PodOpts<'_>,
    ) -> Result<Self> {
        let pods: Api<Pod> = Api::namespaced(client, namespace);
        let name = unique_name();
        let spec = backend.pod_spec(&name, namespace, opts)?;
        pods.create(&PostParams::default(), &spec)
            .await
            .with_context(|| format!("creating pod {name} in {namespace}"))?;
        Ok(Self { pods, name })
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(
            timeout,
            await_condition(self.pods.clone(), &self.name, conditions::is_pod_running()),
        )
        .await
        .map_err(|_| anyhow!("pod {} not running after {timeout:?}", self.name))?
        .with_context(|| format!("waiting for pod {}", self.name))?;
        Ok(())
    }

    async fn finish(&self, mut attached: AttachedProcess, argv: &[String]) -> Result<()> {
        let status = attached
            .take_status()
            .ok_or_else(|| anyhow!("exec status channel already taken"))?
            .await;
        attached.join().await.context("joining exec stream")?;
        match status {
            Some(s) if s.status.as_deref() == Some("Success") => Ok(()),
            Some(s) => bail!(
                "`{}` failed in pod {}: {}",
                argv.join(" "),
                self.name,
                s.message.unwrap_or_else(|| "no error message".to_string())
            ),
            None => bail!(
                "`{}` in pod {}: no exit status received",
                argv.join(" "),
                self.name
            ),
        }
    }

    // exit status comes from the real status frame; a piped exit code once
    // lied about a segfault and that's why this tool exists
    pub async fn exec_stream(&self, argv: &[String]) -> Result<()> {
        let params = AttachParams::default().stdout(true).stderr(true);
        let mut attached = self
            .pods
            .exec(&self.name, argv.iter().map(String::as_str), &params)
            .await
            .with_context(|| format!("exec `{}` in pod {}", argv.join(" "), self.name))?;
        let mut stdout = attached
            .stdout()
            .ok_or_else(|| anyhow!("exec stdout stream missing"))?;
        let mut stderr = attached
            .stderr()
            .ok_or_else(|| anyhow!("exec stderr stream missing"))?;
        let mut our_stdout = tokio::io::stdout();
        let mut our_stderr = tokio::io::stderr();
        let out = tokio::io::copy(&mut stdout, &mut our_stdout);
        let err = tokio::io::copy(&mut stderr, &mut our_stderr);
        let (out, err) = tokio::join!(out, err);
        out.context("streaming exec stdout")?;
        err.context("streaming exec stderr")?;
        self.finish(attached, argv).await
    }

    pub async fn exec_with_stdin(&self, argv: &[String], data: &[u8]) -> Result<()> {
        let params = AttachParams::default()
            .stdin(true)
            .stdout(true)
            .stderr(true);
        let mut attached = self
            .pods
            .exec(&self.name, argv.iter().map(String::as_str), &params)
            .await
            .with_context(|| format!("exec `{}` in pod {}", argv.join(" "), self.name))?;
        let mut stdin = attached
            .stdin()
            .ok_or_else(|| anyhow!("exec stdin stream missing"))?;
        stdin.write_all(data).await.context("writing exec stdin")?;
        stdin.shutdown().await.context("closing exec stdin")?;
        drop(stdin);
        self.finish(attached, argv).await
    }

    pub async fn exec_capture(&self, argv: &[String]) -> Result<String> {
        let params = AttachParams::default().stdout(true).stderr(true);
        let mut attached = self
            .pods
            .exec(&self.name, argv.iter().map(String::as_str), &params)
            .await
            .with_context(|| format!("exec `{}` in pod {}", argv.join(" "), self.name))?;
        let mut stdout = attached
            .stdout()
            .ok_or_else(|| anyhow!("exec stdout stream missing"))?;
        let mut buf = Vec::new();
        tokio::io::copy(&mut stdout, &mut buf)
            .await
            .context("capturing exec stdout")?;
        self.finish(attached, argv).await?;
        String::from_utf8(buf).context("exec output was not utf-8")
    }

    pub async fn delete(&self) -> Result<()> {
        self.pods
            .delete(&self.name, &DeleteParams::default())
            .await
            .with_context(|| format!("deleting pod {}", self.name))?;
        Ok(())
    }
}

// get-or-create the cache PVC; existing claims are used as-is
pub async fn ensure_pvc(
    client: kube::Client,
    namespace: &str,
    name: &str,
    size: &str,
) -> Result<()> {
    use k8s_openapi::api::core::v1::PersistentVolumeClaim;
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client, namespace);
    if pvcs
        .get_opt(name)
        .await
        .context("checking cache PVC")?
        .is_some()
    {
        return Ok(());
    }
    let claim: PersistentVolumeClaim = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": { "app": "buildit", "buildit/cache": "true" }
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": size } }
        }
    }))
    .context("building PVC spec")?;
    pvcs.create(&PostParams::default(), &claim)
        .await
        .with_context(|| format!("creating cache PVC {name}"))?;
    tracing::info!("created cache PVC {name} ({size})");
    Ok(())
}

pub async fn clean(client: kube::Client, namespace: &str) -> Result<usize> {
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let list = pods
        .list(&ListParams::default().labels("app=buildit"))
        .await
        .with_context(|| format!("listing buildit pods in {namespace}"))?;
    let mut deleted = 0;
    for pod in list.items {
        // job pods carry app=buildit too but belong to their Job; the job
        // sweep handles those via cascade
        if !pod
            .metadata
            .owner_references
            .as_deref()
            .unwrap_or_default()
            .is_empty()
        {
            continue;
        }
        if let Some(name) = pod.metadata.name {
            pods.delete(&name, &DeleteParams::default())
                .await
                .with_context(|| format!("deleting pod {name}"))?;
            tracing::info!("deleted pod {name}");
            deleted += 1;
        }
    }
    Ok(deleted)
}
