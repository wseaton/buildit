use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Status;
use kube::api::{
    Api, AttachParams, AttachedProcess, DeleteParams, ListParams, Patch, PatchParams, PostParams,
};
use kube::runtime::wait::{await_condition, conditions};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::backend::Backend;

const CAPTURE_LIMIT: u64 = 64 * 1024 * 1024;

pub struct LogSink {
    out: Box<dyn AsyncWrite + Unpin + Send>,
    err: Box<dyn AsyncWrite + Unpin + Send>,
}

impl LogSink {
    pub fn terminal() -> Self {
        Self {
            out: Box::new(tokio::io::stdout()),
            err: Box::new(tokio::io::stderr()),
        }
    }

    pub async fn line(&mut self, line: &str) -> Result<()> {
        self.out
            .write_all(format!("{line}\n").as_bytes())
            .await
            .context("writing log line")?;
        self.out.flush().await.context("flushing log line")
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.out.flush().await.context("flushing log stdout")?;
        self.err.flush().await.context("flushing log stderr")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecStatus {
    pub code: Option<i32>,
    pub message: String,
}

impl ExecStatus {
    pub fn success(&self) -> bool {
        self.code == Some(0)
    }

    fn from_status(status: &Status) -> Self {
        if status.status.as_deref() == Some("Success") {
            return Self {
                code: Some(0),
                message: "success".to_string(),
            };
        }
        let code = status
            .details
            .as_ref()
            .and_then(|d| d.causes.as_deref())
            .unwrap_or_default()
            .iter()
            .find(|c| c.reason.as_deref() == Some("ExitCode"))
            .and_then(|c| c.message.as_deref())
            .and_then(|m| m.trim().parse().ok());
        Self {
            code,
            message: status
                .message
                .clone()
                .unwrap_or_else(|| "no error message".to_string()),
        }
    }
}

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
        Self::create_named(client, namespace, backend, &unique_name(), opts).await
    }

    pub async fn create_named(
        client: kube::Client,
        namespace: &str,
        backend: Backend,
        name: &str,
        opts: &crate::backend::PodOpts<'_>,
    ) -> Result<Self> {
        let pods: Api<Pod> = Api::namespaced(client, namespace);
        let spec = backend.pod_spec(name, namespace, opts)?;
        pods.create(&PostParams::default(), &spec)
            .await
            .with_context(|| format!("creating pod {name} in {namespace}"))?;
        Ok(Self {
            pods,
            name: name.to_string(),
        })
    }

    pub fn existing(client: kube::Client, namespace: &str, name: &str) -> Self {
        Self {
            pods: Api::namespaced(client, namespace),
            name: name.to_string(),
        }
    }

    pub async fn set_labels(&self, labels: &[(&str, &str)]) -> Result<()> {
        let labels: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), serde_json::json!(v)))
            .collect();
        let patch = serde_json::json!({ "metadata": { "labels": labels } });
        self.pods
            .patch(&self.name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .with_context(|| format!("labelling pod {}", self.name))?;
        Ok(())
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

    // exit status comes from the real status frame; a piped exit code once
    // lied about a segfault and that's why this tool exists
    async fn status(&self, mut attached: AttachedProcess) -> Result<ExecStatus> {
        let status = attached
            .take_status()
            .ok_or_else(|| anyhow!("exec status channel already taken"))?
            .await;
        attached.join().await.context("joining exec stream")?;
        let status =
            status.ok_or_else(|| anyhow!("no exit status received from pod {}", self.name))?;
        Ok(ExecStatus::from_status(&status))
    }

    fn require_success(&self, status: &ExecStatus, argv: &[String], stderr: &[u8]) -> Result<()> {
        if status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(stderr);
        let stderr = stderr.trim();
        bail!(
            "`{}` failed in pod {}: {}{}",
            argv.join(" "),
            self.name,
            status.message,
            if stderr.is_empty() {
                String::new()
            } else {
                format!("\n{stderr}")
            }
        )
    }

    async fn attach(&self, argv: &[String], stdin: bool) -> Result<AttachedProcess> {
        let params = AttachParams::default()
            .stdin(stdin)
            .stdout(true)
            .stderr(true);
        self.pods
            .exec(&self.name, argv.iter().map(String::as_str), &params)
            .await
            .with_context(|| format!("exec `{}` in pod {}", argv.join(" "), self.name))
    }

    pub async fn exec_logged(&self, argv: &[String], sink: &mut LogSink) -> Result<ExecStatus> {
        let mut attached = self.attach(argv, false).await?;
        let mut stdout = attached
            .stdout()
            .ok_or_else(|| anyhow!("exec stdout stream missing"))?;
        let mut stderr = attached
            .stderr()
            .ok_or_else(|| anyhow!("exec stderr stream missing"))?;
        let out = tokio::io::copy(&mut stdout, &mut sink.out);
        let err = tokio::io::copy(&mut stderr, &mut sink.err);
        let (out, err) = tokio::join!(out, err);
        out.context("streaming exec stdout")?;
        err.context("streaming exec stderr")?;
        sink.flush().await?;
        self.status(attached).await
    }

    // output is discarded
    pub async fn exec_status(&self, argv: &[String]) -> Result<ExecStatus> {
        let mut attached = self.attach(argv, false).await?;
        let mut stdout = attached
            .stdout()
            .ok_or_else(|| anyhow!("exec stdout stream missing"))?;
        let mut stderr = attached
            .stderr()
            .ok_or_else(|| anyhow!("exec stderr stream missing"))?;
        let (mut out_sink, mut err_sink) = (tokio::io::sink(), tokio::io::sink());
        let out = tokio::io::copy(&mut stdout, &mut out_sink);
        let err = tokio::io::copy(&mut stderr, &mut err_sink);
        let (out, err) = tokio::join!(out, err);
        out.context("draining exec stdout")?;
        err.context("draining exec stderr")?;
        self.status(attached).await
    }

    pub async fn exec_stream(&self, argv: &[String], sink: &mut LogSink) -> Result<()> {
        let status = self.exec_logged(argv, sink).await?;
        self.require_success(&status, argv, b"")
    }

    pub async fn exec_with_stdin(&self, argv: &[String], data: &[u8]) -> Result<()> {
        let mut attached = self.attach(argv, true).await?;
        let mut stdin = attached
            .stdin()
            .ok_or_else(|| anyhow!("exec stdin stream missing"))?;
        let mut stderr = attached
            .stderr()
            .ok_or_else(|| anyhow!("exec stderr stream missing"))?;
        let write = async {
            stdin.write_all(data).await.context("writing exec stdin")?;
            stdin.shutdown().await.context("closing exec stdin")?;
            drop(stdin);
            anyhow::Ok(())
        };
        let mut err_buf = Vec::new();
        let read_err = tokio::io::copy(&mut stderr, &mut err_buf);
        let (written, read) = tokio::join!(write, read_err);
        written?;
        read.context("reading exec stderr")?;
        let status = self.status(attached).await?;
        self.require_success(&status, argv, &err_buf)
    }

    pub async fn exec_capture_bytes(&self, argv: &[String], limit: u64) -> Result<Vec<u8>> {
        let mut attached = self.attach(argv, false).await?;
        let stdout = attached
            .stdout()
            .ok_or_else(|| anyhow!("exec stdout stream missing"))?;
        let mut stderr = attached
            .stderr()
            .ok_or_else(|| anyhow!("exec stderr stream missing"))?;
        let err_task = tokio::spawn(async move {
            let mut err_buf = Vec::new();
            tokio::io::copy(&mut stderr, &mut err_buf)
                .await
                .map(|_| err_buf)
        });
        let mut buf = Vec::new();
        tokio::io::copy(&mut stdout.take(limit.saturating_add(1)), &mut buf)
            .await
            .context("capturing exec stdout")?;
        if buf.len() as u64 > limit {
            err_task.abort();
            bail!(
                "`{}` in pod {} wrote more than {limit} bytes",
                argv.join(" "),
                self.name
            );
        }
        let err_buf = err_task
            .await
            .context("joining exec stderr reader")?
            .context("capturing exec stderr")?;
        let status = self.status(attached).await?;
        self.require_success(&status, argv, &err_buf)?;
        Ok(buf)
    }

    pub async fn exec_capture(&self, argv: &[String]) -> Result<String> {
        let buf = self.exec_capture_bytes(argv, CAPTURE_LIMIT).await?;
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
        if pod
            .metadata
            .owner_references
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|o| o.kind == "Job")
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

#[cfg(test)]
mod tests {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Status, StatusCause, StatusDetails};

    use crate::pod::ExecStatus;

    #[test]
    fn exec_status_reads_the_exit_code_cause() {
        let ok = Status {
            status: Some("Success".to_string()),
            ..Default::default()
        };
        assert!(ExecStatus::from_status(&ok).success());

        let failed = Status {
            status: Some("Failure".to_string()),
            reason: Some("NonZeroExitCode".to_string()),
            message: Some("command terminated with non-zero exit code".to_string()),
            details: Some(StatusDetails {
                causes: Some(vec![StatusCause {
                    reason: Some("ExitCode".to_string()),
                    message: Some("3".to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let s = ExecStatus::from_status(&failed);
        assert_eq!(s.code, Some(3));
        assert!(!s.success());

        let never_ran = Status {
            status: Some("Failure".to_string()),
            message: Some("executable file not found".to_string()),
            ..Default::default()
        };
        let s = ExecStatus::from_status(&never_ran);
        assert_eq!(s.code, None);
        assert_eq!(s.message, "executable file not found");
    }
}
