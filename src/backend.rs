use anyhow::{Context, Result, anyhow};
use clap::ValueEnum;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

pub const BACKEND_LABEL: &str = "buildit/backend";

#[derive(Clone, Copy)]
pub enum CacheVolume<'a> {
    Pvc(&'a str),
    // node-local NVMe, the coreweave speed play; pair with PodOpts.node
    HostPath(&'a str),
}

pub struct PodOpts<'a> {
    pub idle_nodes: &'a [String],
    pub resources: &'a Resources,
    pub cache: Option<CacheVolume<'a>>,
    pub node: Option<&'a str>,
    pub deadline_secs: i64,
    pub owner: Option<&'a OwnerReference>,
    pub meta: Option<&'a PodMeta>,
}

#[derive(Default)]
pub struct PodMeta {
    pub labels: Vec<(String, String)>,
    pub annotations: Vec<(String, String)>,
    // emptyDir mount path
    pub log_dir: Option<String>,
}

#[derive(Clone, Copy)]
enum Dest<'a> {
    Push(&'a str),
    Local(&'a str),
}

#[derive(Default)]
pub struct Resources {
    pub requests: Vec<(String, String)>,
    pub limits: Vec<(String, String)>,
}

impl Resources {
    fn json(&self) -> Option<serde_json::Value> {
        fn map(kvs: &[(String, String)]) -> Option<serde_json::Value> {
            (!kvs.is_empty()).then(|| {
                serde_json::Value::Object(
                    kvs.iter()
                        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
                        .collect(),
                )
            })
        }
        let mut out = serde_json::Map::new();
        if let Some(r) = map(&self.requests) {
            out.insert("requests".to_string(), r);
        }
        if let Some(l) = map(&self.limits) {
            out.insert("limits".to_string(), l);
        }
        (!out.is_empty()).then_some(serde_json::Value::Object(out))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum Backend {
    /// rootless buildkitd, driven one-shot via buildctl-daemonless.sh
    Buildkit,
    /// kaniko executor (unmaintained upstream, but a known-good fallback)
    Kaniko,
    /// rootless buildah, chroot isolation + vfs storage
    Buildah,
}

impl Backend {
    pub fn shell(&self) -> &'static str {
        match self {
            Backend::Buildkit => "/bin/sh",
            Backend::Kaniko => "/busybox/sh",
            Backend::Buildah => "/bin/sh",
        }
    }

    // rootless backends are uid 1000 and can't mkdir at /, so: $HOME
    pub fn workspace(&self) -> &'static str {
        match self {
            Backend::Buildkit => "/home/user/workspace",
            Backend::Kaniko => "/workspace",
            Backend::Buildah => "/home/build/workspace",
        }
    }

    fn auth_path(&self) -> &'static str {
        match self {
            Backend::Buildkit => "/home/user/.docker/config.json",
            Backend::Kaniko => "/kaniko/.docker/config.json",
            Backend::Buildah => "/tmp/auth.json",
        }
    }

    fn digest_path(&self) -> &'static str {
        match self {
            // buildctl writes a JSON metadata file, not a bare digest
            Backend::Buildkit => "/tmp/metadata.json",
            Backend::Kaniko => "/tmp/digest",
            Backend::Buildah => "/tmp/digest",
        }
    }

    pub fn digest_command(&self) -> Vec<String> {
        vec![
            self.shell().to_string(),
            "-c".to_string(),
            format!("cat {}", self.digest_path()),
        ]
    }

    pub fn digest_from(&self, raw: &str) -> Result<String> {
        let digest = match self {
            Backend::Buildkit => {
                let meta: serde_json::Value =
                    serde_json::from_str(raw).context("parsing buildkit metadata file")?;
                meta.get("containerimage.digest")
                    .and_then(|d| d.as_str())
                    .ok_or_else(|| anyhow!("no containerimage.digest in buildkit metadata"))?
                    .to_string()
            }
            Backend::Kaniko | Backend::Buildah => raw.trim().to_string(),
        };
        if !digest.starts_with("sha256:") {
            return Err(anyhow!("unexpected digest: {digest:?}"));
        }
        Ok(digest)
    }

    pub fn setup_command(&self) -> Vec<String> {
        let ws = self.workspace();
        let script = match self {
            Backend::Buildkit => format!("mkdir -p {ws} /home/user/.docker"),
            // kaniko:debug ships without /tmp, which breaks anything tar-shaped
            Backend::Kaniko => format!("mkdir -p {ws} /tmp /kaniko/.docker"),
            // the stable image's storage.conf points at a rootful store under
            // /usr/lib that rootless vfs can't lock, so bring our own conf
            Backend::Buildah => format!(
                "mkdir -p {ws} /tmp/containers-run && printf '[storage]\\ndriver = \"vfs\"\\ngraphroot = \"/home/build/.local/share/containers/storage\"\\nrunroot = \"/tmp/containers-run\"\\n' > /tmp/storage.conf"
            ),
        };
        vec![self.shell().to_string(), "-c".to_string(), script]
    }

    pub fn untar_command(&self) -> Vec<String> {
        vec![
            self.shell().to_string(),
            "-c".to_string(),
            format!("tar xzf - -C {}", self.workspace()),
        ]
    }

    pub fn auth_upload_command(&self) -> Vec<String> {
        vec![
            self.shell().to_string(),
            "-c".to_string(),
            format!("cat > {}", self.auth_path()),
        ]
    }

    // each step is plain argv, no shell, no quoting games
    pub fn build_steps(
        &self,
        image: &str,
        dockerfile: &str,
        target: Option<&str>,
        build_args: &[String],
        labels: &[(String, String)],
    ) -> Vec<Vec<String>> {
        self.steps(Dest::Push(image), dockerfile, target, build_args, labels)
    }

    pub fn local_build_steps(
        &self,
        tag: &str,
        dockerfile: &str,
        target: Option<&str>,
        build_args: &[String],
    ) -> Vec<Vec<String>> {
        self.steps(Dest::Local(tag), dockerfile, target, build_args, &[])
    }

    fn steps(
        &self,
        dest: Dest<'_>,
        dockerfile: &str,
        target: Option<&str>,
        build_args: &[String],
        labels: &[(String, String)],
    ) -> Vec<Vec<String>> {
        let ws = self.workspace();
        match self {
            Backend::Buildkit => {
                let mut build = vec![
                    "buildctl-daemonless.sh".to_string(),
                    "build".to_string(),
                    "--frontend".to_string(),
                    "dockerfile.v0".to_string(),
                    "--local".to_string(),
                    format!("context={ws}"),
                    "--local".to_string(),
                    format!("dockerfile={ws}"),
                    "--opt".to_string(),
                    format!("filename={dockerfile}"),
                ];
                if let Dest::Push(image) = dest {
                    build.extend([
                        "--output".to_string(),
                        format!("type=image,name={image},push=true"),
                        "--metadata-file".to_string(),
                        self.digest_path().to_string(),
                    ]);
                }
                if let Some(target) = target {
                    build.push("--opt".to_string());
                    build.push(format!("target={target}"));
                }
                for arg in build_args {
                    build.push("--opt".to_string());
                    build.push(format!("build-arg:{arg}"));
                }
                for (k, v) in labels {
                    build.push("--opt".to_string());
                    build.push(format!("label:{k}={v}"));
                }
                vec![build]
            }
            Backend::Kaniko => {
                let mut exec = vec![
                    "/kaniko/executor".to_string(),
                    format!("--dockerfile={ws}/{dockerfile}"),
                    format!("--context=dir://{ws}"),
                ];
                match dest {
                    Dest::Push(image) => exec.extend([
                        format!("--destination={image}"),
                        // no --cleanup: it nukes the fs, digest file and all
                        format!("--digest-file={}", self.digest_path()),
                    ]),
                    Dest::Local(_) => exec.push("--no-push".to_string()),
                }
                if let Some(target) = target {
                    exec.push(format!("--target={target}"));
                }
                for arg in build_args {
                    exec.push(format!("--build-arg={arg}"));
                }
                for (k, v) in labels {
                    exec.push("--label".to_string());
                    exec.push(format!("{k}={v}"));
                }
                vec![exec]
            }
            // isolation/storage-driver flags live in the pod spec env
            Backend::Buildah => {
                let mut bud = vec!["buildah".to_string(), "bud".to_string()];
                if let Dest::Push(_) = dest {
                    bud.extend(["--authfile".to_string(), self.auth_path().to_string()]);
                }
                if let Some(target) = target {
                    bud.push("--target".to_string());
                    bud.push(target.to_string());
                }
                for arg in build_args {
                    bud.push("--build-arg".to_string());
                    bud.push(arg.clone());
                }
                for (k, v) in labels {
                    bud.push("--label".to_string());
                    bud.push(format!("{k}={v}"));
                }
                let tag = match dest {
                    Dest::Push(image) | Dest::Local(image) => image,
                };
                bud.extend([
                    "-f".to_string(),
                    format!("{ws}/{dockerfile}"),
                    "-t".to_string(),
                    tag.to_string(),
                    ws.to_string(),
                ]);
                match dest {
                    Dest::Push(image) => {
                        let push = vec![
                            "buildah".to_string(),
                            "push".to_string(),
                            "--authfile".to_string(),
                            self.auth_path().to_string(),
                            "--digestfile".to_string(),
                            self.digest_path().to_string(),
                            image.to_string(),
                            format!("docker://{image}"),
                        ];
                        vec![bud, push]
                    }
                    Dest::Local(_) => vec![bud],
                }
            }
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Backend::Buildkit => "buildkit",
            Backend::Kaniko => "kaniko",
            Backend::Buildah => "buildah",
        }
    }

    pub fn from_label(label: &str) -> Option<Self> {
        [Backend::Buildkit, Backend::Kaniko, Backend::Buildah]
            .into_iter()
            .find(|b| b.label() == label)
    }

    // where a persistent cache volume pays off for each backend
    fn cache_mount_path(&self) -> &'static str {
        match self {
            Backend::Buildkit => "/home/user/.local/share/buildkit",
            // kaniko caches base images under --cache-dir
            Backend::Kaniko => "/cache",
            Backend::Buildah => "/home/build/.local/share/containers",
        }
    }

    // volume named for the mount it replaces; kaniko gets a dedicated one
    fn cache_volume_name(&self) -> &'static str {
        match self {
            Backend::Buildkit => "buildkit-storage",
            Backend::Kaniko => "kaniko-cache",
            Backend::Buildah => "containers-storage",
        }
    }

    pub fn pod_spec(&self, name: &str, namespace: &str, opts: &PodOpts<'_>) -> Result<Pod> {
        let sleep = opts.deadline_secs.to_string();
        let mut container = match self {
            Backend::Buildkit => serde_json::json!({
                "name": "builder",
                "image": "moby/buildkit:rootless",
                "command": ["sleep", sleep],
                "env": [
                    // unprivileged pods can't unshare a pid namespace
                    { "name": "BUILDKITD_FLAGS", "value": "--oci-worker-no-process-sandbox" },
                    { "name": "HOME", "value": "/home/user" }
                ],
                "securityContext": {
                    "runAsUser": 1000,
                    "runAsGroup": 1000,
                    // rootlesskit needs syscalls the default profile masks
                    "seccompProfile": { "type": "Unconfined" }
                },
                "volumeMounts": [{
                    "name": "buildkit-storage",
                    "mountPath": "/home/user/.local/share/buildkit"
                }]
            }),
            Backend::Kaniko => serde_json::json!({
                "name": "builder",
                "image": "gcr.io/kaniko-project/executor:debug",
                "command": ["/busybox/sh", "-c", format!("sleep {sleep}")]
            }),
            Backend::Buildah => serde_json::json!({
                "name": "builder",
                "image": "quay.io/buildah/stable:latest",
                "command": ["sleep", sleep],
                // uid 1000 so buildah gets its own userns for layer unpack
                "securityContext": {
                    "runAsUser": 1000,
                    "runAsGroup": 1000
                },
                "env": [
                    { "name": "BUILDAH_ISOLATION", "value": "chroot" },
                    { "name": "STORAGE_DRIVER", "value": "vfs" },
                    { "name": "CONTAINERS_STORAGE_CONF", "value": "/tmp/storage.conf" },
                    { "name": "HOME", "value": "/home/build" }
                ],
                "volumeMounts": [{
                    "name": "containers-storage",
                    "mountPath": "/home/build/.local/share/containers"
                }]
            }),
        };
        if let Some(res) = opts.resources.json() {
            container["resources"] = res;
        }
        let mut volumes = match self {
            Backend::Buildkit => {
                serde_json::json!([{ "name": "buildkit-storage", "emptyDir": {} }])
            }
            Backend::Kaniko => serde_json::json!([]),
            Backend::Buildah => {
                serde_json::json!([{ "name": "containers-storage", "emptyDir": {} }])
            }
        };
        if let Some(cache) = opts.cache {
            self.apply_cache(&mut container, &mut volumes, cache)?;
        }
        let mut spec = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": { "app": "buildit", "buildit/backend": self.label() },
                // Ubuntu nodes' default AppArmor profile denies the mount
                // syscalls rootless builders need ("remount /: permission
                // denied" and friends). unconfined or bust.
                "annotations": {
                    "container.apparmor.security.beta.kubernetes.io/builder": "unconfined"
                }
            },
            "spec": {
                "restartPolicy": "Never",
                "activeDeadlineSeconds": opts.deadline_secs,
                "automountServiceAccountToken": false,
                "enableServiceLinks": false,
                "containers": [container],
                "volumes": volumes
            }
        });
        if let Some(owner) = opts.owner {
            spec["metadata"]["ownerReferences"] = serde_json::json!([owner]);
        }
        if let Some(meta) = opts.meta {
            for (k, v) in &meta.labels {
                spec["metadata"]["labels"][k] = serde_json::json!(v);
            }
            for (k, v) in &meta.annotations {
                spec["metadata"]["annotations"][k] = serde_json::json!(v);
            }
            if let Some(dir) = &meta.log_dir {
                let vols = spec["spec"]["volumes"]
                    .as_array_mut()
                    .ok_or_else(|| anyhow!("pod volumes are not a list"))?;
                vols.push(serde_json::json!({ "name": "buildit-logs", "emptyDir": {} }));
                let mounts = &mut spec["spec"]["containers"][0]["volumeMounts"];
                if mounts.is_null() {
                    *mounts = serde_json::json!([]);
                }
                mounts
                    .as_array_mut()
                    .ok_or_else(|| anyhow!("builder volumeMounts are not a list"))?
                    .push(serde_json::json!({ "name": "buildit-logs", "mountPath": dir }));
            }
        }
        if matches!(opts.cache, Some(CacheVolume::Pvc(_))) {
            // fresh PVC filesystems are root:root 0755 (unlike 0777 emptyDirs);
            // fsGroup lets the uid-1000 builders write to them
            spec["spec"]["securityContext"] = serde_json::json!({ "fsGroup": 1000 });
        }
        if let Some(init) = self.cache_perm_fix(opts.cache) {
            spec["spec"]["initContainers"] = serde_json::json!([init]);
        }
        if let Some(node) = opts.node {
            spec["spec"]["nodeSelector"] = serde_json::json!({ "kubernetes.io/hostname": node });
        }
        if !opts.idle_nodes.is_empty() {
            spec["spec"]["affinity"] = serde_json::json!({
                "nodeAffinity": {
                    "preferredDuringSchedulingIgnoredDuringExecution": [{
                        "weight": 100,
                        "preference": {
                            "matchExpressions": [{
                                "key": "kubernetes.io/hostname",
                                "operator": "In",
                                "values": opts.idle_nodes
                            }]
                        }
                    }]
                }
            });
        }
        serde_json::from_value(spec).context("building pod spec")
    }

    // point the backend's cache volume at the PVC or hostPath; for kaniko
    // also add the mount (it has no storage volume otherwise) and cache flags
    fn apply_cache(
        &self,
        container: &mut serde_json::Value,
        volumes: &mut serde_json::Value,
        cache: CacheVolume<'_>,
    ) -> Result<()> {
        let vol_name = self.cache_volume_name();
        let mut vol = serde_json::json!({ "name": vol_name });
        match cache {
            CacheVolume::Pvc(name) => {
                vol["persistentVolumeClaim"] = serde_json::json!({ "claimName": name });
            }
            CacheVolume::HostPath(path) => {
                vol["hostPath"] = serde_json::json!({ "path": path, "type": "DirectoryOrCreate" });
            }
        }
        let vols = volumes
            .as_array_mut()
            .ok_or_else(|| anyhow!("pod volumes are not a json array"))?;
        vols.retain(|v| v["name"] != vol_name);
        vols.push(vol);
        if let Backend::Kaniko = self {
            let mut mounts = container["volumeMounts"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            mounts.push(serde_json::json!({
                "name": vol_name,
                "mountPath": self.cache_mount_path()
            }));
            container["volumeMounts"] = serde_json::json!(mounts);
            // kaniko: cache-dir holds base images; layer cache goes to the
            // registry (push failures there are warnings, not errors)
            if let Some(cmd) = container["command"].as_array_mut() {
                cmd.push(serde_json::json!("--cache=true"));
                cmd.push(serde_json::json!(format!(
                    "--cache-dir={}",
                    self.cache_mount_path()
                )));
            }
        }
        Ok(())
    }

    // hostPath dirs come up root:root 0755 and fsGroup doesn't touch them,
    // so rootless builders need a one-shot chmod before they can write
    fn cache_perm_fix(&self, cache: Option<CacheVolume<'_>>) -> Option<serde_json::Value> {
        match (cache, self) {
            (Some(CacheVolume::HostPath(_)), Backend::Buildkit | Backend::Buildah) => {
                Some(serde_json::json!({
                    "name": "fix-cache-perms",
                    "image": "busybox:1.36",
                    "command": ["chmod", "1777", self.cache_mount_path()],
                    "securityContext": { "runAsUser": 0, "runAsGroup": 0 },
                    "volumeMounts": [{
                        "name": self.cache_volume_name(),
                        "mountPath": self.cache_mount_path()
                    }]
                }))
            }
            _ => None,
        }
    }

    // job mode: build runs as the container command with context staged by an
    // initContainer, so the Job gets real completion/retry/TTL semantics. the
    // digest goes to /dev/termination-log, which k8s preserves in pod status.
    fn job_builder_container(
        &self,
        image: &str,
        dockerfile: &str,
        target: Option<&str>,
        build_args: &[String],
        labels: &[(String, String)],
    ) -> serde_json::Value {
        let df = format!("/workspace/{dockerfile}");
        let mut container = match self {
            Backend::Buildkit => {
                let mut args: String = target
                    .map(|t| format!(" --opt {}", shell_quote(&format!("target={t}"))))
                    .unwrap_or_default();
                args.extend(
                    build_args
                        .iter()
                        .map(|a| format!(" --opt build-arg:{}", shell_quote(a))),
                );
                for (k, v) in labels {
                    args.push_str(&format!(
                        " --opt {}",
                        shell_quote(&format!("label:{k}={v}"))
                    ));
                }
                // metadata goes to /tmp first: buildctl writes it atomically
                // (tmp + rename) and you can't rename onto the bind-mounted
                // termination-log file, only write into it
                let script = format!(
                    "buildctl-daemonless.sh build --frontend dockerfile.v0 \
                     --local context=/workspace --local dockerfile=/workspace \
                     --opt filename={df} --output type=image,name={img},push=true \
                     --metadata-file /tmp/metadata.json{args} \
                     && cat /tmp/metadata.json > /dev/termination-log",
                    df = shell_quote(dockerfile),
                    img = shell_quote(image),
                );
                serde_json::json!({
                    "image": "moby/buildkit:rootless",
                    "command": ["sh", "-c", script],
                    "env": [
                        { "name": "BUILDKITD_FLAGS", "value": "--oci-worker-no-process-sandbox" },
                        { "name": "HOME", "value": "/home/user" },
                        { "name": "DOCKER_CONFIG", "value": "/auth" }
                    ],
                    "securityContext": {
                        "runAsUser": 1000,
                        "runAsGroup": 1000,
                        "seccompProfile": { "type": "Unconfined" }
                    },
                    "volumeMounts": [
                        { "name": "buildkit-storage", "mountPath": "/home/user/.local/share/buildkit" }
                    ]
                })
            }
            Backend::Kaniko => {
                let mut command = vec![
                    "/kaniko/executor".to_string(),
                    format!("--dockerfile={df}"),
                    "--context=dir:///workspace".to_string(),
                    format!("--destination={image}"),
                    "--digest-file=/dev/termination-log".to_string(),
                ];
                if let Some(target) = target {
                    command.push(format!("--target={target}"));
                }
                for arg in build_args {
                    command.push(format!("--build-arg={arg}"));
                }
                for (k, v) in labels {
                    command.push(format!("--label={k}={v}"));
                }
                serde_json::json!({
                    "image": "gcr.io/kaniko-project/executor:debug",
                    "command": command,
                    "env": [{ "name": "DOCKER_CONFIG", "value": "/auth" }]
                })
            }
            Backend::Buildah => {
                let mut args: String = target
                    .map(|t| format!(" --target {}", shell_quote(t)))
                    .unwrap_or_default();
                args.extend(
                    build_args
                        .iter()
                        .map(|a| format!(" --build-arg {}", shell_quote(a))),
                );
                for (k, v) in labels {
                    args.push_str(&format!(" --label {}", shell_quote(&format!("{k}={v}"))));
                }
                let script = format!(
                    "printf '[storage]\\ndriver = \"vfs\"\\ngraphroot = \"/home/build/.local/share/containers/storage\"\\nrunroot = \"/tmp/containers-run\"\\n' > /tmp/storage.conf \
                     && mkdir -p /tmp/containers-run \
                     && buildah bud --authfile /auth/config.json{args} -f {df} -t {img} /workspace \
                     && buildah push --authfile /auth/config.json --digestfile /dev/termination-log {img} docker://{img}",
                    img = shell_quote(image),
                    df = shell_quote(&df),
                );
                serde_json::json!({
                    "image": "quay.io/buildah/stable:latest",
                    "command": ["sh", "-c", script],
                    "env": [
                        { "name": "BUILDAH_ISOLATION", "value": "chroot" },
                        { "name": "STORAGE_DRIVER", "value": "vfs" },
                        { "name": "CONTAINERS_STORAGE_CONF", "value": "/tmp/storage.conf" },
                        { "name": "HOME", "value": "/home/build" }
                    ],
                    "securityContext": { "runAsUser": 1000, "runAsGroup": 1000 },
                    "volumeMounts": [
                        { "name": "containers-storage", "mountPath": "/home/build/.local/share/containers" }
                    ]
                })
            }
        };
        container["name"] = serde_json::json!("builder");
        // carry failure logs in the message when there's no digest to report
        container["terminationMessagePolicy"] = serde_json::json!("FallbackToLogsOnError");
        let mounts = container["volumeMounts"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let mut all = vec![
            serde_json::json!({ "name": "workspace", "mountPath": "/workspace" }),
            serde_json::json!({ "name": "auth", "mountPath": "/auth", "readOnly": true }),
        ];
        all.extend(mounts);
        container["volumeMounts"] = serde_json::json!(all);
        container
    }

    pub fn job_spec(
        &self,
        name: &str,
        namespace: &str,
        args: &crate::job::DetachArgs<'_>,
    ) -> Result<Job> {
        let mut builder = self.job_builder_container(
            args.image,
            args.dockerfile,
            args.target,
            args.build_args,
            args.labels,
        );
        if let Some(res) = args.resources.json() {
            builder["resources"] = res;
        }

        let mut backend_volumes = serde_json::json!(match self {
            Backend::Buildkit => {
                vec![serde_json::json!({ "name": "buildkit-storage", "emptyDir": {} })]
            }
            Backend::Kaniko => vec![],
            Backend::Buildah => {
                vec![serde_json::json!({ "name": "containers-storage", "emptyDir": {} })]
            }
        });
        if let Some(cache) = args.cache {
            self.apply_cache(&mut builder, &mut backend_volumes, cache)?;
        }
        let mut volumes = vec![
            serde_json::json!({ "name": "workspace", "emptyDir": {} }),
            serde_json::json!({
                "name": "auth",
                "secret": {
                    "secretName": name,
                    "items": [{ "key": ".dockerconfigjson", "path": "config.json" }]
                }
            }),
        ];
        if let Some(extra) = backend_volumes.as_array() {
            volumes.extend(extra.iter().cloned());
        }
        let mut spec = serde_json::json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": { "app": "buildit", "buildit/backend": self.label() },
                "annotations": { "buildit/image": args.image }
            },
            "spec": {
                // retries are safe: the context is content-addressed in the registry
                "backoffLimit": 2,
                "ttlSecondsAfterFinished": 3600,
                "activeDeadlineSeconds": 7200,
                "template": {
                    "metadata": {
                        "labels": { "app": "buildit", "buildit/backend": self.label() },
                        "annotations": {
                            "container.apparmor.security.beta.kubernetes.io/builder": "unconfined",
                            "container.apparmor.security.beta.kubernetes.io/fetch-context": "unconfined"
                        }
                    },
                    "spec": {
                        "restartPolicy": "Never",
                        "automountServiceAccountToken": false,
                        "enableServiceLinks": false,
                        // emptyDir is root:root 0755; fsGroup lets uid-1000 builders read/write it
                        "securityContext": { "fsGroup": 1000 },
                        "initContainers": [{
                            "name": "fetch-context",
                            "image": "gcr.io/go-containerregistry/crane:debug",
                            "command": ["sh", "-c", "crane export \"$CTX_REF\" - | tar -x -C /workspace"],
                            "env": [
                                { "name": "DOCKER_CONFIG", "value": "/auth" },
                                { "name": "CTX_REF", "value": args.ctx_ref }
                            ],
                            "volumeMounts": [
                                { "name": "workspace", "mountPath": "/workspace" },
                                { "name": "auth", "mountPath": "/auth", "readOnly": true }
                            ]
                        }],
                        "containers": [builder],
                        "volumes": volumes
                    }
                }
            }
        });
        if let Some(init) = self.cache_perm_fix(args.cache) {
            let inits = spec["spec"]["template"]["spec"]["initContainers"]
                .as_array_mut()
                .ok_or_else(|| anyhow!("job template has no initContainers array"))?;
            inits.insert(0, init);
        }
        if let Some(node) = args.node {
            spec["spec"]["template"]["spec"]["nodeSelector"] =
                serde_json::json!({ "kubernetes.io/hostname": node });
        }
        if !args.idle_nodes.is_empty() {
            spec["spec"]["template"]["spec"]["affinity"] = serde_json::json!({
                "nodeAffinity": {
                    "preferredDuringSchedulingIgnoredDuringExecution": [{
                        "weight": 100,
                        "preference": {
                            "matchExpressions": [{
                                "key": "kubernetes.io/hostname",
                                "operator": "In",
                                "values": args.idle_nodes
                            }]
                        }
                    }]
                }
            });
        }
        serde_json::from_value(spec).context("building job spec")
    }
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use crate::backend::{Backend, Resources};

    #[test]
    fn buildkit_build_steps() {
        let steps = Backend::Buildkit.build_steps(
            "quay.io/acme/foo:tag",
            "Dockerfile.tap",
            None,
            &["FOO=bar".to_string()],
            &[("quay.expires-after".to_string(), "1d".to_string())],
        );
        assert_eq!(steps.len(), 1);
        let argv = &steps[0];
        assert_eq!(argv[0], "buildctl-daemonless.sh");
        assert!(argv.contains(&"filename=Dockerfile.tap".to_string()));
        assert!(argv.contains(&"type=image,name=quay.io/acme/foo:tag,push=true".to_string()));
        assert!(argv.contains(&"build-arg:FOO=bar".to_string()));
        assert!(argv.contains(&"label:quay.expires-after=1d".to_string()));
    }

    #[test]
    fn kaniko_build_steps() {
        let steps = Backend::Kaniko.build_steps(
            "quay.io/acme/foo:tag",
            "Dockerfile.tap",
            None,
            &["FOO=bar".to_string()],
            &[("team".to_string(), "infra".to_string())],
        );
        assert_eq!(steps.len(), 1);
        let argv = &steps[0];
        assert_eq!(argv[0], "/kaniko/executor");
        assert!(argv.contains(&"--dockerfile=/workspace/Dockerfile.tap".to_string()));
        assert!(argv.contains(&"--destination=quay.io/acme/foo:tag".to_string()));
        assert!(argv.contains(&"--digest-file=/tmp/digest".to_string()));
        assert!(argv.contains(&"--build-arg=FOO=bar".to_string()));
        assert!(argv.contains(&"--label".to_string()) || argv.contains(&"team=infra".to_string()));
    }

    #[test]
    fn buildah_build_steps() {
        let steps = Backend::Buildah.build_steps(
            "quay.io/acme/foo:tag",
            "Dockerfile",
            None,
            &[],
            &[("team".to_string(), "infra".to_string())],
        );
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0][..2], ["buildah".to_string(), "bud".to_string()]);
        assert_eq!(steps[1][..2], ["buildah".to_string(), "push".to_string()]);
        assert_eq!(
            steps[1].last().map(String::as_str),
            Some("docker://quay.io/acme/foo:tag")
        );
        assert!(steps[1].contains(&"--digestfile".to_string()));
        assert!(steps[0].contains(&"team=infra".to_string()));
    }

    #[test]
    fn shell_quote_survives_hostile_input() {
        assert_eq!(crate::backend::shell_quote("plain"), "'plain'");
        assert_eq!(
            crate::backend::shell_quote("a'b; rm -rf /"),
            r#"'a'\''b; rm -rf /'"#
        );
    }

    #[test]
    fn job_specs_deserialize() {
        let resources = crate::backend::Resources {
            requests: vec![("cpu".to_string(), "2".to_string())],
            limits: vec![("memory".to_string(), "8Gi".to_string())],
        };
        for backend in [Backend::Buildkit, Backend::Buildah, Backend::Kaniko] {
            let job = backend
                .job_spec(
                    "buildit-abc123",
                    "builds",
                    &crate::job::DetachArgs {
                        image: "quay.io/acme/foo:tag",
                        dockerfile: "Dockerfile",
                        target: Some("runtime"),
                        build_args: &["FOO=bar".to_string()],
                        ctx_ref: "quay.io/acme/foo:buildit-ctx-deadbeef0123",
                        authfile: b"",
                        idle_nodes: &["node-a".to_string()],
                        resources: &resources,
                        labels: &[("team".to_string(), "infra".to_string())],
                        cache: Some(crate::backend::CacheVolume::HostPath("/mnt/nvme/c")),
                        node: Some("nvme-node-1"),
                    },
                )
                .unwrap();
            let spec = job.spec.unwrap();
            assert_eq!(spec.backoff_limit, Some(2));
            assert_eq!(spec.ttl_seconds_after_finished, Some(3600));
            let pod_spec = spec.template.spec.unwrap();
            assert!(pod_spec.affinity.is_some());
            assert_eq!(
                pod_spec.security_context.unwrap().fs_group,
                Some(1000),
                "uid-1000 builders need the emptyDir group-writable"
            );
            let inits = pod_spec.init_containers.unwrap();
            assert!(inits.iter().any(|c| c.name == "fetch-context"));
            assert_eq!(
                inits.iter().any(|c| c.name == "fix-cache-perms"),
                backend != Backend::Kaniko,
                "{backend:?}"
            );
            assert_eq!(
                pod_spec.node_selector.as_ref().unwrap()["kubernetes.io/hostname"],
                "nvme-node-1"
            );
            let builder = &pod_spec.containers[0];
            assert_eq!(builder.name, "builder");
            assert_eq!(
                builder.termination_message_policy.as_deref(),
                Some("FallbackToLogsOnError")
            );
            let mounts: Vec<_> = builder
                .volume_mounts
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|m| m.mount_path.as_str())
                .collect();
            assert!(mounts.contains(&"/workspace"), "{mounts:?}");
            assert!(mounts.contains(&"/auth"), "{mounts:?}");
            let cmd = builder.command.as_deref().unwrap_or_default().join(" ");
            assert!(cmd.contains("/dev/termination-log"), "{cmd}");
            let want = match backend {
                Backend::Buildkit => "--opt 'target=runtime'",
                Backend::Buildah => "--target 'runtime'",
                Backend::Kaniko => "--target=runtime",
            };
            assert!(cmd.contains(want), "{backend:?}: {cmd}");
            assert_eq!(pod_spec.automount_service_account_token, Some(false));
            assert_eq!(pod_spec.enable_service_links, Some(false));
            let res = builder.resources.as_ref().unwrap();
            assert_eq!(res.requests.as_ref().unwrap()["cpu"].0, "2");
            assert_eq!(res.limits.as_ref().unwrap()["memory"].0, "8Gi");
            let annotations = job.metadata.annotations.unwrap();
            assert_eq!(
                annotations.get("buildit/image").map(String::as_str),
                Some("quay.io/acme/foo:tag")
            );
        }
    }

    #[test]
    fn digest_parsing() {
        assert_eq!(
            Backend::Kaniko.digest_from("sha256:abc\n").unwrap(),
            "sha256:abc"
        );
        let meta = r#"{"containerimage.digest":"sha256:def","image.name":"x"}"#;
        assert_eq!(Backend::Buildkit.digest_from(meta).unwrap(), "sha256:def");
        assert!(Backend::Buildkit.digest_from("{}").is_err());
        assert!(Backend::Kaniko.digest_from("garbage").is_err());
    }

    fn opts<'a>(idle_nodes: &'a [String], resources: &'a Resources) -> crate::backend::PodOpts<'a> {
        crate::backend::PodOpts {
            idle_nodes,
            resources,
            cache: None,
            node: None,
            deadline_secs: 7200,
            owner: None,
            meta: None,
        }
    }

    #[test]
    fn pod_spec_cache_and_node_pinning() {
        let empty = Resources::default();
        for backend in [Backend::Buildkit, Backend::Buildah, Backend::Kaniko] {
            let pvc_opts = crate::backend::PodOpts {
                cache: Some(crate::backend::CacheVolume::Pvc("build-cache")),
                node: Some("nvme-node-1"),
                ..opts(&[], &empty)
            };
            let pod = backend
                .pod_spec("buildit-abc123", "builds", &pvc_opts)
                .unwrap();
            let spec = pod.spec.unwrap();
            assert_eq!(
                spec.node_selector.as_ref().unwrap()["kubernetes.io/hostname"],
                "nvme-node-1"
            );
            let vols = spec.volumes.unwrap();
            assert!(
                vols.iter().any(|v| v
                    .persistent_volume_claim
                    .as_ref()
                    .is_some_and(|c| c.claim_name == "build-cache")),
                "{backend:?} missing pvc volume"
            );

            let hp_opts = crate::backend::PodOpts {
                cache: Some(crate::backend::CacheVolume::HostPath("/mnt/nvme/cache")),
                ..opts(&[], &empty)
            };
            let pod = backend
                .pod_spec("buildit-abc123", "builds", &hp_opts)
                .unwrap();
            let spec = pod.spec.unwrap();
            let vols = spec.volumes.unwrap();
            assert!(
                vols.iter().any(|v| v
                    .host_path
                    .as_ref()
                    .is_some_and(|h| h.path == "/mnt/nvme/cache")),
                "{backend:?} missing hostPath volume"
            );
            // rootless backends need the chmod init; kaniko runs as root
            let has_fix = spec
                .init_containers
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|c| c.name == "fix-cache-perms");
            assert_eq!(has_fix, backend != Backend::Kaniko, "{backend:?}");
            if backend == Backend::Kaniko {
                let cmd = spec.containers[0].command.as_deref().unwrap_or_default();
                assert!(cmd.iter().any(|a| a == "--cache=true"), "{cmd:?}");
            }
        }
    }

    #[test]
    fn pod_specs_deserialize_with_and_without_affinity() {
        let cpu2 = crate::backend::Resources {
            requests: vec![("cpu".to_string(), "2".to_string())],
            limits: vec![],
        };
        for backend in [Backend::Buildkit, Backend::Buildah, Backend::Kaniko] {
            let pod = backend
                .pod_spec("buildit-abc123", "builds", &opts(&[], &cpu2))
                .unwrap();
            let res = pod.spec.as_ref().unwrap().containers[0]
                .resources
                .as_ref()
                .unwrap();
            assert_eq!(res.requests.as_ref().unwrap()["cpu"].0, "2");
            assert!(res.limits.is_none());
            let spec = pod.spec.unwrap();
            assert_eq!(spec.active_deadline_seconds, Some(7200));
            assert_eq!(spec.restart_policy.as_deref(), Some("Never"));
            assert!(spec.affinity.is_none());
            let labels = pod.metadata.labels.unwrap();
            assert_eq!(labels.get("app").map(String::as_str), Some("buildit"));

            let idle = vec!["node-a".to_string(), "node-b".to_string()];
            let none = Resources::default();
            let pod = backend
                .pod_spec("buildit-abc123", "builds", &opts(&idle, &none))
                .unwrap();
            let affinity = pod.spec.unwrap().affinity.unwrap();
            let prefs = affinity
                .node_affinity
                .unwrap()
                .preferred_during_scheduling_ignored_during_execution
                .unwrap();
            assert_eq!(prefs.len(), 1);
            let expr = &prefs[0].preference.match_expressions.as_ref().unwrap()[0];
            assert_eq!(expr.key, "kubernetes.io/hostname");
            assert_eq!(expr.values.as_ref().unwrap(), &idle);
        }
    }

    #[test]
    fn target_flag_per_backend() {
        let push = |b: Backend| {
            b.build_steps("quay.io/a/b:t", "Dockerfile", Some("builder"), &[], &[])
                .concat()
        };
        let local = |b: Backend| {
            b.local_build_steps(
                "localhost/buildit/x:latest",
                "Dockerfile",
                Some("builder"),
                &[],
            )
            .concat()
        };
        for argv in [push(Backend::Buildkit), local(Backend::Buildkit)] {
            let at = argv.iter().position(|a| a == "target=builder").unwrap();
            assert_eq!(argv[at - 1], "--opt");
        }
        for argv in [push(Backend::Buildah), local(Backend::Buildah)] {
            let at = argv.iter().position(|a| a == "--target").unwrap();
            assert_eq!(argv[at + 1], "builder");
        }
        for argv in [push(Backend::Kaniko), local(Backend::Kaniko)] {
            assert!(argv.contains(&"--target=builder".to_string()), "{argv:?}");
        }
        let none = Backend::Buildah
            .local_build_steps("t", "Dockerfile", None, &[])
            .concat();
        assert!(!none.iter().any(|a| a.contains("target")), "{none:?}");
    }

    #[test]
    fn local_builds_push_nothing_and_need_no_auth() {
        let tag = "localhost/buildit/b1:latest";
        let buildah =
            Backend::Buildah.local_build_steps(tag, "Dockerfile", None, &["A=1".to_string()]);
        assert_eq!(buildah.len(), 1, "no push step");
        let bud = &buildah[0];
        assert_eq!(bud[..2], ["buildah".to_string(), "bud".to_string()]);
        assert!(!bud.contains(&"--authfile".to_string()), "{bud:?}");
        let at = bud.iter().position(|a| a == "-t").unwrap();
        assert_eq!(bud[at + 1], tag);
        assert!(bud.contains(&"A=1".to_string()));

        let buildkit = Backend::Buildkit
            .local_build_steps(tag, "Dockerfile", None, &[])
            .concat();
        assert!(
            !buildkit
                .iter()
                .any(|a| a == "--output" || a.contains("push")),
            "{buildkit:?}"
        );

        let kaniko = Backend::Kaniko
            .local_build_steps(tag, "Dockerfile", None, &[])
            .concat();
        assert!(kaniko.contains(&"--no-push".to_string()), "{kaniko:?}");
        assert!(
            !kaniko.iter().any(|a| a.starts_with("--destination")),
            "{kaniko:?}"
        );
    }

    #[test]
    fn pods_get_no_token_no_service_env_and_an_owner() {
        let none = Resources::default();
        let owner = k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            name: "loop-0".to_string(),
            uid: "1234".to_string(),
            ..Default::default()
        };
        for backend in [Backend::Buildkit, Backend::Buildah, Backend::Kaniko] {
            let pod = backend
                .pod_spec(
                    "buildit-abc",
                    "builds",
                    &crate::backend::PodOpts {
                        deadline_secs: 900,
                        owner: Some(&owner),
                        ..opts(&[], &none)
                    },
                )
                .unwrap();
            let spec = pod.spec.unwrap();
            assert_eq!(spec.automount_service_account_token, Some(false));
            assert_eq!(spec.enable_service_links, Some(false));
            assert_eq!(spec.active_deadline_seconds, Some(900));
            let c = &spec.containers[0];
            assert!(
                c.env
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .all(|e| e.value_from.is_none()),
                "{backend:?}"
            );
            assert!(c.env_from.is_none());
            let cmd = c.command.as_deref().unwrap_or_default().join(" ");
            assert!(cmd.contains("sleep") && cmd.contains("900"), "{cmd}");
            let refs = pod.metadata.owner_references.unwrap();
            assert_eq!(refs, vec![owner.clone()]);

            let pod = backend
                .pod_spec("buildit-abc", "builds", &opts(&[], &none))
                .unwrap();
            assert!(pod.metadata.owner_references.is_none());
            assert_eq!(
                pod.spec.unwrap().automount_service_account_token,
                Some(false)
            );
        }
    }

    #[test]
    fn pod_meta_adds_labels_annotations_and_a_log_volume() {
        let none = Resources::default();
        let meta = crate::backend::PodMeta {
            labels: vec![("buildit.dev/build-id".to_string(), "b1".to_string())],
            annotations: vec![("buildit.dev/context".to_string(), "svc".to_string())],
            log_dir: Some("/buildit/logs".to_string()),
        };
        for backend in [Backend::Buildkit, Backend::Buildah, Backend::Kaniko] {
            let pod = backend
                .pod_spec(
                    "b1",
                    "builds",
                    &crate::backend::PodOpts {
                        meta: Some(&meta),
                        ..opts(&[], &none)
                    },
                )
                .unwrap();
            let labels = pod.metadata.labels.unwrap();
            assert_eq!(labels["buildit.dev/build-id"], "b1");
            assert_eq!(labels["app"], "buildit");
            assert_eq!(labels[crate::backend::BACKEND_LABEL], backend.label());
            let notes = pod.metadata.annotations.unwrap();
            assert_eq!(notes["buildit.dev/context"], "svc");
            assert!(notes.contains_key("container.apparmor.security.beta.kubernetes.io/builder"));
            let spec = pod.spec.unwrap();
            let vols = spec.volumes.unwrap();
            assert!(
                vols.iter()
                    .any(|v| v.name == "buildit-logs" && v.empty_dir.is_some()),
                "{backend:?}"
            );
            let mounts = spec.containers[0].volume_mounts.clone().unwrap();
            assert!(
                mounts
                    .iter()
                    .any(|m| m.name == "buildit-logs" && m.mount_path == "/buildit/logs"),
                "{backend:?}"
            );
            assert_eq!(Backend::from_label(backend.label()), Some(backend));

            let plain = backend.pod_spec("b1", "builds", &opts(&[], &none)).unwrap();
            assert!(
                !plain
                    .spec
                    .unwrap()
                    .volumes
                    .unwrap_or_default()
                    .iter()
                    .any(|v| v.name == "buildit-logs")
            );
        }
        assert_eq!(Backend::from_label("docker"), None);
    }
}
