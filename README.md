# buildit

Build and push container images remotely on a Kubernetes cluster, from a
machine that can't build them locally. Born on an Apple Silicon mac where
heavy amd64 cross-builds segfault under QEMU emulation; the cluster's amd64
nodes build natively instead.

No daemons, no privileged pods, no in-cluster controllers: `buildit` creates
a single unprivileged builder pod, streams your build context to it over the
Kubernetes exec API, builds + pushes, prints the digest, and deletes the pod.

```
┌─────────────┐  tar.gz over exec stdin   ┌──────────────────────┐
│ your laptop │ ────────────────────────► │ builder pod (uid1000)│──► registry
│  (buildit)  │ ◄──────────────────────── │ buildkit/kaniko/     │
└─────────────┘   build logs + digest     │ buildah, no privilege│
                                          └──────────────────────┘
```

## Install

```sh
cargo install --git https://github.com/wseaton/buildit
```

## Usage

```sh
buildit build quay.io/acme/foo:tag                 # context=., Dockerfile, current kubecontext
buildit build quay.io/acme/foo:tag \
  -f Dockerfile.prod -c ./svc -n builds \
  --backend buildah --build-arg FOO=bar
JOB=$(buildit build quay.io/acme/foo:tag --detach | tail -1)   # fire and forget
buildit wait $JOB                                  # reattach anytime, prints the digest
buildit clean                                      # delete leftover builder pods/jobs
```

The last stdout line is the digest-pinned ref (`repo@sha256:...`), everything
else goes to stderr, so `$(buildit build ... | tail -1)` is scriptable.
Logging is `tracing`-based; set `RUST_LOG` (e.g. `RUST_LOG=debug` for kube
client internals) to adjust verbosity.

The namespace defaults to `default`; override per-invocation with `-n` or
persistently with `BUILDIT_NAMESPACE`. `--kubecontext` selects a kubeconfig
context (defaults to the current one).

Registry auth comes from `~/.docker/config.json`; only the destination
registry's inline token is shipped to the pod (`docker login <registry>` first
if it's missing; a macOS keychain credsStore won't work).

`.dockerignore` is honored when the context is tarred; top-level `.git/` and
`target/` are always excluded.

## Backends

All run in unprivileged pods. Each pod carries a
`container.apparmor.security.beta.kubernetes.io/builder: unconfined`
annotation: on Ubuntu nodes the default AppArmor profile denies the mount
syscalls rootless user-namespace builders need, which presents as
`remount /: permission denied` (buildah) or `failed to share mount point: /`
(rootlesskit). If your cluster policy forbids the annotation, kaniko still
works — it never touches mount syscalls.

- `buildkit` (default): rootless buildkitd via `buildctl-daemonless.sh`, uid 1000
- `kaniko`: userspace layer unpack; unmaintained upstream but a solid fallback
- `buildah`: rootless, chroot isolation + vfs storage, needs a minimal
  `storage.conf` because the stable image's config points at a rootful store
  (vfs is slower than buildkit on layer-heavy images)

## Scheduling

Before creating the builder pod, buildit lists nodes and pods and adds a
*preferred* nodeAffinity for Ready, schedulable nodes with zero
GPU-requesting pods, keeping builds off nodes with active GPU workloads.
Best effort only: if the scout fails (RBAC) or no node is idle, the build
schedules normally. `--any-node` skips the scout.

Builder pods are labeled `app=buildit`, self-terminate after 2h
(`activeDeadlineSeconds`), and are deleted on completion, error, or ctrl-C;
`buildit clean` sweeps any survivors.

## Detached builds (`--detach` + `wait`)

The default mode is interactive: context streamed over the exec API, build
driven by execs, pod deleted when done. Fast and zero-footprint, but the
build's fate is tied to your connection.

`--detach` trades a little registry storage for durability. The context is
pushed to the registry as a single-layer OCI image (via
[`oci-client`](https://github.com/oras-project/rust-oci-client)), tagged
`buildit-ctx-<sha256[..12]>` in the target repo — content-addressed, so an
unchanged tree skips the upload. The build then runs as a Kubernetes **Job**:
an initContainer (`crane export`) stages the context into an emptyDir, the
builder runs as the container's actual command, and the digest is written to
`/dev/termination-log` so it survives in pod status. Registry auth rides in a
short-lived Secret owner-ref'd to the Job.

Because the context lives in the registry, everything Jobs are good at works
for real: `backoffLimit: 2` retries rebuild from a re-fetched context,
`ttlSecondsAfterFinished: 3600` garbage-collects the Job, its pods, and (via
the ownerRef) the Secret an hour after finishing.

`buildit build ... --detach` prints the job name and exits; `buildit wait
<job>` reattaches at any point — it follows builder logs across retries,
then prints the digest-pinned ref (read from the termination message, with a
registry manifest HEAD as fallback). Killing `wait` kills nothing.

One piece of leftover state: `buildit-ctx-*` tags accumulate in the repo.
They're tiny (your source tree, compressed by the registry) and
content-addressed; prune them with your registry's UI/API or a lifecycle
policy.

## Agent skill

[`skills/buildit/`](skills/buildit/SKILL.md) ships an
[Agent Skill](https://docs.claude.com/en/docs/agents-and-tools/agent-skills)
that teaches Claude Code (or any skill-aware agent) when and how to reach for
buildit — e.g. when a Dockerfile needs an amd64 image and the local machine
is Apple Silicon. Install it by copying or symlinking:

```sh
ln -s "$(pwd)/skills/buildit" ~/.claude/skills/buildit
```

## License

MIT
