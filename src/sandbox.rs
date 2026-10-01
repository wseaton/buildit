use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail};

pub const EXCLUDED: [&str; 6] = [".git", ".mcp.json", ".claude", ".buildit", ".kube", ".jira"];

pub const RESULTS_DIR: &str = ".buildit";

const SYNC_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxName(String);

impl SandboxName {
    pub fn parse(raw: &str) -> Result<Self> {
        let ok = !raw.is_empty()
            && raw.len() <= 128
            && !raw.starts_with('-')
            && raw
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
        if !ok {
            bail!("invalid sandbox name {raw:?}");
        }
        Ok(Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelPath(String);

impl RelPath {
    pub fn parse(raw: &str) -> Result<Self> {
        let mut parts = Vec::new();
        for comp in Path::new(raw).components() {
            match comp {
                Component::Normal(part) => {
                    let part = part
                        .to_str()
                        .ok_or_else(|| anyhow!("path {raw:?} is not utf-8"))?;
                    if EXCLUDED.contains(&part) {
                        bail!("path {raw:?} goes through {part}, which is never shipped");
                    }
                    parts.push(part);
                }
                Component::CurDir => {}
                Component::ParentDir => bail!("path {raw:?} must not contain `..`"),
                Component::RootDir | Component::Prefix(_) => {
                    bail!("path {raw:?} must be relative")
                }
            }
        }
        if parts.is_empty() {
            bail!("path {raw:?} must name a subdirectory, not the root");
        }
        Ok(Self(parts.join("/")))
    }

    pub fn parse_container(raw: &str) -> Result<Self> {
        Self::parse(raw.trim_start_matches('/'))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub enum Workspace {
    Openshell { workdir: String },
    Local { dir: PathBuf },
}

impl Workspace {
    pub fn needs_sandbox(&self) -> bool {
        matches!(self, Workspace::Openshell { .. })
    }

    pub fn fetch_context(
        &self,
        sandbox: Option<&SandboxName>,
        ctx: &RelPath,
        dest: &Path,
    ) -> Result<()> {
        match self {
            Workspace::Openshell { workdir } => {
                let name = sandbox.ok_or_else(|| anyhow!("no sandbox name for this request"))?;
                let path = format!("{}/{}", workdir.trim_end_matches('/'), ctx.as_str());
                verified_sync(&mut || sandbox_tree_hash(name, &path), &mut || {
                    download(name, &path, dest)
                })?;
            }
            Workspace::Local { dir } => {
                let src = confined(dir, ctx)?;
                let _ = std::fs::remove_dir_all(dest);
                copy_tree(&src, dest)?;
            }
        }
        sanitize(dest)
    }

    pub fn publish(&self, sandbox: Option<&SandboxName>, results: &Path) -> Result<()> {
        let id = results
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("results dir {} has no name", results.display()))?;
        match self {
            Workspace::Openshell { workdir } => {
                let name = sandbox.ok_or_else(|| anyhow!("no sandbox name for this request"))?;
                run(
                    &upload_argv(name, results, workdir),
                    "openshell sandbox upload",
                )?;
                Ok(())
            }
            Workspace::Local { dir } => {
                let root = dir.join(RESULTS_DIR);
                refuse_symlink(&root)?;
                let dest = root.join(id);
                refuse_symlink(&dest)?;
                copy_tree(results, &dest)
            }
        }
    }
}

fn run(argv: &[String], what: &str) -> Result<String> {
    let (bin, args) = argv
        .split_first()
        .ok_or_else(|| anyhow!("empty argv for {what}"))?;
    let out = Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .with_context(|| format!("running `{what}` (is it on PATH?)"))?;
    if !out.status.success() {
        bail!(
            "{what} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    String::from_utf8(out.stdout).with_context(|| format!("{what} output was not utf-8"))
}

fn download_argv(name: &SandboxName, path: &str, dest: &Path) -> Vec<String> {
    vec![
        "openshell".to_string(),
        "sandbox".to_string(),
        "download".to_string(),
        name.as_str().to_string(),
        path.to_string(),
        dest.to_string_lossy().into_owned(),
    ]
}

fn upload_argv(name: &SandboxName, results: &Path, workdir: &str) -> Vec<String> {
    vec![
        "openshell".to_string(),
        "sandbox".to_string(),
        "upload".to_string(),
        "--no-git-ignore".to_string(),
        name.as_str().to_string(),
        results.to_string_lossy().into_owned(),
        format!("{}/{RESULTS_DIR}/", workdir.trim_end_matches('/')),
    ]
}

fn tree_hash_argv(name: &SandboxName, path: &str) -> Vec<String> {
    vec![
        "openshell".to_string(),
        "sandbox".to_string(),
        "exec".to_string(),
        "--name".to_string(),
        name.as_str().to_string(),
        "--no-tty".to_string(),
        "--".to_string(),
        "sh".to_string(),
        "-c".to_string(),
        format!(
            "cd {} && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum",
            shell_quote(path)
        ),
    ]
}

fn sandbox_tree_hash(name: &SandboxName, path: &str) -> Result<String> {
    let hash = run(&tree_hash_argv(name, path), "openshell sandbox exec")?;
    let hash = hash.trim();
    if hash.is_empty() {
        bail!("sandbox tree hash came back empty");
    }
    Ok(hash.to_string())
}

fn download(name: &SandboxName, path: &str, dest: &Path) -> Result<()> {
    let _ = std::fs::remove_dir_all(dest);
    std::fs::create_dir_all(dest).with_context(|| format!("creating {}", dest.display()))?;
    run(
        &download_argv(name, path, dest),
        "openshell sandbox download",
    )?;
    Ok(())
}

fn verified_sync(
    tree_hash: &mut dyn FnMut() -> Result<String>,
    download: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    let mut before = match tree_hash() {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!("sandbox tree hash unavailable ({e:#}); syncing unverified");
            return download();
        }
    };
    for attempt in 1..=SYNC_ATTEMPTS {
        download()?;
        let after = tree_hash()?;
        if before == after {
            return Ok(());
        }
        tracing::warn!(
            "sandbox tree changed during sync (attempt {attempt}/{SYNC_ATTEMPTS}); retrying"
        );
        before = after;
    }
    bail!(
        "sandbox tree kept changing across {SYNC_ATTEMPTS} sync attempts; a background process \
         is still writing. Let it finish, then call the tool again"
    )
}

fn confined(root: &Path, rel: &RelPath) -> Result<PathBuf> {
    let mut path = root.to_path_buf();
    for part in rel.as_str().split('/') {
        path.push(part);
        if refuse_symlink(&path).is_err() {
            bail!("symlinks are refused: {} is a link", rel.as_str());
        }
    }
    if !path.is_dir() {
        bail!("context {} is not a directory", rel.as_str());
    }
    Ok(path)
}

fn refuse_symlink(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            bail!("symlinks are refused: {}", path.display())
        }
        _ => Ok(()),
    }
}

fn is_excluded(name: &std::ffi::OsStr) -> bool {
    name.to_str().is_some_and(|n| EXCLUDED.contains(&n))
}

fn copy_tree(src: &Path, dest: &Path) -> Result<()> {
    copy_tree_in(src, src, dest)
}

fn copy_tree_in(root: &Path, src: &Path, dest: &Path) -> Result<()> {
    std::fs::create_dir_all(dest).with_context(|| format!("creating {}", dest.display()))?;
    for entry in std::fs::read_dir(src).with_context(|| format!("reading {}", src.display()))? {
        let entry = entry.with_context(|| format!("reading {}", src.display()))?;
        if is_excluded(&entry.file_name()) {
            continue;
        }
        let from = entry.path();
        let rel = from
            .strip_prefix(root)
            .unwrap_or(&from)
            .display()
            .to_string();
        let to = dest.join(entry.file_name());
        let ft = entry
            .file_type()
            .with_context(|| format!("inspecting {rel}"))?;
        if ft.is_symlink() {
            bail!("symlinks are refused: {rel}");
        } else if ft.is_dir() {
            copy_tree_in(root, &from, &to)?;
        } else if ft.is_file() {
            refuse_symlink(&to)?;
            std::fs::copy(&from, &to).with_context(|| format!("copying {rel}"))?;
        } else {
            bail!("special files are refused: {rel}");
        }
    }
    Ok(())
}

pub fn sanitize(root: &Path) -> Result<()> {
    sanitize_in(root, root)
}

fn sanitize_in(root: &Path, dir: &Path) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry.with_context(|| format!("reading {}", dir.display()))?;
        let path = entry.path();
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .display()
            .to_string();
        let ft = entry
            .file_type()
            .with_context(|| format!("inspecting {rel}"))?;
        if is_excluded(&entry.file_name()) {
            if ft.is_dir() {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            }
            .with_context(|| format!("removing excluded {rel}"))?;
        } else if ft.is_symlink() {
            bail!("symlinks are refused in the build context: {rel}");
        } else if ft.is_dir() {
            sanitize_in(root, &path)?;
        } else if !ft.is_file() {
            bail!("special files are refused in the build context: {rel}");
        }
    }
    Ok(())
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::Path;

    use crate::sandbox::{
        EXCLUDED, RelPath, SandboxName, Workspace, download_argv, sanitize, tree_hash_argv,
        upload_argv, verified_sync,
    };

    fn touch(path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, b"x").unwrap();
    }

    fn tree(root: &Path) -> BTreeSet<String> {
        fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) {
            for e in fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
                if p.is_dir() {
                    walk(root, &p, out);
                } else {
                    out.insert(rel);
                }
            }
        }
        let mut out = BTreeSet::new();
        walk(root, root, &mut out);
        out
    }

    #[test]
    fn rel_path_confines_to_a_subdir() {
        assert_eq!(RelPath::parse("svc").unwrap().as_str(), "svc");
        assert_eq!(RelPath::parse("./svc/api/").unwrap().as_str(), "svc/api");
        for bad in [
            "",
            ".",
            "./",
            "/",
            "/etc",
            "../x",
            "svc/../../x",
            "svc/..",
            "a/./../b",
        ] {
            assert!(RelPath::parse(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn rel_path_refuses_excluded_names_anywhere() {
        for name in EXCLUDED {
            assert!(RelPath::parse(name).is_err(), "{name}");
            assert!(RelPath::parse(&format!("svc/{name}")).is_err(), "{name}");
            assert!(RelPath::parse(&format!("{name}/x")).is_err(), "{name}");
        }
        assert!(RelPath::parse("svc/.gitignore").is_ok());
        assert!(RelPath::parse("svc/.github").is_ok());
    }

    #[test]
    fn container_paths_are_rooted_at_the_image() {
        assert_eq!(
            RelPath::parse_container("/out/bin/app").unwrap().as_str(),
            "out/bin/app"
        );
        assert_eq!(RelPath::parse_container("out/x").unwrap().as_str(), "out/x");
        assert!(RelPath::parse_container("/").is_err());
        assert!(RelPath::parse_container("/../etc").is_err());
        assert!(RelPath::parse_container("/out/../../etc").is_err());
    }

    #[test]
    fn sandbox_names_cannot_smuggle_flags() {
        assert!(SandboxName::parse("ci-1234-abcdef").is_ok());
        assert!(SandboxName::parse("my_box.v2").is_ok());
        for bad in [
            "",
            "-n",
            "--gateway=x",
            "a b",
            "a;b",
            "a/b",
            &"x".repeat(129),
        ] {
            assert!(SandboxName::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn openshell_argv() {
        let name = SandboxName::parse("ci-1-abc").unwrap();
        assert_eq!(
            download_argv(&name, "/sandbox/repo/svc", Path::new("/tmp/ctx")),
            [
                "openshell",
                "sandbox",
                "download",
                "ci-1-abc",
                "/sandbox/repo/svc",
                "/tmp/ctx"
            ]
        );
        assert_eq!(
            upload_argv(&name, Path::new("/tmp/x/buildit-1234"), "/sandbox/repo/"),
            [
                "openshell",
                "sandbox",
                "upload",
                "--no-git-ignore",
                "ci-1-abc",
                "/tmp/x/buildit-1234",
                "/sandbox/repo/.buildit/"
            ]
        );
        let hash = tree_hash_argv(&name, "/sandbox/it's");
        assert_eq!(
            hash[..9],
            [
                "openshell",
                "sandbox",
                "exec",
                "--name",
                "ci-1-abc",
                "--no-tty",
                "--",
                "sh",
                "-c"
            ]
        );
        assert!(
            hash[9].starts_with(r#"cd '/sandbox/it'\''s' && "#),
            "{}",
            hash[9]
        );
    }

    #[test]
    fn verified_sync_retries_on_churn_then_gives_up() {
        let hashes = RefCell::new(vec!["a", "a"].into_iter());
        let downloads = RefCell::new(0);
        verified_sync(
            &mut || Ok(hashes.borrow_mut().next().unwrap().to_string()),
            &mut || {
                *downloads.borrow_mut() += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(*downloads.borrow(), 1, "stable tree syncs once");

        let hashes = RefCell::new(vec!["a", "b", "b"].into_iter());
        let downloads = RefCell::new(0);
        verified_sync(
            &mut || Ok(hashes.borrow_mut().next().unwrap().to_string()),
            &mut || {
                *downloads.borrow_mut() += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(*downloads.borrow(), 2, "one retry after churn");

        let n = RefCell::new(0);
        let err = verified_sync(
            &mut || {
                *n.borrow_mut() += 1;
                Ok(n.borrow().to_string())
            },
            &mut || Ok(()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("kept changing"), "{err}");
    }

    #[test]
    fn verified_sync_falls_back_when_hash_is_unavailable() {
        let downloads = RefCell::new(0);
        verified_sync(&mut || anyhow::bail!("no sha256sum"), &mut || {
            *downloads.borrow_mut() += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(*downloads.borrow(), 1);
    }

    #[test]
    fn sanitize_strips_exclusions_at_every_depth() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        touch(&root.join("Dockerfile"));
        touch(&root.join("src/main.rs"));
        for name in EXCLUDED {
            touch(&root.join(name).join("inner"));
            touch(&root.join("deep/er").join(name).join("inner"));
        }
        touch(&root.join("deep/.mcp.json"));
        touch(&root.join("keep/.gitignore"));
        sanitize(root).unwrap();
        assert_eq!(
            tree(root),
            BTreeSet::from([
                "Dockerfile".to_string(),
                "src/main.rs".to_string(),
                "keep/.gitignore".to_string(),
            ])
        );
    }

    #[cfg(unix)]
    #[test]
    fn sanitize_refuses_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("a/f"));
        std::os::unix::fs::symlink("/etc/passwd", dir.path().join("a/link")).unwrap();
        let err = sanitize(dir.path()).unwrap_err().to_string();
        assert!(err.contains("symlinks are refused"), "{err}");
        assert!(err.contains("a/link"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn local_fetch_copies_the_subdir_minus_exclusions() {
        let work = tempfile::tempdir().unwrap();
        let w = work.path();
        touch(&w.join(".mcp.json"));
        touch(&w.join(".kube/config"));
        touch(&w.join("svc/Dockerfile"));
        touch(&w.join("svc/app/main.go"));
        touch(&w.join("svc/.git/HEAD"));
        touch(&w.join("svc/.claude/settings.json"));
        touch(&w.join("other/secret"));
        let ws = Workspace::Local {
            dir: w.to_path_buf(),
        };
        let out = tempfile::tempdir().unwrap();
        let dest = out.path().join("ctx");
        ws.fetch_context(None, &RelPath::parse("svc").unwrap(), &dest)
            .unwrap();
        assert_eq!(
            tree(&dest),
            BTreeSet::from(["Dockerfile".to_string(), "app/main.go".to_string()])
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_fetch_refuses_symlinked_context_and_contents() {
        let work = tempfile::tempdir().unwrap();
        let w = work.path();
        let outside = tempfile::tempdir().unwrap();
        touch(&outside.path().join("Dockerfile"));
        std::os::unix::fs::symlink(outside.path(), w.join("escape")).unwrap();
        let ws = Workspace::Local {
            dir: w.to_path_buf(),
        };
        let out = tempfile::tempdir().unwrap();
        let err = ws
            .fetch_context(
                None,
                &RelPath::parse("escape").unwrap(),
                &out.path().join("a"),
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains("symlinks are refused"), "{err}");

        touch(&w.join("svc/Dockerfile"));
        std::os::unix::fs::symlink("/etc/hosts", w.join("svc/hosts")).unwrap();
        let err = ws
            .fetch_context(None, &RelPath::parse("svc").unwrap(), &out.path().join("b"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("symlinks are refused"), "{err}");
    }

    #[test]
    fn openshell_fetch_needs_a_sandbox_name() {
        let ws = Workspace::Openshell {
            workdir: "/sandbox/repo".to_string(),
        };
        let out = tempfile::tempdir().unwrap();
        let err = ws
            .fetch_context(None, &RelPath::parse("svc").unwrap(), out.path())
            .unwrap_err()
            .to_string();
        assert!(err.contains("no sandbox name"), "{err}");
    }

    #[test]
    fn local_publish_merges_into_the_results_dir() {
        let work = tempfile::tempdir().unwrap();
        let ws = Workspace::Local {
            dir: work.path().to_path_buf(),
        };
        let stage = tempfile::tempdir().unwrap();
        let results = stage.path().join("buildit-0001");
        touch(&results.join("build.log"));
        ws.publish(None, &results).unwrap();
        fs::remove_dir_all(&results).unwrap();
        touch(&results.join("run.log"));
        touch(&results.join("out/bin/app"));
        ws.publish(None, &results).unwrap();
        assert_eq!(
            tree(&work.path().join(".buildit/buildit-0001")),
            BTreeSet::from([
                "build.log".to_string(),
                "run.log".to_string(),
                "out/bin/app".to_string(),
            ])
        );
    }
}
