use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail};
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags};
use rustix::io::Errno;

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
                let src = open_beneath(dir, ctx)?;
                let _ = std::fs::remove_dir_all(dest);
                std::fs::create_dir_all(dest)
                    .with_context(|| format!("creating {}", dest.display()))?;
                copy_dir(&src, &open_root(dest)?, Path::new(""), Links::Keep)?;
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
                let root = ensure_dir(&open_root(dir)?, OsStr::new(RESULTS_DIR), RESULTS_DIR)?;
                let shown = format!("{RESULTS_DIR}/{id}");
                let dest = ensure_dir(&root, OsStr::new(id), &shown)?;
                copy_dir(
                    &open_root(results)?,
                    &dest,
                    Path::new(&shown),
                    Links::Refuse,
                )
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Links {
    Keep,
    Refuse,
}

const DIR_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

const MAX_LINK_HOPS: u32 = 40;

const TMP_ATTEMPTS: u32 = 64;

fn open_root(path: &Path) -> Result<OwnedFd> {
    rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .with_context(|| format!("opening {}", path.display()))
}

fn is_symlink(dir: &OwnedFd, name: &OsStr) -> bool {
    rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)
        .is_ok_and(|st| FileType::from_raw_mode(st.st_mode) == FileType::Symlink)
}

fn open_dir(parent: &OwnedFd, name: &OsStr, shown: &str) -> Result<OwnedFd> {
    match rustix::fs::openat(parent, name, DIR_FLAGS, Mode::empty()) {
        Ok(fd) => Ok(fd),
        Err(_) if is_symlink(parent, name) => bail!("symlinks are refused: {shown} is a link"),
        Err(Errno::NOTDIR) => bail!("{shown} is not a directory"),
        Err(e) => Err(e).with_context(|| format!("opening {shown}")),
    }
}

fn ensure_dir(parent: &OwnedFd, name: &OsStr, shown: &str) -> Result<OwnedFd> {
    match rustix::fs::mkdirat(parent, name, Mode::from_raw_mode(0o755)) {
        Ok(()) | Err(Errno::EXIST) => open_dir(parent, name, shown),
        Err(e) => Err(e).with_context(|| format!("creating {shown}")),
    }
}

fn open_beneath(root: &Path, rel: &RelPath) -> Result<OwnedFd> {
    let mut dir = open_root(root)?;
    for part in rel.as_str().split('/') {
        dir = open_dir(&dir, OsStr::new(part), rel.as_str())?;
    }
    Ok(dir)
}

fn is_excluded(name: &OsStr) -> bool {
    name.to_str().is_some_and(|n| EXCLUDED.contains(&n))
}

fn entries(dir: &OwnedFd, shown: &Path) -> Result<Vec<OsString>> {
    let mut names = Vec::new();
    for entry in Dir::read_from(dir).with_context(|| format!("reading {}", shown.display()))? {
        let entry = entry.with_context(|| format!("reading {}", shown.display()))?;
        let name = OsStr::from_bytes(entry.file_name().to_bytes());
        if name != "." && name != ".." {
            names.push(name.to_os_string());
        }
    }
    Ok(names)
}

fn copy_dir(src: &OwnedFd, dest: &OwnedFd, shown: &Path, links: Links) -> Result<()> {
    for name in entries(src, shown)? {
        if is_excluded(&name) {
            continue;
        }
        let rel = shown.join(&name);
        let rel_s = rel.display().to_string();
        let st = rustix::fs::statat(src, &name, AtFlags::SYMLINK_NOFOLLOW)
            .with_context(|| format!("inspecting {rel_s}"))?;
        match FileType::from_raw_mode(st.st_mode) {
            FileType::Directory => {
                let from = open_dir(src, &name, &rel_s)?;
                let to = ensure_dir(dest, &name, &rel_s)?;
                copy_dir(&from, &to, &rel, links)?;
            }
            FileType::RegularFile => copy_file(src, dest, &name, &rel_s)?,
            FileType::Symlink if links == Links::Keep => {
                let target = rustix::fs::readlinkat(src, &name, Vec::new())
                    .with_context(|| format!("reading link {rel_s}"))?;
                rustix::fs::symlinkat(target.as_c_str(), dest, &name)
                    .with_context(|| format!("copying link {rel_s}"))?;
            }
            FileType::Symlink => bail!("symlinks are refused: {rel_s}"),
            _ => bail!("special files are refused: {rel_s}"),
        }
    }
    Ok(())
}

fn copy_file(src: &OwnedFd, dest: &OwnedFd, name: &OsStr, shown: &str) -> Result<()> {
    let from = rustix::fs::openat(
        src,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .with_context(|| format!("opening {shown}"))?;
    let st = rustix::fs::fstat(&from).with_context(|| format!("inspecting {shown}"))?;
    if FileType::from_raw_mode(st.st_mode) != FileType::RegularFile {
        bail!("{shown} changed type while being copied");
    }
    let (tmp, to) = create_tmp(dest, shown)?;
    let written = (|| -> Result<()> {
        rustix::fs::fchmod(&to, Mode::from_raw_mode(st.st_mode & 0o7777))
            .with_context(|| format!("setting mode on {shown}"))?;
        std::io::copy(&mut File::from(from), &mut File::from(to))
            .with_context(|| format!("copying {shown}"))?;
        if is_symlink(dest, name) {
            bail!("symlinks are refused: {shown}");
        }
        rustix::fs::renameat(dest, &tmp, dest, name)
            .with_context(|| format!("moving {shown} into place"))
    })();
    if written.is_err() {
        let _ = rustix::fs::unlinkat(dest, &tmp, AtFlags::empty());
    }
    written
}

fn create_tmp(dir: &OwnedFd, shown: &str) -> Result<(OsString, OwnedFd)> {
    for n in 0..TMP_ATTEMPTS {
        let name = OsString::from(format!(".buildit-tmp-{n}"));
        match rustix::fs::openat(
            dir,
            &name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        ) {
            Ok(fd) => return Ok((name, fd)),
            Err(Errno::EXIST) => continue,
            Err(e) => return Err(e).with_context(|| format!("creating a temp file for {shown}")),
        }
    }
    bail!("no free temp name for {shown}")
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
            if !link_stays_inside(root, &path)? {
                tracing::info!("skipping symlink {rel}: it points outside the build context");
                std::fs::remove_file(&path).with_context(|| format!("removing link {rel}"))?;
            }
        } else if ft.is_dir() {
            sanitize_in(root, &path)?;
        } else if !ft.is_file() {
            bail!("special files are refused in the build context: {rel}");
        }
    }
    Ok(())
}

enum Step {
    Up,
    Name(OsString),
}

fn push_target(todo: &mut Vec<Step>, target: &Path) -> bool {
    let mut steps = Vec::new();
    for comp in target.components() {
        match comp {
            Component::Normal(name) => steps.push(Step::Name(name.to_os_string())),
            Component::ParentDir => steps.push(Step::Up),
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) => return false,
        }
    }
    todo.extend(steps.into_iter().rev());
    true
}

fn link_stays_inside(root: &Path, link: &Path) -> Result<bool> {
    let mut at: Vec<OsString> = link
        .parent()
        .and_then(|p| p.strip_prefix(root).ok())
        .map(|p| p.iter().map(OsStr::to_os_string).collect())
        .unwrap_or_default();
    let mut todo = Vec::new();
    let target = std::fs::read_link(link).with_context(|| format!("reading {}", link.display()))?;
    if !push_target(&mut todo, &target) {
        return Ok(false);
    }
    let mut hops = 0;
    while let Some(step) = todo.pop() {
        match step {
            Step::Up => {
                if at.pop().is_none() {
                    return Ok(false);
                }
            }
            Step::Name(name) => {
                let here: PathBuf = root
                    .iter()
                    .chain(at.iter().map(OsString::as_os_str))
                    .chain([name.as_os_str()])
                    .collect();
                match std::fs::symlink_metadata(&here) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        hops += 1;
                        let next = std::fs::read_link(&here)
                            .with_context(|| format!("reading {}", here.display()))?;
                        if hops > MAX_LINK_HOPS || !push_target(&mut todo, &next) {
                            return Ok(false);
                        }
                    }
                    _ => at.push(name),
                }
            }
        }
    }
    Ok(true)
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::Path;

    use crate::sandbox::{
        EXCLUDED, RelPath, SandboxName, Workspace, download_argv, link_stays_inside, sanitize,
        tree_hash_argv, upload_argv, verified_sync,
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

    fn link(target: &str, at: &Path) {
        fs::create_dir_all(at.parent().unwrap()).unwrap();
        symlink(target, at).unwrap();
    }

    fn links(root: &Path) -> BTreeMap<String, String> {
        fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
            for e in fs::read_dir(dir).unwrap() {
                let e = e.unwrap();
                let ft = e.file_type().unwrap();
                let p = e.path();
                if ft.is_symlink() {
                    let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
                    let target = fs::read_link(&p).unwrap().to_string_lossy().into_owned();
                    out.insert(rel, target);
                } else if ft.is_dir() {
                    walk(root, &p, out);
                }
            }
        }
        let mut out = BTreeMap::new();
        walk(root, root, &mut out);
        out
    }

    #[test]
    fn sanitize_keeps_links_inside_the_context_and_drops_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("ctx");
        touch(&root.join("AGENTS.md"));
        touch(&root.join("sub/file"));
        link("AGENTS.md", &root.join("CLAUDE.md"));
        link(
            "../AGENTS.md",
            &root.join(".github/copilot-instructions.md"),
        );
        link("sub", &root.join("docs"));
        link("./sub/../sub/./file", &root.join("dotted"));
        link("missing", &root.join("dangling"));
        link("docs/file", &root.join("via-dir-link"));
        link("/etc/passwd", &root.join("absolute"));
        link("../outside", &root.join("parent"));
        link("../../../etc/passwd", &root.join("sub/deep"));
        link("../..", &root.join("a/b/top"));
        link("../../..", &root.join("a/b/above"));
        sanitize(&root).unwrap();
        assert_eq!(
            links(&root),
            BTreeMap::from(
                [
                    ("CLAUDE.md", "AGENTS.md"),
                    (".github/copilot-instructions.md", "../AGENTS.md"),
                    ("docs", "sub"),
                    ("dotted", "./sub/../sub/./file"),
                    ("dangling", "missing"),
                    ("via-dir-link", "docs/file"),
                    ("a/b/top", "../.."),
                ]
                .map(|(k, v)| (k.to_string(), v.to_string()))
            )
        );
        assert!(tree(&root).contains("sub/file"));
    }

    #[test]
    fn link_resolution_follows_nested_links() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("ctx");
        touch(&root.join("a/b/x"));
        link("../../..", &root.join("a/b/up"));
        link("a/b/up/../x", &root.join("lexically-inside"));
        link("a/b/../b/x", &root.join("inside"));
        link("loop2", &root.join("loop1"));
        link("loop1", &root.join("loop2"));
        link("/etc/passwd", &root.join("absolute"));
        link("absolute", &root.join("via-absolute"));
        link("a/b/up", &root.join("via-up"));
        for (name, inside) in [
            ("lexically-inside", false),
            ("inside", true),
            ("loop1", false),
            ("loop2", false),
            ("via-absolute", false),
            ("via-up", false),
        ] {
            assert_eq!(
                link_stays_inside(&root, &root.join(name)).unwrap(),
                inside,
                "{name}"
            );
        }
    }

    #[test]
    fn sanitize_refuses_special_files() {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("pipe");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(made.success());
        let err = sanitize(dir.path()).unwrap_err().to_string();
        assert!(err.contains("special files are refused"), "{err}");
    }

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

    #[test]
    fn local_fetch_refuses_a_symlinked_context() {
        let work = tempfile::tempdir().unwrap();
        let w = work.path();
        let outside = tempfile::tempdir().unwrap();
        touch(&outside.path().join("Dockerfile"));
        symlink(outside.path(), w.join("escape")).unwrap();
        touch(&outside.path().join("svc/Dockerfile"));
        symlink(outside.path(), w.join("hop")).unwrap();
        touch(&w.join("file"));
        let ws = Workspace::Local {
            dir: w.to_path_buf(),
        };
        let out = tempfile::tempdir().unwrap();
        for (ctx, want) in [
            ("escape", "symlinks are refused"),
            ("hop/svc", "symlinks are refused"),
            ("file", "not a directory"),
        ] {
            let err = ws
                .fetch_context(None, &RelPath::parse(ctx).unwrap(), &out.path().join("a"))
                .unwrap_err()
                .to_string();
            assert!(err.contains(want), "{ctx}: {err}");
        }
    }

    #[test]
    fn local_fetch_ships_inner_links_and_skips_outer_ones() {
        let work = tempfile::tempdir().unwrap();
        let w = work.path();
        let outside = tempfile::tempdir().unwrap();
        touch(&outside.path().join("secret"));
        touch(&w.join("svc/Dockerfile"));
        touch(&w.join("svc/AGENTS.md"));
        touch(&w.join("svc/tools/run.sh"));
        fs::set_permissions(
            w.join("svc/tools/run.sh"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        touch(&w.join("other/secret"));
        link("AGENTS.md", &w.join("svc/CLAUDE.md"));
        link(
            "../AGENTS.md",
            &w.join("svc/.github/copilot-instructions.md"),
        );
        link(
            outside.path().join("secret").to_str().unwrap(),
            &w.join("svc/abs"),
        );
        link("../other/secret", &w.join("svc/sibling"));
        link(outside.path().to_str().unwrap(), &w.join("svc/outdir"));
        let ws = Workspace::Local {
            dir: w.to_path_buf(),
        };
        let out = tempfile::tempdir().unwrap();
        let dest = out.path().join("ctx");
        ws.fetch_context(None, &RelPath::parse("svc").unwrap(), &dest)
            .unwrap();
        assert_eq!(
            links(&dest),
            BTreeMap::from([
                ("CLAUDE.md".to_string(), "AGENTS.md".to_string()),
                (
                    ".github/copilot-instructions.md".to_string(),
                    "../AGENTS.md".to_string()
                ),
            ])
        );
        assert_eq!(
            fs::metadata(dest.join("tools/run.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );

        let bytes = crate::context::tar_bytes(&dest).unwrap();
        let mut archive = tar::Archive::new(&bytes[..]);
        let mut kinds = BTreeMap::new();
        for e in archive.entries().unwrap() {
            let e = e.unwrap();
            let path = e.path().unwrap().to_string_lossy().into_owned();
            let target = e
                .link_name()
                .unwrap()
                .map(|t| t.to_string_lossy().into_owned());
            kinds.insert(path, (e.header().entry_type(), target));
        }
        assert_eq!(
            kinds["CLAUDE.md"],
            (tar::EntryType::Symlink, Some("AGENTS.md".to_string()))
        );
        assert_eq!(
            kinds[".github/copilot-instructions.md"],
            (tar::EntryType::Symlink, Some("../AGENTS.md".to_string()))
        );
        assert!(
            !kinds
                .keys()
                .any(|k| k.contains("secret") || k.starts_with("abs") || k.starts_with("outdir")),
            "{kinds:?}"
        );
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

    #[test]
    fn local_publish_never_writes_through_symlinks() {
        let elsewhere = tempfile::tempdir().unwrap();
        let stage = tempfile::tempdir().unwrap();
        let results = stage.path().join("buildit-0001");
        touch(&results.join("build.log"));
        touch(&results.join("out/bin/app"));

        for at in [
            ".buildit",
            ".buildit/buildit-0001",
            ".buildit/buildit-0001/out",
            ".buildit/buildit-0001/out/bin",
        ] {
            let work = tempfile::tempdir().unwrap();
            link(elsewhere.path().to_str().unwrap(), &work.path().join(at));
            let ws = Workspace::Local {
                dir: work.path().to_path_buf(),
            };
            let err = ws.publish(None, &results).unwrap_err().to_string();
            assert!(err.contains("symlinks are refused"), "{at}: {err}");
            assert_eq!(
                fs::read_dir(elsewhere.path()).unwrap().count(),
                0,
                "{at}: wrote outside the results dir"
            );
        }
    }

    #[test]
    fn local_publish_replaces_planted_file_links_without_touching_targets() {
        let elsewhere = tempfile::tempdir().unwrap();
        let victim = elsewhere.path().join("victim");
        fs::write(&victim, b"original").unwrap();
        let stage = tempfile::tempdir().unwrap();
        let results = stage.path().join("buildit-0001");
        fs::create_dir_all(&results).unwrap();
        fs::write(results.join("build.log"), b"log").unwrap();
        fs::write(results.join("run.log"), b"log").unwrap();

        let work = tempfile::tempdir().unwrap();
        let dest = work.path().join(".buildit/buildit-0001");
        fs::create_dir_all(&dest).unwrap();
        symlink(&victim, dest.join("build.log")).unwrap();
        let ws = Workspace::Local {
            dir: work.path().to_path_buf(),
        };
        let err = ws.publish(None, &results).unwrap_err().to_string();
        assert!(err.contains("symlinks are refused"), "{err}");
        assert_eq!(fs::read(&victim).unwrap(), b"original");

        fs::remove_file(dest.join("build.log")).unwrap();
        fs::hard_link(&victim, dest.join("build.log")).unwrap();
        ws.publish(None, &results).unwrap();
        assert_eq!(fs::read(&victim).unwrap(), b"original");
        assert_eq!(fs::read(dest.join("build.log")).unwrap(), b"log");
        assert_eq!(
            tree(&dest),
            BTreeSet::from(["build.log".to_string(), "run.log".to_string()])
        );
    }

    #[test]
    fn local_publish_refuses_links_in_the_results() {
        let stage = tempfile::tempdir().unwrap();
        let results = stage.path().join("buildit-0001");
        touch(&results.join("build.log"));
        symlink("/etc/passwd", results.join("passwd")).unwrap();
        let work = tempfile::tempdir().unwrap();
        let ws = Workspace::Local {
            dir: work.path().to_path_buf(),
        };
        let err = ws.publish(None, &results).unwrap_err().to_string();
        assert!(err.contains("symlinks are refused"), "{err}");
    }
}
