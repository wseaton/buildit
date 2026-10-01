use std::fmt;

use anyhow::{Context, Result, anyhow, bail};
use regex::bytes::{Regex, RegexBuilder};
use serde::Serialize;

pub const LOG_DIR: &str = "/buildit/logs";
pub const READ_LIMIT: u64 = 32 * 1024 * 1024;

const MAX_LINE_BYTES: usize = 1024;
const DEFAULT_MAX_BYTES: u64 = 16 * 1024;
const MIN_MAX_BYTES: u64 = 2 * 1024;
const MAX_MAX_BYTES: u64 = 64 * 1024;
const MAX_CONTEXT_LINES: u64 = 20;
const MAX_PATTERN_LEN: usize = 512;
const REGEX_SIZE_LIMIT: usize = 1 << 20;
const SUMMARY_TAIL_LINES: u64 = 40;
const SUMMARY_TAIL_BYTES: u64 = 6 * 1024;
const SUMMARY_ERROR_LINES: u64 = 12;
const SUMMARY_ERROR_BYTES: u64 = 4 * 1024;
// lines starting with `$ ` are the command echo, not output
const ERROR_PATTERN: &str = r"(?i)^(?:[^$].*)?(?:error|fatal|undefined|cannot|failed)";

const READ_SCRIPT: &str = r#"f="$1"; cap="$2"
[ -f "$f" ] || { echo "no such log" >&2; exit 3; }
size=$(wc -c < "$f")
skip=0; lines=0
if [ "$size" -gt "$cap" ]; then skip=$((size - cap)); lines=$(head -c "$skip" "$f" | wc -l); fi
echo "$skip $lines"
tail -c "+$((skip + 1))" "$f" | head -c "$cap""#;

const LIST_SCRIPT: &str = r#"cd "$1" && ls -1"#;

const ALLOC_SCRIPT: &str = r#"d="$1"; n=1; set -C
until (: > "$d/run-$n.log") 2>/dev/null; do
  n=$((n + 1)); [ "$n" -le 100000 ] || { echo "no free run number" >&2; exit 1; }
done
echo "$n""#;

const STEP_SCRIPT: &str = r#"log="$1"; secs="$2"; shift 2; printf '$ %s\n' "$*" >> "$log"; exec timeout "$secs" "$@" >> "$log" 2>&1"#;

const NOTE_SCRIPT: &str = r#"printf '%s\n' "$2" >> "$1""#;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogName {
    Build,
    Run(u32),
}

impl LogName {
    pub fn file_name(&self) -> String {
        match self {
            LogName::Build => "build.log".to_string(),
            LogName::Run(n) => format!("run-{n}.log"),
        }
    }

    pub fn path(&self) -> String {
        format!("{LOG_DIR}/{}", self.file_name())
    }

    fn from_file_name(name: &str) -> Option<Self> {
        if name == "build.log" {
            return Some(LogName::Build);
        }
        let n = name.strip_prefix("run-")?.strip_suffix(".log")?;
        if n.starts_with('0') || !n.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        n.parse().ok().map(LogName::Run)
    }
}

impl fmt::Display for LogName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogName::Build => f.write_str("build"),
            LogName::Run(n) => write!(f, "run:{n}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSelector {
    Name(LogName),
    Latest,
}

impl LogSelector {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "latest" => Ok(LogSelector::Latest),
            "build" => Ok(LogSelector::Name(LogName::Build)),
            other => other
                .strip_prefix("run:")
                .and_then(|n| n.parse::<u32>().ok())
                .filter(|n| *n > 0)
                .map(|n| LogSelector::Name(LogName::Run(n)))
                .ok_or_else(|| anyhow!("which must be build, run:<n> or latest, got {raw:?}")),
        }
    }

    pub fn resolve(self, available: &[LogName]) -> Result<LogName> {
        let found = match self {
            LogSelector::Latest => available.iter().max().copied(),
            LogSelector::Name(name) => available.contains(&name).then_some(name),
        };
        found.ok_or_else(|| {
            let have: Vec<String> = available.iter().map(ToString::to_string).collect();
            match self {
                LogSelector::Latest => anyhow!("no logs yet"),
                LogSelector::Name(n) => anyhow!("no {n} log; have [{}]", have.join(", ")),
            }
        })
    }
}

fn argv(shell: &str, script: &str, args: &[&str]) -> Vec<String> {
    let mut out = vec![
        shell.to_string(),
        "-c".to_string(),
        script.to_string(),
        "sh".to_string(),
    ];
    out.extend(args.iter().map(|a| (*a).to_string()));
    out
}

pub fn read_argv(shell: &str, log: LogName) -> Vec<String> {
    argv(shell, READ_SCRIPT, &[&log.path(), &READ_LIMIT.to_string()])
}

pub fn list_argv(shell: &str) -> Vec<String> {
    argv(shell, LIST_SCRIPT, &[LOG_DIR])
}

pub fn alloc_run_argv(shell: &str) -> Vec<String> {
    argv(shell, ALLOC_SCRIPT, &[LOG_DIR])
}

pub fn note_argv(shell: &str, log: LogName, line: &str) -> Vec<String> {
    argv(shell, NOTE_SCRIPT, &[&log.path(), line])
}

// appends `$ argv` then the command's stdout and stderr to the log
pub fn step_argv(shell: &str, log: LogName, secs: u64, step: &[String]) -> Vec<String> {
    let mut out = argv(shell, STEP_SCRIPT, &[&log.path(), &secs.max(1).to_string()]);
    out.extend(step.iter().cloned());
    out
}

pub fn parse_list(out: &str) -> Vec<LogName> {
    let mut names: Vec<LogName> = out.lines().filter_map(LogName::from_file_name).collect();
    names.sort();
    names
}

pub fn parse_run_number(out: &str) -> Result<u32> {
    out.trim()
        .parse()
        .with_context(|| format!("bad run number {out:?}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogText {
    first_line: u64,
    dropped_bytes: u64,
    body: Vec<u8>,
}

impl LogText {
    pub fn whole(body: Vec<u8>) -> Self {
        Self {
            first_line: 1,
            dropped_bytes: 0,
            body,
        }
    }

    // output of READ_SCRIPT: "<skipped bytes> <skipped lines>\n" then the bytes
    pub fn parse(out: Vec<u8>) -> Result<Self> {
        let nl = out
            .iter()
            .position(|b| *b == b'\n')
            .ok_or_else(|| anyhow!("log read returned no header"))?;
        let header = std::str::from_utf8(&out[..nl]).context("log read header")?;
        let mut parts = header.split_whitespace();
        let mut num = || -> Result<u64> {
            parts
                .next()
                .ok_or_else(|| anyhow!("short log read header {header:?}"))?
                .parse()
                .with_context(|| format!("bad log read header {header:?}"))
        };
        let dropped_bytes = num()?;
        let skipped_lines = num()?;
        Ok(Self {
            first_line: skipped_lines + 1,
            dropped_bytes,
            body: out[nl + 1..].to_vec(),
        })
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    fn lines(&self) -> Vec<(u64, &[u8])> {
        let body = self.body.strip_suffix(b"\n").unwrap_or(&self.body);
        if self.body.is_empty() {
            return Vec::new();
        }
        body.split(|b| *b == b'\n')
            .zip(self.first_line..)
            .map(|(line, n)| (n, line.strip_suffix(b"\r").unwrap_or(line)))
            .collect()
    }

    pub fn total_lines(&self) -> u64 {
        self.first_line - 1 + self.lines().len() as u64
    }
}

#[derive(Debug)]
pub struct LogQuery {
    from_line: Option<u64>,
    to_line: Option<u64>,
    head_lines: Option<u64>,
    tail_lines: Option<u64>,
    grep: Option<Regex>,
    context_lines: u64,
    max_bytes: u64,
}

pub struct QueryParams<'a> {
    pub from_line: Option<u64>,
    pub to_line: Option<u64>,
    pub head_lines: Option<u64>,
    pub tail_lines: Option<u64>,
    pub grep: Option<&'a str>,
    pub context_lines: Option<u64>,
    pub max_bytes: Option<u64>,
}

fn pattern(raw: &str) -> Result<Regex> {
    if raw.len() > MAX_PATTERN_LEN {
        bail!("grep pattern is longer than {MAX_PATTERN_LEN} bytes");
    }
    RegexBuilder::new(raw)
        .size_limit(REGEX_SIZE_LIMIT)
        .build()
        .with_context(|| format!("invalid grep pattern {raw:?}"))
}

impl LogQuery {
    pub fn new(p: QueryParams<'_>) -> Result<Self> {
        if p.head_lines.is_some() && p.tail_lines.is_some() {
            bail!("pass head_lines or tail_lines, not both");
        }
        if let (Some(from), Some(to)) = (p.from_line, p.to_line)
            && from > to
        {
            bail!("from_line {from} is past to_line {to}");
        }
        Ok(Self {
            from_line: p.from_line,
            to_line: p.to_line,
            head_lines: p.head_lines,
            tail_lines: p.tail_lines,
            grep: p.grep.map(pattern).transpose()?,
            context_lines: p.context_lines.unwrap_or(0).min(MAX_CONTEXT_LINES),
            max_bytes: p
                .max_bytes
                .unwrap_or(DEFAULT_MAX_BYTES)
                .clamp(MIN_MAX_BYTES, MAX_MAX_BYTES),
        })
    }

    fn keeps_end(&self) -> bool {
        self.tail_lines.is_some() || (self.head_lines.is_none() && self.from_line.is_none())
    }

    pub fn run(&self, text: &LogText, log: LogName, available: &[LogName]) -> LogPage {
        let lines = text.lines();
        let lo = self.from_line.unwrap_or(1);
        let hi = self.to_line.unwrap_or(u64::MAX);
        let in_range: Vec<(u64, &[u8])> = lines
            .into_iter()
            .filter(|(n, _)| (lo..=hi).contains(n))
            .collect();

        let (mut entries, matched) = match &self.grep {
            None => (
                in_range
                    .iter()
                    .map(|&(n, line)| Entry {
                        n,
                        line,
                        hit: true,
                        gap: false,
                    })
                    .collect::<Vec<_>>(),
                None,
            ),
            Some(re) => {
                let hits: Vec<bool> = in_range.iter().map(|(_, l)| re.is_match(l)).collect();
                let ctx = usize::try_from(self.context_lines).unwrap_or(0);
                let mut keep = vec![false; in_range.len()];
                for (i, _) in hits.iter().enumerate().filter(|(_, h)| **h) {
                    let end = (i + ctx + 1).min(keep.len());
                    keep[i.saturating_sub(ctx)..end].fill(true);
                }
                let mut entries = Vec::new();
                let mut prev: Option<usize> = None;
                for (i, &(n, line)) in in_range.iter().enumerate() {
                    if !keep[i] {
                        continue;
                    }
                    entries.push(Entry {
                        n,
                        line,
                        hit: hits[i],
                        gap: prev.is_some_and(|p| p + 1 != i),
                    });
                    prev = Some(i);
                }
                (entries, Some(hits.iter().filter(|h| **h).count() as u64))
            }
        };

        if let Some(h) = self.head_lines {
            entries.truncate(usize::try_from(h).unwrap_or(usize::MAX));
        } else if let Some(t) = self.tail_lines {
            let t = usize::try_from(t).unwrap_or(usize::MAX);
            entries.drain(..entries.len().saturating_sub(t));
        }

        let rendered: Vec<(bool, String, bool)> = entries
            .iter()
            .map(|e| {
                let (text, clipped) = e.render();
                (e.gap, text, clipped)
            })
            .collect();
        let budget = usize::try_from(self.max_bytes).unwrap_or(usize::MAX);
        let (start, end) = fit(&rendered, budget, self.keeps_end());
        let kept = &rendered[start..end];
        let mut out = String::new();
        for (i, (gap, line, _)) in kept.iter().enumerate() {
            if i > 0 && *gap {
                out.push_str("--\n");
            }
            out.push_str(line);
            out.push('\n');
        }
        let truncated = end - start < rendered.len() || kept.iter().any(|(_, _, c)| *c);
        LogPage {
            log: log.to_string(),
            logs: available.iter().map(ToString::to_string).collect(),
            total_lines: text.total_lines(),
            matched,
            shown: (start < end).then(|| [entries[start].n, entries[end - 1].n]),
            truncated,
            unread_head_bytes: (text.dropped_bytes > 0).then_some(text.dropped_bytes),
            lines: out,
        }
    }
}

struct Entry<'a> {
    n: u64,
    line: &'a [u8],
    hit: bool,
    gap: bool,
}

impl Entry<'_> {
    // grep -n style: `12:text` for a hit, `13-text` for context
    fn render(&self) -> (String, bool) {
        let sep = if self.hit { ':' } else { '-' };
        if self.line.len() <= MAX_LINE_BYTES {
            return (
                format!("{}{sep}{}", self.n, String::from_utf8_lossy(self.line)),
                false,
            );
        }
        let cut = &self.line[..MAX_LINE_BYTES];
        (
            format!(
                "{}{sep}{}…[+{} bytes]",
                self.n,
                String::from_utf8_lossy(cut),
                self.line.len() - MAX_LINE_BYTES
            ),
            true,
        )
    }
}

// the widest window of whole lines within budget, anchored at one end
fn fit(rendered: &[(bool, String, bool)], budget: usize, keep_end: bool) -> (usize, usize) {
    let cost = |(gap, line, _): &(bool, String, bool)| line.len() + 1 + if *gap { 3 } else { 0 };
    let mut used = 0;
    let mut count = 0;
    let order: Box<dyn Iterator<Item = &(bool, String, bool)>> = if keep_end {
        Box::new(rendered.iter().rev())
    } else {
        Box::new(rendered.iter())
    };
    for entry in order {
        let c = cost(entry);
        if count > 0 && used + c > budget {
            break;
        }
        used += c;
        count += 1;
    }
    if keep_end {
        (rendered.len() - count, rendered.len())
    } else {
        (0, count)
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct LogPage {
    pub log: String,
    pub logs: Vec<String>,
    pub total_lines: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shown: Option<[u64; 2]>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unread_head_bytes: Option<u64>,
    pub lines: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct LogSummary {
    pub log: String,
    pub path: String,
    pub total_lines: u64,
    pub tail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<String>,
}

pub fn summarize(text: &LogText, log: LogName, path: String, failed: bool) -> Result<LogSummary> {
    let tail = LogQuery::new(QueryParams {
        from_line: None,
        to_line: None,
        head_lines: None,
        tail_lines: Some(SUMMARY_TAIL_LINES),
        grep: None,
        context_lines: None,
        max_bytes: Some(SUMMARY_TAIL_BYTES),
    })?
    .run(text, log, &[]);
    let errors = if failed {
        let page = LogQuery::new(QueryParams {
            from_line: None,
            to_line: None,
            head_lines: Some(SUMMARY_ERROR_LINES),
            tail_lines: None,
            grep: Some(ERROR_PATTERN),
            context_lines: None,
            max_bytes: Some(SUMMARY_ERROR_BYTES),
        })?
        .run(text, log, &[]);
        (!page.lines.is_empty()).then_some(page.lines)
    } else {
        None
    };
    Ok(LogSummary {
        log: log.to_string(),
        path,
        total_lines: text.total_lines(),
        tail: tail.lines,
        errors,
    })
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use crate::buildlog::{
        LIST_SCRIPT, LogName, LogQuery, LogSelector, LogText, QueryParams, alloc_run_argv,
        list_argv, note_argv, parse_list, parse_run_number, read_argv, step_argv, summarize,
    };

    fn text(n: u64) -> LogText {
        LogText::whole(
            (1..=n)
                .map(|i| format!("line {i}\n"))
                .collect::<String>()
                .into_bytes(),
        )
    }

    fn q() -> QueryParams<'static> {
        QueryParams {
            from_line: None,
            to_line: None,
            head_lines: None,
            tail_lines: None,
            grep: None,
            context_lines: None,
            max_bytes: None,
        }
    }

    fn run(t: &LogText, p: QueryParams<'_>) -> crate::buildlog::LogPage {
        LogQuery::new(p)
            .unwrap()
            .run(t, LogName::Build, &[LogName::Build])
    }

    #[test]
    fn log_names_round_trip() {
        assert_eq!(LogName::Build.file_name(), "build.log");
        assert_eq!(LogName::Run(3).file_name(), "run-3.log");
        assert_eq!(LogName::Run(3).path(), "/buildit/logs/run-3.log");
        assert_eq!(LogName::Run(3).to_string(), "run:3");
        assert_eq!(
            parse_list(
                "run-10.log\nbuild.log\nrun-2.log\nrun-02.log\nrun-x.log\nrun-.log\nother\n"
            ),
            [LogName::Build, LogName::Run(2), LogName::Run(10)]
        );
        assert_eq!(parse_run_number(" 7\n").unwrap(), 7);
        assert!(parse_run_number("").is_err());
    }

    #[test]
    fn selectors_parse_and_resolve() {
        let have = [LogName::Build, LogName::Run(1), LogName::Run(12)];
        assert_eq!(
            LogSelector::parse("latest")
                .unwrap()
                .resolve(&have)
                .unwrap(),
            LogName::Run(12)
        );
        assert_eq!(
            LogSelector::parse("latest")
                .unwrap()
                .resolve(&[LogName::Build])
                .unwrap(),
            LogName::Build
        );
        assert_eq!(
            LogSelector::parse("run:1").unwrap().resolve(&have).unwrap(),
            LogName::Run(1)
        );
        assert_eq!(
            LogSelector::parse(" build ").unwrap(),
            LogSelector::Name(LogName::Build)
        );
        let err = LogSelector::parse("run:2")
            .unwrap()
            .resolve(&have)
            .unwrap_err()
            .to_string();
        assert!(err.contains("run:2") && err.contains("run:12"), "{err}");
        assert!(LogSelector::parse("latest").unwrap().resolve(&[]).is_err());
        for bad in [
            "", "run:", "run:0", "run:-1", "run:x", "run-1", "../build", "Build",
        ] {
            assert!(LogSelector::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn default_query_keeps_the_end_within_max_bytes() {
        let t = text(5000);
        let page = run(&t, q());
        assert_eq!(page.total_lines, 5000);
        assert!(page.truncated);
        assert!(page.lines.len() <= 16 * 1024, "{}", page.lines.len());
        assert!(page.lines.ends_with("5000:line 5000\n"), "{}", page.lines);
        let [first, last] = page.shown.unwrap();
        assert_eq!(last, 5000);
        assert!(page.lines.starts_with(&format!("{first}:line {first}\n")));
        assert_eq!(page.matched, None);

        let small = run(&text(3), q());
        assert!(!small.truncated);
        assert_eq!(small.lines, "1:line 1\n2:line 2\n3:line 3\n");
        assert_eq!(small.shown, Some([1, 3]));
    }

    #[test]
    fn paging_by_range_head_and_tail() {
        let t = text(100);
        let page = run(
            &t,
            QueryParams {
                from_line: Some(10),
                to_line: Some(12),
                ..q()
            },
        );
        assert_eq!(page.lines, "10:line 10\n11:line 11\n12:line 12\n");
        assert!(!page.truncated);

        let page = run(
            &t,
            QueryParams {
                head_lines: Some(2),
                ..q()
            },
        );
        assert_eq!(page.lines, "1:line 1\n2:line 2\n");
        let page = run(
            &t,
            QueryParams {
                tail_lines: Some(2),
                ..q()
            },
        );
        assert_eq!(page.lines, "99:line 99\n100:line 100\n");
        let page = run(
            &t,
            QueryParams {
                from_line: Some(50),
                tail_lines: Some(1),
                to_line: Some(60),
                ..q()
            },
        );
        assert_eq!(page.lines, "60:line 60\n");

        let page = run(
            &t,
            QueryParams {
                from_line: Some(500),
                ..q()
            },
        );
        assert_eq!(page.lines, "");
        assert_eq!(page.shown, None);

        // from_line pages forward: the cut keeps the start
        let t = text(5000);
        let page = run(
            &t,
            QueryParams {
                from_line: Some(1000),
                max_bytes: Some(2048),
                ..q()
            },
        );
        assert!(page.lines.starts_with("1000:line 1000\n"), "{}", page.lines);
        assert!(page.truncated);
        assert!(page.lines.len() <= 2048);

        assert!(
            LogQuery::new(QueryParams {
                head_lines: Some(1),
                tail_lines: Some(1),
                ..q()
            })
            .is_err()
        );
        assert!(
            LogQuery::new(QueryParams {
                from_line: Some(5),
                to_line: Some(4),
                ..q()
            })
            .is_err()
        );
    }

    #[test]
    fn grep_marks_hits_context_and_gaps() {
        let t = LogText::whole(b"a\nb\nerror: one\nc\nd\ne\nf\nERROR two\ng\n".to_vec());
        let page = run(
            &t,
            QueryParams {
                grep: Some("(?i)error"),
                context_lines: Some(1),
                ..q()
            },
        );
        assert_eq!(page.matched, Some(2));
        assert_eq!(
            page.lines,
            "2-b\n3:error: one\n4-c\n--\n7-f\n8:ERROR two\n9-g\n"
        );
        let page = run(
            &t,
            QueryParams {
                grep: Some("error"),
                ..q()
            },
        );
        assert_eq!(page.matched, Some(1));
        assert_eq!(page.lines, "3:error: one\n");
        let page = run(
            &t,
            QueryParams {
                grep: Some("(?i)error"),
                context_lines: Some(100),
                head_lines: Some(3),
                ..q()
            },
        );
        assert_eq!(page.lines, "1-a\n2-b\n3:error: one\n");
        let page = run(
            &t,
            QueryParams {
                grep: Some("(?i)error"),
                from_line: Some(4),
                ..q()
            },
        );
        assert_eq!(page.lines, "8:ERROR two\n");
        assert!(
            LogQuery::new(QueryParams {
                grep: Some("("),
                ..q()
            })
            .is_err()
        );
        let long = "a".repeat(513);
        assert!(
            LogQuery::new(QueryParams {
                grep: Some(&long),
                ..q()
            })
            .is_err()
        );
    }

    #[test]
    fn caps_clip_long_lines_and_survive_bad_bytes() {
        let mut body = b"ok\r\n".to_vec();
        body.extend(std::iter::repeat_n(b'x', 5000));
        body.extend(b"\n\xff\xfe tail\n");
        let t = LogText::whole(body);
        let page = run(
            &t,
            QueryParams {
                max_bytes: Some(1),
                head_lines: Some(100),
                ..q()
            },
        );
        // max_bytes clamps up to 2 KiB; a 5000-byte line is clipped to 1 KiB
        assert!(page.truncated);
        assert!(
            page.lines.starts_with("1:ok\n2:xxx"),
            "{}",
            &page.lines[..20]
        );
        assert!(page.lines.contains("…[+3976 bytes]"), "{}", page.lines);
        assert!(
            page.lines.contains("3:\u{fffd}\u{fffd} tail"),
            "{}",
            page.lines
        );
        assert_eq!(page.total_lines, 3);

        let page = run(
            &t,
            QueryParams {
                grep: Some(r"(?-u)\xff"),
                ..q()
            },
        );
        assert_eq!(page.matched, Some(1));

        let huge = run(
            &text(100_000),
            QueryParams {
                max_bytes: Some(10_000_000),
                ..q()
            },
        );
        assert!(huge.lines.len() <= 64 * 1024);
        assert!(huge.truncated);

        let empty = run(&LogText::whole(Vec::new()), q());
        assert_eq!(empty.total_lines, 0);
        assert_eq!(empty.lines, "");
        assert!(!empty.truncated);
        let blank = LogText::whole(b"\n\n".to_vec());
        assert_eq!(blank.total_lines(), 2);
    }

    #[test]
    fn read_header_offsets_line_numbers() {
        let t = LogText::parse(b"120 7\npartial\nnext\n".to_vec()).unwrap();
        assert_eq!(t.total_lines(), 9);
        let page = LogQuery::new(q()).unwrap().run(&t, LogName::Run(2), &[]);
        assert_eq!(page.lines, "8:partial\n9:next\n");
        assert_eq!(page.unread_head_bytes, Some(120));
        assert_eq!(page.log, "run:2");
        let t = LogText::parse(b"0 0\n".to_vec()).unwrap();
        assert_eq!(t.total_lines(), 0);
        for bad in [&b""[..], b"0\nx", b"a b\nx", b"no newline"] {
            assert!(LogText::parse(bad.to_vec()).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn summary_reports_tail_and_errors_only_on_failure() {
        let mut log = String::from("$ buildah bud -Werror -f Dockerfile\n");
        for i in 0..100 {
            log.push_str(&format!("step {i}\n"));
        }
        log.push_str("main.c:3: undefined reference to `frob'\n");
        log.push_str("Error: building at STEP \"RUN make\": exit status 2\n");
        let t = LogText::whole(log.into_bytes());
        let s = summarize(&t, LogName::Build, ".buildit/b/build.log".to_string(), true).unwrap();
        assert_eq!(s.log, "build");
        assert_eq!(s.path, ".buildit/b/build.log");
        assert_eq!(s.total_lines, 103);
        assert_eq!(s.tail.lines().count(), 40);
        assert!(s.tail.ends_with("exit status 2\n"), "{}", s.tail);
        let errors = s.errors.unwrap();
        assert_eq!(
            errors,
            "102:main.c:3: undefined reference to `frob'\n\
             103:Error: building at STEP \"RUN make\": exit status 2\n"
        );
        let ok = summarize(&t, LogName::Run(1), String::new(), false).unwrap();
        assert_eq!(ok.errors, None);
        assert_eq!(ok.log, "run:1");
        let clean = summarize(&text(3), LogName::Build, String::new(), true).unwrap();
        assert_eq!(clean.errors, None);
    }

    fn sh(argv: &[String]) -> std::process::Output {
        let (_, rest) = argv.split_first().unwrap();
        Command::new("/bin/sh").args(rest).output().unwrap()
    }

    fn rebase(mut argv: Vec<String>, from: &str, to: &str) -> Vec<String> {
        for a in &mut argv {
            if a.starts_with(from) {
                *a = a.replacen(from, to, 1);
            }
        }
        argv
    }

    // a temp dir stands in for LOG_DIR
    #[test]
    fn pod_scripts_append_allocate_list_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().to_str().unwrap();
        let at = |argv: Vec<String>| rebase(argv, "/buildit/logs", d);

        let out = sh(&at(note_argv("/bin/sh", LogName::Build, "hello $(id) `x`")));
        assert!(out.status.success());
        let out = sh(&at(note_argv("/bin/sh", LogName::Build, "second")));
        assert!(out.status.success());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("build.log")).unwrap(),
            "hello $(id) `x`\nsecond\n"
        );

        let mut seen = Vec::new();
        for _ in 0..3 {
            let out = sh(&at(alloc_run_argv("/bin/sh")));
            assert!(out.status.success(), "{out:?}");
            seen.push(parse_run_number(&String::from_utf8_lossy(&out.stdout)).unwrap());
        }
        assert_eq!(seen, [1, 2, 3]);
        std::fs::write(dir.path().join("run-2.log"), b"kept").unwrap();
        let out = sh(&at(alloc_run_argv("/bin/sh")));
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "4");
        assert_eq!(
            std::fs::read(dir.path().join("run-2.log")).unwrap(),
            b"kept"
        );

        let out = sh(&at(list_argv("/bin/sh")));
        assert!(out.status.success());
        assert_eq!(
            parse_list(&String::from_utf8_lossy(&out.stdout)),
            [
                LogName::Build,
                LogName::Run(1),
                LogName::Run(2),
                LogName::Run(3),
                LogName::Run(4)
            ]
        );
        assert!(LIST_SCRIPT.contains("ls"));

        let out = sh(&at(read_argv("/bin/sh", LogName::Build)));
        assert!(out.status.success(), "{out:?}");
        let t = LogText::parse(out.stdout).unwrap();
        assert_eq!(t.body(), b"hello $(id) `x`\nsecond\n");
        assert_eq!(t.total_lines(), 2);

        let out = sh(&at(read_argv("/bin/sh", LogName::Run(9))));
        assert_eq!(out.status.code(), Some(3));

        // over the cap: the head is skipped and line numbers stay true
        let big: String = (1..=10).map(|i| format!("l{i}\n")).collect();
        std::fs::write(dir.path().join("run-5.log"), &big).unwrap();
        let mut argv = at(read_argv("/bin/sh", LogName::Run(5)));
        let last = argv.len() - 1;
        argv[last] = "10".to_string();
        let out = sh(&argv);
        assert!(out.status.success(), "{out:?}");
        let t = LogText::parse(out.stdout).unwrap();
        assert_eq!(t.body(), b"l8\nl9\nl10\n");
        assert_eq!(t.total_lines(), 10);
    }

    #[test]
    fn step_argv_passes_argv_through() {
        let argv = step_argv(
            "/bin/sh",
            LogName::Run(2),
            0,
            &[
                "buildah".to_string(),
                "run".to_string(),
                "c; rm -rf /".to_string(),
            ],
        );
        assert_eq!(argv[0], "/bin/sh");
        assert_eq!(argv[1], "-c");
        assert_eq!(
            argv[3..],
            [
                "sh",
                "/buildit/logs/run-2.log",
                "1",
                "buildah",
                "run",
                "c; rm -rf /"
            ]
        );
    }
}
