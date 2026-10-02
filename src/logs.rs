use std::fmt;

use anyhow::{Context, Result, bail};
use regex::{Regex, RegexBuilder};
use serde::Serialize;

use crate::pod::BuilderPod;

pub const LOG_DIR: &str = "/tmp/buildit-logs";
const MAX_READ: u64 = 4 * 1024 * 1024;
const LINE_CAP: usize = 1024;
const REGEX_SIZE: usize = 1024 * 1024;
const MAX_CONTEXT: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogName {
    Build,
    Run(u32),
}

impl LogName {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "build" => Ok(Self::Build),
            _ => match s.strip_prefix("run:").and_then(|n| n.parse().ok()) {
                Some(n) => Ok(Self::Run(n)),
                None => bail!("which must be build or run:<n>, got {s:?}"),
            },
        }
    }

    pub fn path(self) -> String {
        match self {
            Self::Build => format!("{LOG_DIR}/build.log"),
            Self::Run(n) => format!("{LOG_DIR}/run-{n}.log"),
        }
    }
}

impl fmt::Display for LogName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Build => f.write_str("build"),
            Self::Run(n) => write!(f, "run:{n}"),
        }
    }
}

#[derive(Debug)]
pub enum Query {
    Tail { lines: usize },
    Range { from: usize, to: Option<usize> },
    Grep { pattern: Regex, context: usize },
}

impl Query {
    pub fn grep(pattern: &str, context: usize) -> Result<Self> {
        let pattern = RegexBuilder::new(pattern)
            .size_limit(REGEX_SIZE)
            .build()
            .with_context(|| format!("bad grep pattern {pattern:?}"))?;
        Ok(Self::Grep {
            pattern,
            context: context.min(MAX_CONTEXT),
        })
    }
}

#[derive(Debug, Serialize)]
pub struct Page {
    pub lines: String,
    pub total_lines: usize,
    pub matched: usize,
    pub truncated: bool,
    pub truncated_head: bool,
}

pub struct Log {
    text: String,
    truncated_head: bool,
}

impl Log {
    pub async fn fetch(pod: &BuilderPod, name: LogName) -> Result<Self> {
        let read = (MAX_READ + 1).to_string();
        let argv = ["tail", "-c", &read, "--", &name.path()].map(String::from);
        let out = pod.exec_output(&argv, MAX_READ + 1).await?;
        if out.code != Some(0) {
            bail!(
                "reading the {name} log: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(Self::from_bytes(out.stdout))
    }

    fn from_bytes(mut bytes: Vec<u8>) -> Self {
        let truncated_head = bytes.len() as u64 > MAX_READ;
        if truncated_head {
            let cut = bytes
                .iter()
                .position(|b| *b == b'\n')
                .map_or(bytes.len(), |i| i + 1);
            bytes.drain(..cut);
        }
        Self {
            text: String::from_utf8_lossy(&bytes).into_owned(),
            truncated_head,
        }
    }

    pub fn query(&self, query: &Query, max_bytes: usize) -> Page {
        let lines: Vec<&str> = self.text.lines().collect();
        let total = lines.len();
        let (picks, matched): (Vec<(usize, bool)>, usize) = match query {
            Query::Tail { lines: n } => {
                let picks: Vec<_> = (total.saturating_sub(*n)..total)
                    .map(|i| (i, true))
                    .collect();
                let n = picks.len();
                (picks, n)
            }
            Query::Range { from, to } => {
                let end = to.unwrap_or(total).min(total);
                let picks: Vec<_> = (from.max(&1) - 1..end).map(|i| (i, true)).collect();
                let n = picks.len();
                (picks, n)
            }
            Query::Grep { pattern, context } => {
                let hits: Vec<bool> = lines.iter().map(|l| pattern.is_match(l)).collect();
                let mut keep = vec![false; total];
                for (i, _) in hits.iter().enumerate().filter(|(_, hit)| **hit) {
                    let hi = (i + context + 1).min(total);
                    keep[i.saturating_sub(*context)..hi].fill(true);
                }
                let picks = (0..total).filter(|i| keep[*i]).map(|i| (i, hits[i]));
                (picks.collect(), hits.iter().filter(|h| **h).count())
            }
        };
        let mut rendered = Vec::with_capacity(picks.len());
        let mut prev: Option<usize> = None;
        for (i, hit) in picks {
            if prev.is_some_and(|p| i > p + 1) {
                rendered.push("--".to_string());
            }
            let mark = if hit { ':' } else { '-' };
            rendered.push(format!("{}{mark}{}", i + 1, clip(lines[i])));
            prev = Some(i);
        }
        let keep_end = matches!(query, Query::Tail { .. });
        let (lines, truncated) = fit(rendered, max_bytes, keep_end);
        Page {
            lines,
            total_lines: total,
            matched,
            truncated,
            truncated_head: self.truncated_head,
        }
    }
}

fn clip(line: &str) -> String {
    if line.len() <= LINE_CAP {
        return line.to_string();
    }
    let cut = line.floor_char_boundary(LINE_CAP);
    format!("{}... [{} bytes cut]", &line[..cut], line.len() - cut)
}

// whole lines up to max_bytes, from the end when keep_end
fn fit(rendered: Vec<String>, max_bytes: usize, keep_end: bool) -> (String, bool) {
    let all = rendered.len();
    let mut used = 0;
    let fits = |l: &String| {
        used += l.len() + 1;
        used <= max_bytes
    };
    let kept: Vec<String> = if keep_end {
        let mut tail: Vec<_> = rendered.into_iter().rev().take_while(fits).collect();
        tail.reverse();
        tail
    } else {
        rendered.into_iter().take_while(fits).collect()
    };
    let truncated = kept.len() < all;
    let mut out = kept.join("\n");
    if !kept.is_empty() {
        out.push('\n');
    }
    (out, truncated)
}

#[cfg(test)]
mod tests {
    use crate::logs::{Log, LogName, MAX_READ, Query};

    fn log(text: &str) -> Log {
        Log::from_bytes(text.as_bytes().to_vec())
    }

    fn numbered(n: usize) -> Log {
        log(&(1..=n).map(|i| format!("line {i}\n")).collect::<String>())
    }

    #[test]
    fn log_names_are_build_or_numbered_runs() {
        assert_eq!(
            LogName::parse("build").unwrap().path(),
            "/tmp/buildit-logs/build.log"
        );
        let run = LogName::parse("run:7").unwrap();
        assert_eq!(run.path(), "/tmp/buildit-logs/run-7.log");
        assert_eq!(run.to_string(), "run:7");
        for bad in [
            "",
            "Build",
            "run:",
            "run:-1",
            "run:1/../x",
            "../build",
            "run-1",
        ] {
            assert!(LogName::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn tail_and_range_number_lines_from_one() {
        let log = numbered(10);
        let page = log.query(&Query::Tail { lines: 2 }, 1024);
        assert_eq!(page.lines, "9:line 9\n10:line 10\n");
        assert_eq!(
            (page.total_lines, page.matched, page.truncated),
            (10, 2, false)
        );
        let page = log.query(
            &Query::Range {
                from: 3,
                to: Some(4),
            },
            1024,
        );
        assert_eq!(page.lines, "3:line 3\n4:line 4\n");
        let page = log.query(
            &Query::Range {
                from: 0,
                to: Some(1),
            },
            1024,
        );
        assert_eq!(page.lines, "1:line 1\n");
        let page = log.query(&Query::Range { from: 9, to: None }, 1024);
        assert_eq!(page.lines, "9:line 9\n10:line 10\n");
        let page = log.query(
            &Query::Range {
                from: 20,
                to: Some(30),
            },
            1024,
        );
        assert_eq!((page.lines.as_str(), page.matched), ("", 0));
        assert_eq!(log.query(&Query::Tail { lines: 99 }, 1024).matched, 10);
    }

    #[test]
    fn grep_marks_matches_context_and_gaps() {
        let log = numbered(12);
        let page = log.query(&Query::grep("^line (2|4|10)$", 1).unwrap(), 1024);
        assert_eq!(
            page.lines,
            "1-line 1\n2:line 2\n3-line 3\n4:line 4\n5-line 5\n--\n9-line 9\n10:line 10\n11-line 11\n"
        );
        assert_eq!((page.total_lines, page.matched), (12, 3));
        let page = log.query(&Query::grep("line 1[12]", 0).unwrap(), 1024);
        assert_eq!(page.lines, "11:line 11\n12:line 12\n");
        let page = log.query(&Query::grep("nope", 3).unwrap(), 1024);
        assert_eq!((page.lines.as_str(), page.matched), ("", 0));
    }

    #[test]
    fn grep_rejects_bad_and_oversized_patterns() {
        assert!(Query::grep("(", 0).is_err());
        assert!(Query::grep(r"\w{1000}{1000}", 0).is_err());
        let Query::Grep { context, .. } = Query::grep("x", 1000).unwrap() else {
            panic!("not a grep");
        };
        assert_eq!(context, 20);
    }

    #[test]
    fn output_keeps_whole_lines_within_max_bytes() {
        let log = numbered(10);
        let page = log.query(&Query::Tail { lines: 10 }, 20);
        assert_eq!(
            (page.lines.as_str(), page.truncated),
            ("9:line 9\n10:line 10\n", true)
        );
        let page = log.query(&Query::Range { from: 1, to: None }, 20);
        assert_eq!(
            (page.lines.as_str(), page.truncated),
            ("1:line 1\n2:line 2\n", true)
        );
        assert!(page.lines.len() <= 20);
    }

    #[test]
    fn long_lines_are_clipped_on_a_char_boundary() {
        let line = format!("{}é{}", "a".repeat(1023), "b".repeat(500));
        let page = log(&line).query(&Query::Tail { lines: 1 }, 64 * 1024);
        assert_eq!(
            page.lines,
            format!("1:{}... [502 bytes cut]\n", "a".repeat(1023))
        );
    }

    #[test]
    fn reads_past_the_window_drop_the_partial_first_line() {
        let window = usize::try_from(MAX_READ).unwrap();
        let mut bytes = b"partial\nwhole\n".to_vec();
        bytes.resize(window + 1, b'x');
        let log = Log::from_bytes(bytes);
        let page = log.query(
            &Query::Range {
                from: 1,
                to: Some(1),
            },
            64 * 1024,
        );
        assert!(page.truncated_head);
        assert_eq!(page.lines, "1:whole\n");
        assert_eq!(page.total_lines, 2);

        let page = Log::from_bytes(b"ok \xff\n".to_vec()).query(&Query::Tail { lines: 1 }, 1024);
        assert!(!page.truncated_head);
        assert_eq!(page.lines, "1:ok \u{fffd}\n");
    }
}
