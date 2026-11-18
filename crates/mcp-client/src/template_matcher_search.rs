// Encoding-aware suffix reachability and bounded binding search.
//
// Complexity
// ----------
// Capture links consume one reserved token or one decoded UTF-8 scalar.
// Building links and each variable suffix row costs O(uri bytes); literal rows
// additionally compare their literal. Storage is O(parts * uri bytes), capped
// at MAX_CELLS. Unique-name reconstruction follows one path. Repeated names
// require binding-dependent search (word equations are not generally a regular
// language); an explicit stack and byte-charged budget bound that search rather
// than memoizing unsoundly by only (part, offset). Exhaustion fails closed.

use super::{Part, is_reserved_char};
use std::collections::{HashMap, HashSet};

const MAX_CELLS: usize = 2_000_000;
const SEARCH_WORK: usize = 4_000_000;
const NONE: usize = usize::MAX;

pub(super) struct Links {
    pub(super) simple: Vec<usize>,
    reserved: Vec<usize>,
}

fn byte(uri: &[u8], start: usize) -> Option<(u8, usize)> {
    let first = *uri.get(start)?;
    if first == b'%' {
        let a = (*uri.get(start + 1)? as char).to_digit(16)?;
        let b = (*uri.get(start + 2)? as char).to_digit(16)?;
        Some(((a * 16 + b) as u8, start + 3))
    } else {
        Some((first, start + 1))
    }
}

impl Links {
    pub(super) fn new(uri: &str) -> Self {
        let mut simple = vec![NONE; uri.len() + 1];
        let mut reserved = simple.clone();
        for (start, ch) in uri.char_indices() {
            // A valid triplet is indivisible. Malformed percent text retains
            // its existing literal spelling for reserved captures.
            reserved[start] = if ch == '%' {
                byte(uri.as_bytes(), start).map_or(start + 1, |(_, end)| end)
            } else {
                start + ch.len_utf8()
            };
            if is_reserved_char(ch) {
                continue;
            }
            let Some((first, mut end)) = byte(uri.as_bytes(), start) else {
                // The decoder preserves malformed '%' literally.
                simple[start] = start + 1;
                continue;
            };
            let width = match first {
                0..=0x7f => 1,
                0xc2..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf4 => 4,
                _ => continue,
            };
            let mut decoded = [0; 4];
            decoded[0] = first;
            let mut valid = true;
            for slot in decoded.iter_mut().take(width).skip(1) {
                if let Some((value, next)) = byte(uri.as_bytes(), end) {
                    *slot = value;
                    end = next;
                } else {
                    valid = false;
                    break;
                }
            }
            if valid && std::str::from_utf8(&decoded[..width]).is_ok() {
                simple[start] = end;
            }
        }
        Self { simple, reserved }
    }

    fn for_part(&self, part: &Part) -> &[usize] {
        if matches!(part, Part::SimpleVar(_)) {
            &self.simple
        } else {
            &self.reserved
        }
    }
}

fn name(part: &Part) -> Option<&str> {
    match part {
        Part::Literal(_) => None,
        Part::SimpleVar(name) | Part::ReservedVar(name) | Part::FragmentVar(name) => Some(name),
    }
}

fn decode(value: &str) -> Option<String> {
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .ok()
        .map(|s| s.into_owned())
}

// Used only for names shared between simple and reserved operators: reserved
// expansion preserves existing triplets, while simple expansion decodes once.
fn reserved_expansion(value: &str) -> String {
    let mut out = String::new();
    let bytes = value.as_bytes();
    let mut i = 0;
    const HEX: &[u8] = b"0123456789ABCDEF";
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%'
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
        {
            out.push_str(&value[i..i + 3]);
            i += 3;
        } else {
            if b.is_ascii_alphanumeric()
                || matches!(b, b'-' | b'.' | b'_' | b'~')
                || is_reserved_char(b as char)
            {
                out.push(b as char);
            } else {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 15) as usize] as char);
            }
            i += 1;
        }
    }
    out
}

pub(super) fn bind(parts: &[Part], uri: &str) -> Option<HashMap<String, String>> {
    let mut work = SEARCH_WORK;
    bind_with_budget(parts, uri, &mut work)
}

fn spend(work: &mut usize, amount: usize) -> Option<()> {
    *work = work.checked_sub(amount)?;
    Some(())
}

pub(super) fn bind_with_budget(
    parts: &[Part],
    uri: &str,
    work: &mut usize,
) -> Option<HashMap<String, String>> {
    if parts
        .windows(2)
        .any(|pair| name(&pair[0]).is_some() && name(&pair[1]).is_some())
    {
        return None;
    }
    if parts.iter().all(|p| matches!(p, Part::Literal(_))) {
        let mut cursor = uri;
        for part in parts {
            if let Part::Literal(lit) = part {
                cursor = cursor.strip_prefix(lit)?;
            }
        }
        return cursor.is_empty().then(HashMap::new);
    }
    let cells = (parts.len() + 1).checked_mul(uri.len().checked_add(1)?)?;
    if cells > MAX_CELLS {
        return None;
    }
    spend(
        work,
        uri.len().saturating_mul(8).saturating_add(parts.len()),
    )?;
    let links = Links::new(uri);
    let n = uri.len();
    let mut rows = vec![vec![NONE; n + 1]; parts.len() + 1];
    rows[parts.len()][n] = n;
    for index in (0..parts.len()).rev() {
        let part = &parts[index];
        for start in (0..=n).rev() {
            let charge = match part {
                Part::Literal(lit) => lit.len().saturating_add(1),
                _ => 1,
            };
            spend(work, charge)?;
            if !uri.is_char_boundary(start) {
                continue;
            }
            rows[index][start] = match part {
                Part::Literal(lit) => {
                    if uri[start..].starts_with(lit) && rows[index + 1][start + lit.len()] != NONE {
                        start + lit.len()
                    } else {
                        NONE
                    }
                }
                Part::FragmentVar(_) => {
                    if uri[start..].starts_with('#') && start + 1 < n {
                        n
                    } else {
                        NONE
                    }
                }
                _ => {
                    let next = links.for_part(part)[start];
                    if next == NONE {
                        continue;
                    }
                    if index + 1 < parts.len() && !matches!(parts[index + 1], Part::Literal(_)) {
                        NONE
                    } else {
                        let direct = rows[index + 1][next] != NONE;
                        let later = rows[index][next];
                        if matches!(part, Part::ReservedVar(_)) && later != NONE {
                            later
                        } else if direct {
                            next
                        } else {
                            later
                        }
                    }
                }
            };
        }
    }
    if rows[0][0] == NONE {
        return None;
    }
    let mut seen = HashSet::new();
    let repeated = parts.iter().filter_map(name).any(|s| !seen.insert(s));
    if !repeated {
        let mut values = HashMap::new();
        let mut start = 0;
        for (i, part) in parts.iter().enumerate() {
            let end = rows[i][start];
            match part {
                Part::Literal(_) => {}
                Part::SimpleVar(s) => {
                    values.insert(s.clone(), decode(&uri[start..end])?);
                }
                Part::ReservedVar(s) => {
                    values.insert(s.clone(), uri[start..end].to_owned());
                }
                Part::FragmentVar(s) => {
                    values.insert(s.clone(), uri[start + 1..end].to_owned());
                }
            }
            start = end;
        }
        return Some(values);
    }
    // Each constraint is either a decoded scalar from a simple occurrence,
    // or an encoded spelling shared by reserved occurrences. A later simple
    // occurrence resolves the latter; rollback restores the previous constraint.
    #[derive(Clone)]
    enum Constraint {
        Scalar(String),
        Encoded(String),
    }
    struct Frame {
        index: usize,
        start: usize,
        next: usize,
        undo: Option<(String, Option<Constraint>)>,
    }
    let mut stack = vec![Frame {
        index: 0,
        start: 0,
        next: 0,
        undo: None,
    }];
    let mut values = HashMap::<String, Constraint>::new();
    while let Some(frame) = stack.last_mut() {
        spend(work, 1)?;
        if frame.index == parts.len() {
            return Some(
                values
                    .into_iter()
                    .map(|(key, constraint)| {
                        let (Constraint::Scalar(value) | Constraint::Encoded(value)) = constraint;
                        (key, value)
                    })
                    .collect(),
            );
        }
        let index = frame.index;
        let start = frame.start;
        let part = &parts[index];
        let end = match part {
            Part::Literal(_) | Part::FragmentVar(_) => {
                if frame.next == NONE {
                    NONE
                } else {
                    frame.next = NONE;
                    rows[index][start]
                }
            }
            _ => {
                let mut candidate = if frame.next == NONE {
                    NONE
                } else {
                    links.for_part(part)[frame.next]
                };
                while candidate != NONE && rows[index + 1][candidate] == NONE {
                    spend(work, 1)?;
                    candidate = links.for_part(part)[candidate];
                }
                frame.next = candidate;
                candidate
            }
        };
        if end == NONE {
            let old = stack.pop().expect("frame exists");
            if let Some((key, previous)) = old.undo {
                if let Some(previous) = previous {
                    values.insert(key, previous);
                } else {
                    values.remove(&key);
                }
            }
            continue;
        }
        let mut undo = None;
        if let Some(key) = name(part) {
            let capture_start = start + usize::from(matches!(part, Part::FragmentVar(_)));
            let capture = &uri[capture_start..end];
            // Covers decoding, encoding, equality, allocation, and hash-key work.
            let previous_len = values.get(key).map_or(0, |c| match c {
                Constraint::Scalar(s) | Constraint::Encoded(s) => s.len(),
            });
            spend(
                work,
                capture
                    .len()
                    .saturating_add(previous_len)
                    .saturating_add(key.len())
                    .saturating_mul(8)
                    .saturating_add(1),
            )?;
            let candidate = if matches!(part, Part::SimpleVar(_)) {
                let Some(value) = decode(capture) else {
                    continue;
                };
                Constraint::Scalar(value)
            } else {
                Constraint::Encoded(capture.to_owned())
            };
            let replacement = match (values.get(key), candidate) {
                (None, candidate) => Some(candidate),
                (Some(Constraint::Scalar(a)), Constraint::Scalar(b)) => {
                    if a != &b {
                        continue;
                    }
                    None
                }
                (Some(Constraint::Encoded(a)), Constraint::Encoded(b)) => {
                    if a != &b {
                        continue;
                    }
                    None
                }
                (Some(Constraint::Scalar(a)), Constraint::Encoded(b)) => {
                    if reserved_expansion(a) != b {
                        continue;
                    }
                    None
                }
                (Some(Constraint::Encoded(a)), Constraint::Scalar(b)) => {
                    if *a != reserved_expansion(&b) {
                        continue;
                    }
                    Some(Constraint::Scalar(b))
                }
            };
            if let Some(replacement) = replacement {
                let previous = values.insert(key.to_owned(), replacement);
                undo = Some((key.to_owned(), previous));
            }
        }
        stack.push(Frame {
            index: index + 1,
            start: end,
            next: end,
            undo,
        });
    }
    None
}
