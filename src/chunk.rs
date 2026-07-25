//! Boundary-aware text chunking shared by channels.
//!
//! Splits a reply into pieces no longer than a channel's limit, preferring to
//! break at a newline so paragraphs, list items, and code fences are not cut
//! mid-line when a clean break fits. The concatenation of the chunks always
//! equals the input, and every chunk measures at most `limit`.

/// Splits `text` into chunks, each measuring at most `limit` in the unit
/// returned by `measure` (for example UTF-16 code units for Telegram or
/// characters for Slack). Breaks at the last newline within a chunk when one is
/// available; otherwise falls back to a hard length split.
pub fn split(text: &str, limit: usize, measure: impl Fn(char) -> usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_len = 0;
    // Byte offset just past the most recent newline in `current`, if any.
    let mut last_break: Option<usize> = None;
    for character in text.chars() {
        let character_len = measure(character);
        if current_len + character_len > limit && !current.is_empty() {
            match last_break.filter(|&at| at > 0 && at < current.len()) {
                Some(at) => {
                    let tail = current.split_off(at);
                    chunks.push(std::mem::take(&mut current));
                    current = tail;
                    current_len = current.chars().map(&measure).sum();
                    // The carried remainder plus the next character may itself
                    // exceed the limit; flush it too to keep the guarantee.
                    if current_len + character_len > limit && !current.is_empty() {
                        chunks.push(std::mem::take(&mut current));
                        current_len = 0;
                    }
                }
                None => {
                    chunks.push(std::mem::take(&mut current));
                    current_len = 0;
                }
            }
            last_break = None;
        }
        current.push(character);
        current_len += character_len;
        if character == '\n' {
            last_break = Some(current.len());
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chars(text: &str, limit: usize) -> Vec<String> {
        split(text, limit, |_| 1)
    }

    #[test]
    fn empty_input_yields_no_chunks() {
        assert!(chars("", 10).is_empty());
    }

    #[test]
    fn short_text_stays_one_chunk() {
        assert_eq!(chars("hello world", 100), vec!["hello world".to_string()]);
    }

    #[test]
    fn prefers_a_newline_boundary_over_a_hard_cut() {
        // With a mid-buffer newline the break lands after it instead of
        // cutting the following line mid-word.
        assert_eq!(chars("ab\ncdef", 4), vec!["ab\n".to_string(), "cdef".to_string()]);
        // Without a newline the same limit falls back to a hard cut.
        assert_eq!(chars("abcdef", 4), vec!["abcd".to_string(), "ef".to_string()]);
    }

    #[test]
    fn falls_back_to_hard_split_without_newlines() {
        let chunks = chars(&"x".repeat(25), 10);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].chars().count(), 10);
        assert_eq!(chunks[2].chars().count(), 5);
    }

    #[test]
    fn concatenation_always_equals_input_and_respects_limit() {
        let text = "alpha\nbeta gamma delta\n\nepsilon zeta\nlong_unbroken_token_that_exceeds";
        for limit in [3, 5, 8, 13, 21] {
            let chunks = chars(text, limit);
            assert_eq!(chunks.concat(), text, "limit {limit} lost content");
            assert!(
                chunks.iter().all(|chunk| chunk.chars().count() <= limit),
                "limit {limit} exceeded"
            );
        }
    }

    #[test]
    fn measures_in_utf16_when_asked() {
        // An emoji is two UTF-16 units, so a limit of 2 fits exactly one.
        let chunks = split("😀😀😀", 2, |c| c.len_utf16());
        assert_eq!(chunks.len(), 3);
        assert!(chunks
            .iter()
            .all(|chunk| chunk.encode_utf16().count() <= 2));
        assert_eq!(chunks.concat(), "😀😀😀");
    }
}
