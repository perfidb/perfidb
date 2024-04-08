use std::borrow::Cow::{self, Borrowed, Owned};

use rustyline::completion::FilenameCompleter;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::hint::HistoryHinter;
use rustyline::validate::MatchingBracketValidator;
use rustyline::{Completer, Helper, Hinter, Validator};

#[derive(Helper, Completer, Hinter, Validator)]
pub(crate) struct PerfidbHelper {
    #[rustyline(Completer)]
    pub(crate) completer: FilenameCompleter,
    pub(crate) highlighter: MatchingBracketHighlighter,
    #[rustyline(Validator)]
    pub(crate) validator: MatchingBracketValidator,
    #[rustyline(Hinter)]
    pub(crate) hinter: HistoryHinter,
    pub(crate) colored_prompt: String,
}

impl Highlighter for PerfidbHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Borrowed(&self.colored_prompt)
        } else {
            Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned("\x1b[2m".to_owned() + hint + "\x1b[m")
    }

    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize, forced: bool) -> bool {
        self.highlighter.highlight_char(line, pos, forced)
    }
}