use std::io::{Error, stdout};

use crossterm::{execute, terminal};
use crossterm::cursor::{MoveTo, MoveToColumn};
use crossterm::event::{Event, KeyCode, read};
use crossterm::style::{self, Color, SetBackgroundColor, SetForegroundColor};
use crossterm::terminal::{ClearType, EnterAlternateScreen, LeaveAlternateScreen};

use crate::{Database, db};
use crate::transaction::Transaction;

/// Open a terminal dialog to label transactions in a live table
/// It takes last_query_results as a list of ids because we might change labels, so we'll need to re-render labels.
pub(crate) fn live_label(last_query_results: Vec<u32>, db: &mut Database) -> Result<(), Error> {
    let mut transactions: Vec<Transaction> = last_query_results.iter().map(|trans_id| db.find_by_id(*trans_id)).collect();

    execute!(stdout(), EnterAlternateScreen, MoveTo(0, 0))?;
    terminal::enable_raw_mode()?;
    // TODO: handle terminal resize
    let (_columns, rows) = terminal::size()?;

    let mut window = Window {
        rows,
        transactions_count: transactions.len(),
        offset: 0,
        selected_row: 0
    };

    repaint_window(window.repaint(), &transactions, window.selected_row);

    loop {
        // `read()` blocks until an `Event` is available
        match read().unwrap() {
            Event::FocusGained => println!("FocusGained"),
            Event::FocusLost => println!("FocusLost"),
            Event::Key(event) => {
                if let KeyCode::Char(c) = event.code {
                    match c {
                        'q' => break,
                        'j' => {
                            let delta = window.move_down();
                            repaint_window(delta, &transactions, window.selected_row);
                        },
                        'k' => {
                            let delta = window.move_up();
                            repaint_window(delta, &transactions, window.selected_row);
                        },
                        'l' => {
                            execute!(stdout(), MoveTo(114, window.selected_row)).unwrap();
                            terminal::disable_raw_mode().unwrap();
                            let mut new_labels = String::new();
                            std::io::stdin().read_line(&mut new_labels)?;
                            let trans_id = transactions[window.selected_transaction_index()].id;

                            let label_ops = db::label_op::parse_label_ops(&new_labels);
                            if let Ok((_, label_ops)) = label_ops {
                                db.apply_label_ops(trans_id, label_ops);
                            }

                            transactions[window.selected_transaction_index()].labels = db.find_by_id(trans_id).labels;
                            terminal::enable_raw_mode().unwrap();
                            repaint_window(vec![(window.selected_row, window.offset + window.selected_row as usize, true)], &transactions, window.selected_row);
                            execute!(stdout(), MoveTo(114, window.selected_row)).unwrap();
                        },
                        _ => {}
                    }
                }
            },
            Event::Mouse(event) => println!("{:?}", event),
            #[cfg(feature = "bracketed-paste")]
            Event::Paste(data) => println!("{:?}", data),
            Event::Resize(width, height) => println!("New size {}x{}", width, height),
            Event::Paste(s) => println!("{}", s),
        }
    }

    terminal::disable_raw_mode()?;
    execute!(stdout(), LeaveAlternateScreen)?;

    Ok(())
}

struct Window {
    /// Number of rows in this window
    rows: u16,

    /// Number of total transactions
    transactions_count: usize,

    /// Scrolling offset
    offset: usize,

    /// The row that is selected. 0 <= selected_row < rows
    selected_row: u16,
}

impl Window {
    fn selected_transaction_index(&self) -> usize {
        self.offset + self.selected_row as usize
    }

    fn repaint(&mut self) -> Vec<(u16, usize, bool)> {
        let remaining_trans_count = self.transactions_count - self.offset;
        let print_trans_count :usize = if remaining_trans_count > self.rows as usize { self.rows as usize } else { remaining_trans_count };
        let mut delta :Vec<(u16, usize, bool)> = vec![(0, 0, true)];
        for i in 1..print_trans_count {
            delta.push((i as u16, i, false));
        }
        delta
    }

    fn move_down(&mut self) -> Vec<(u16, usize, bool)> {
        if self.offset + self.selected_row as usize >= self.transactions_count - 1 {
            return vec![];
        }

        if self.selected_row < self.rows - 1 {
            let mut delta = vec![];
            delta.push((self.selected_row, self.offset + self.selected_row as usize, false));
            self.selected_row += 1;
            delta.push((self.selected_row, self.offset + self.selected_row as usize, true));
            delta
        } else {
            self.scroll_up()
        }
    }

    fn scroll_up(&mut self) -> Vec<(u16, usize, bool)> {
        if self.offset + self.rows as usize >= self.transactions_count {
            return vec![];
        }

        self.offset += 1;
        let mut delta = vec![];
        for i in 0..self.rows - 1 {
            delta.push((i, self.offset + i as usize, false));
        }
        delta.push((self.rows - 1, self.offset + self.rows as usize - 1, true));
        delta
    }

    fn move_up(&mut self) -> Vec<(u16, usize, bool)> {
        if self.offset + self.selected_row as usize == 0 {
            return vec![];
        }

        if self.selected_row > 0 {
            let mut delta = vec![];
            delta.push((self.selected_row, self.offset + self.selected_row as usize, false));
            self.selected_row -= 1;
            delta.push((self.selected_row, self.offset + self.selected_row as usize, true));

            delta
        } else {
            self.scroll_down()
        }
    }

    fn scroll_down(&mut self) -> Vec<(u16, usize, bool)> {
        if self.offset == 0 {
            return vec![];
        }

        self.offset -= 1;
        let mut delta = vec![];
        delta.push((0, self.offset, true));
        for i in 1..self.rows {
            delta.push((i, self.offset + i as usize, false));
        }
        delta
    }
}

fn repaint_window(delta: Vec<(u16, usize, bool)>, transactions: &[Transaction], selected_row: u16) {
    for (row, trans_index, highlight) in delta {
        execute!(stdout(), MoveTo(0, row), terminal::Clear(ClearType::CurrentLine)).unwrap();
        print_transaction(&transactions[trans_index], highlight);
    }
    execute!(stdout(), MoveTo(0, selected_row)).unwrap();
}

/// Print a single transaction, in current terminal line
fn print_transaction(t: &Transaction, highlight: bool) {
    if highlight {
        execute!(stdout(), SetForegroundColor(Color::Black), SetBackgroundColor(Color::White)).unwrap();
    }
    let desc = if t.description.len() > 50 {
        let mut cut_down_version = t.description[0..49].to_owned();
        cut_down_version.push('…');
        cut_down_version
    } else {
        t.description.clone()
    };
    execute!(stdout(), style::Print(format!("| {:4} | {:14} | {} | {:50} | {:10} | {:15} |", t.id, t.account, t.date, desc, t.amount, t.tags_display())), MoveToColumn(0)).unwrap();
    if highlight {
        execute!(stdout(), SetForegroundColor(Color::White), SetBackgroundColor(Color::Black)).unwrap();
    }
}
