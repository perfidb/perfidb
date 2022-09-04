use std::io::{Error, stdout, Write};
use comfy_table::{Cell, Table, TableComponent};
use crossterm::{execute, terminal, QueueableCommand, cursor};
use crossterm::cursor::{MoveDown, MoveTo, MoveToColumn, MoveUp};
use crossterm::event::{Event, KeyCode, read};
use crossterm::terminal::{Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::style::{self, Color, SetBackgroundColor, SetForegroundColor, Stylize};
use crate::Database;
use crate::transaction::Transaction;

/// Open a terminal dialog to label transactions in a live table
pub(crate) fn live_label(db: &mut Database) -> Result<(), Error> {
    if let Some(transactions) = &db.last_query_results {
        execute!(stdout(), EnterAlternateScreen, MoveTo(0, 0))?;
        terminal::enable_raw_mode()?;
        // TODO: handle terminal resize
        let (columns, rows) = terminal::size()?;

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
    }

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
    fn repaint(&mut self) -> Vec<(u16, usize, bool)> {
        let remaining_trans_count = self.transactions_count - self.offset;
        let print_trains_count :usize = if remaining_trans_count > self.offset as usize { self.rows as usize } else { remaining_trans_count };
        let mut delta :Vec<(u16, usize, bool)> = vec![(0, 0, true)];
        for i in 1..print_trains_count {
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
            return delta;
        } else {
            return self.scroll_up();
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
            return delta;
        } else {
            return self.scroll_down();
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

fn repaint_window(delta: Vec<(u16, usize, bool)>, transactions: &Vec<Transaction>, selected_row: u16) {
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
    execute!(stdout(), style::Print(format!("|{}|{}|{}|{}|{}|{}|", t.id, t.account, t.date, t.description, t.amount, t.tags_display())), MoveToColumn(0)).unwrap();
    if highlight {
        execute!(stdout(), SetForegroundColor(Color::White), SetBackgroundColor(Color::Black)).unwrap();
    }
}
