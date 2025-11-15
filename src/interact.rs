use std::{
    cmp::max,
    collections::VecDeque,
    fs::File,
    io::{Write, stdin, stdout},
    panic::{set_hook, take_hook},
    path::Path,
};

use anyhow::Context;
use rusqlite::Transaction;
use termion::{
    cursor::HideCursor,
    event::Key,
    input::TermRead,
    raw::IntoRawMode,
    screen::{IntoAlternateScreen, ToMainScreen},
};

use crate::{
    Order,
    article::{Article, ArxivId},
    config::{Config, Highlight},
    filter::Filter,
    rate_limited_client::Client,
};

pub fn init_panic_hook() -> anyhow::Result<()> {
    let screen = stdout().into_raw_mode()?;
    screen.suspend_raw_mode()?;

    let original_hook = take_hook();
    set_hook(Box::new(move |panic_info| {
        let _ = screen.suspend_raw_mode();
        let _ = write!(stdout(), "{}", ToMainScreen);
        let _ = stdout().flush();
        original_hook(panic_info);
    }));
    Ok(())
}

/// Interactively show one article at a time.
///
/// Only articles matching the filter will be shown.
/// If update_filter is Some(...), it means that we are reading new
/// articles, which will be marked as seen. The update_filter specifies
/// for which articles we also want to see updates (new versions, etc.).
#[allow(clippy::too_many_arguments)]
pub fn interact(
    base_dir: &Path,
    conn: &Transaction,
    highlight: &Highlight,
    config: &Config,
    client: &mut Client,
    filter: &Filter,
    update_filter: Option<&Filter>,
    sort_by: Order,
) -> anyhow::Result<()> {
    let mut articles = Article::load(base_dir, conn)?;

    let mut seen_file = File::options()
        .append(true)
        .create(true)
        .open(base_dir.join("seen-articles"))
        .context("opening seen-articles file")?;

    let mut seen: Vec<ArxivId> = Vec::new();
    let mut unseen: Vec<ArxivId> = Vec::new();
    let mut updated: Vec<ArxivId> = Vec::new();

    for article in articles.values() {
        if filter.matches(article) {
            if let Some(update_filter) = update_filter {
                if article.last_seen_version() == 0 {
                    unseen.push(article.id().clone());
                } else if update_filter.matches(article)
                    && (article.last_seen_version() < article.last_version().number
                        || (article.journal_ref().is_some() && !article.seen_journal())
                        || (article.doi().is_some() && !article.seen_doi()))
                {
                    updated.push(article.id().clone());
                } else {
                    seen.push(article.id().clone());
                }
            } else {
                seen.push(article.id().clone());
            }
        }
    }

    match sort_by {
        Order::Date => {
            // Sort seen articles by date of the first version.
            seen.sort_by_cached_key(|id| articles[id].first_version().date);
        }
        Order::Seen => {
            // Sort seen articles in the order in which they were seen.
            seen.sort_by_cached_key(|id| articles[id].last_seen_at());
        }
    }
    unseen.sort_by_cached_key(|id| articles[id].first_version().date);
    updated.sort_by_cached_key(|id| articles[id].first_version().date);

    // Convert to a VecDeque so that we can efficiently remove the first unseen or updated article
    // when marking it as seen.
    let mut unseen_or_updated: VecDeque<(ArxivId, bool)> =
        unseen.into_iter().map(|a| (a, false)).collect();
    unseen_or_updated.extend(updated.into_iter().map(|a| (a, true)));

    // Currently displayed article.
    enum Current {
        Read(usize), // the i-th seen article
        FirstUnseen, // the first unseen article
    }

    // If possible, show first unseen article.
    // Otherwise, if possible, show last seen article.
    // Otherwise, quit.
    #[allow(clippy::collapsible_else_if)]
    let mut state = if update_filter.is_some() {
        if !unseen_or_updated.is_empty() {
            Current::FirstUnseen
        } else if !seen.is_empty() {
            Current::Read(seen.len() - 1)
        } else {
            println!("No articles. You should probably run `arxiv-reader pull`.");
            return Ok(());
        }
    } else {
        if !seen.is_empty() {
            Current::Read(0)
        } else {
            println!("No articles.");
            return Ok(());
        }
    };
    let mut latex_to_unicode = config.latex_to_unicode;
    let mut error_message = String::new();

    init_panic_hook().context("initializing panic hook")?;
    let screen = stdout().into_raw_mode()?.into_alternate_screen()?;
    // Suspend raw mode as it interferes with printing.
    screen.suspend_raw_mode()?;
    let mut screen = HideCursor::from(screen);

    loop {
        // Currently displayed article and its index in the list of all articles (whether
        // seen or unseen).
        let (article, show_updates, index) = match state {
            Current::Read(i) => (articles.get_mut(&seen[i]).unwrap(), false, i),
            Current::FirstUnseen => {
                let (id, show_updates) = unseen_or_updated.front().unwrap();
                (articles.get_mut(id).unwrap(), *show_updates, seen.len())
            }
        };

        let (width, height) = termion::terminal_size().context("retrieving terminal size")?;
        let width = width as usize;
        let height = height as usize;

        // Clear screen and move cursor to top left corner.
        write!(
            screen,
            "{}{}",
            termion::clear::All,
            termion::cursor::Goto(1, 1),
        )?;
        screen.flush()?;

        // Print the status line.
        let mut status_items = Vec::new();
        let mut info = String::new();
        if article.last_seen_version() > 0 {
            info += "(seen)";
        } else {
            info += "      ";
        }
        info += "  ";
        if article.is_bookmarked() {
            info += "(bookmarked)";
        } else {
            info += "            ";
        }
        status_items.push(info);
        if update_filter.is_some() {
            status_items.push(format!("{} unseen left", unseen_or_updated.len()));
        }
        status_items.push(format!(
            "article {} of {}",
            index + 1,
            seen.len() + unseen_or_updated.len()
        ));
        let mut status_line = String::new();
        let mut remaining_length = max(
            width - status_items.iter().map(|s| s.len()).sum::<usize>(),
            status_items.len() - 1,
        );
        for (i, item) in status_items.iter().enumerate() {
            if i > 0 {
                let cnt = remaining_length / (status_items.len() - i);
                status_line += &" ".repeat(cnt);
                remaining_length -= cnt;
            }
            status_line += item;
        }

        println!("{}", status_line);
        println!();

        // Print the article.
        article.print(highlight, show_updates, latex_to_unicode);

        // Print list of keyboard shortcuts.
        let append_shortcut_lines = |shortcuts: Vec<String>, shortcut_lines: &mut Vec<String>| {
            let mut current_line = String::new();
            for shortcut in shortcuts.into_iter() {
                if !current_line.is_empty() && current_line.len() + 2 + shortcut.len() > width {
                    shortcut_lines.push(current_line.clone());
                    current_line.clear();
                }
                current_line += &shortcut;
                current_line += "; ";
            }
            if !current_line.is_empty() {
                shortcut_lines.push(current_line.clone());
            }
        };
        println!();
        let mut shortcuts = vec![
            "[q] quit",
            "[o] open webpage",
            "[p] open pdf",
            "[d] open directory",
            "[n] edit notes",
            "[b] toggle bookmark",
            "[u] turn on/off latex-to-unicode",
            "[RIGHT] next article",
            "[LEFT] previous article",
        ];
        if update_filter.is_none() {
            shortcuts.extend(vec!["[END] last article", "[HOME] first article"]);
        }
        let mut shortcut_lines = Vec::new();
        append_shortcut_lines(
            shortcuts.into_iter().map(|s| s.to_string()).collect(),
            &mut shortcut_lines,
        );
        shortcut_lines.push(String::new());
        shortcut_lines.push("Toggle tags:".to_string());
        let mut shortcuts = Vec::new();
        for (shortcut, name) in &config.tags {
            shortcuts.push(format!("[{}] {}", shortcut, name).to_string());
        }
        append_shortcut_lines(shortcuts, &mut shortcut_lines);
        write!(
            screen,
            "{}{}",
            termion::cursor::Goto(1, max(1, (height - shortcut_lines.len() - 2) as u16)),
            error_message,
        )?;
        write!(
            screen,
            "{}",
            termion::cursor::Goto(1, max(1, (height - shortcut_lines.len() + 1) as u16))
        )?;
        screen.flush()?;
        print!("{}", shortcut_lines.join("\n"));
        screen.flush()?;

        // Read the next key event.
        screen.activate_raw_mode()?;
        let c = match stdin().keys().next() {
            Some(c) => c,
            None => break,
        };
        screen.suspend_raw_mode()?;

        write!(
            screen,
            "{}{}",
            termion::cursor::Goto(1, max(1, (height - shortcut_lines.len()) as u16)),
            termion::clear::CurrentLine,
        )?;
        write!(
            screen,
            "{}{}",
            termion::cursor::Goto(1, max(1, (height - shortcut_lines.len() - 1) as u16)),
            termion::clear::CurrentLine,
        )?;
        write!(
            screen,
            "{}{}",
            termion::cursor::Goto(1, max(1, (height - shortcut_lines.len() - 2) as u16)),
            termion::clear::CurrentLine,
        )?;

        match c? {
            Key::Char('q') => {
                // Quit.
                break;
            }
            Key::Char('o') => {
                // Open webpage.
                article.open_abs()?;
                error_message = String::new();
            }
            Key::Char('p') => {
                // Download and then open pdf.
                if article.last_version().probably_has_pdf() {
                    match article.download_pdf(base_dir, client) {
                        Ok(_) => {
                            article.open_pdf(base_dir)?;
                            error_message = String::new();
                        }
                        Err(err) => {
                            error_message = format!("{err:#}");
                        }
                    }
                }
            }
            Key::Char('d') => {
                // Open the data directory.
                article.open_dir(base_dir)?;
                error_message = String::new();
            }
            Key::Char('n') => {
                // Show cursor and switch to main screen before starting the editor.
                write!(
                    screen,
                    "{}{}",
                    termion::cursor::Show,
                    termion::screen::ToMainScreen
                )?;
                screen.flush()?;
                // Edit the notes file.
                let res = article.edit_notes(base_dir);
                // Switch back to alternate screen and hide cursor.
                write!(
                    screen,
                    "{}{}",
                    termion::screen::ToAlternateScreen,
                    termion::cursor::Hide
                )?;
                screen.flush()?;
                // Relay any errors from the editor.
                res?;
                error_message = String::new();
            }
            Key::Char('u') => {
                // Toggle latex-to-unicode.
                latex_to_unicode = !latex_to_unicode;
                error_message = String::new();
            }
            Key::End if update_filter.is_none() => {
                state = Current::Read(seen.len() - 1);
                error_message = String::new();
            }
            Key::Home if update_filter.is_none() => {
                state = Current::Read(0);
                error_message = String::new();
            }
            Key::Right => {
                // Mark the current article as seen and go to the next article.
                state = match state {
                    Current::Read(i) => {
                        if i + 1 < seen.len() {
                            Current::Read(i + 1)
                        } else if !unseen_or_updated.is_empty() {
                            Current::FirstUnseen
                        } else {
                            Current::Read(i)
                        }
                    }
                    Current::FirstUnseen => {
                        // Mark this article as seen.
                        article.mark_as_seen(&mut seen_file)?;
                        seen.push(article.id().clone());
                        unseen_or_updated.pop_front();
                        if !unseen_or_updated.is_empty() {
                            Current::FirstUnseen
                        } else {
                            Current::Read(seen.len() - 1)
                        }
                    }
                };
                error_message = String::new();
            }
            Key::Left => {
                // Go the the previous article.
                state = match state {
                    Current::Read(i) => {
                        if i > 0 {
                            Current::Read(i - 1)
                        } else {
                            Current::Read(i)
                        }
                    }
                    Current::FirstUnseen => {
                        if !seen.is_empty() {
                            Current::Read(seen.len() - 1)
                        } else {
                            Current::FirstUnseen
                        }
                    }
                };
                error_message = String::new();
            }
            Key::Char(c) => {
                for (shortcut, name) in &config.tags {
                    if c == *shortcut {
                        // Toggle tag.
                        article.toggle_tag(base_dir, name)?;
                        error_message = String::new();
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}
