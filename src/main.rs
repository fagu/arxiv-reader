mod article;
mod bibtex;
mod config;
mod db;
mod filter;
mod interact;
mod oai;
mod rate_limited_client;
mod util;

use std::{
    fs::{OpenOptions, create_dir},
    io::{Write, stdout},
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;

use crate::{
    article::{Article, ArxivId},
    config::{Config, Highlight},
    filter::Filter,
    rate_limited_client::Client,
};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize the database.
    Init,
    /// Pull article data from arXiv.
    Pull,
    /// Look at new articles.
    News {
        /// How to sort the older (seen) articles.
        #[arg(long, default_value = "seen")]
        sort_by: Order,
    },
    /// Find articles matching certain patterns.
    Find {
        /// What to do with the matching articles.
        #[arg(short, long, default_value = "short")]
        show: LsFormat,
        /// How to sort the matching articles.
        ///
        /// "seen" also filters out articles that have not been seen in the news.
        #[arg(long, default_value = "date")]
        sort_by: Order,
        #[command(flatten, next_help_heading = "Patterns")]
        filters: Filters,
    },
    /// Interact with a bibtex file.
    #[command(subcommand)]
    Bibtex(BibtexCommand),
    /// Save or load metadata.
    #[command(subcommand)]
    Database(DatabaseCommand),
    #[command(hide = true)]
    GenerateCompletions { generator: Shell },
}

#[derive(Subcommand)]
enum BibtexCommand {
    /// Create bookmarks from a bibtex file.
    Bookmark {
        #[arg(value_hint = clap::ValueHint::FilePath)]
        file: PathBuf,
    },
    /// Suggest updates to a bibtex file.
    Check {
        #[arg(value_hint = clap::ValueHint::FilePath)]
        file: PathBuf,
    },
}

#[derive(Subcommand)]
enum DatabaseCommand {
    /// Write metadata of all articles to stdout in json format.
    Dump,
    /// Load metadata of articles from stdin.
    Load,
}

#[derive(Args)]
struct Filters {
    /// Find articles with these ids.
    #[arg(long)]
    id: Vec<ArxivId>,
    /// Also include non-bookmarked articles.
    #[arg(short, long, conflicts_with = "id")]
    non_bookmarked: bool,
    /// Find articles containing these strings in the title.
    #[arg(short, long, conflicts_with = "id", value_hint = clap::ValueHint::Other)]
    title: Vec<String>,
    /// Find articles with these authors.
    #[arg(short, long, conflicts_with = "id", value_hint = clap::ValueHint::Other)]
    author: Vec<String>,
    /// Find articles containing these strings in the notes.
    #[arg(long, conflicts_with = "id", value_hint = clap::ValueHint::Other)]
    notes: Vec<String>,
    /// Find articles containing these words in the title, abstract, authors, notes, ...
    #[arg(conflicts_with = "id", value_hint = clap::ValueHint::Other)]
    word: Vec<String>,
    /// Find bookmarked articles satisfying these conditions.
    ///
    /// You can use the following types of conditions and combine them with logical operators: && || !
    /// Strings can be unquoted or quoted with '' or "".
    ///
    ///   id id1 id2 ...
    ///       matches articles with the given arXiv identifiers
    ///
    ///   primary_category math.NT
    ///       matches articles with primary category math.NT
    ///
    ///   category math.NT
    ///       matches articles with primary or secondary (cross-list) category math.NT
    ///
    ///   first_version_encountered_after 2025-10-01
    ///       matches articles that were first downloaded on or after 2025-10-01 with `arxiv-reader pull`
    ///
    ///   first_version_submitted_after 2025-10-01
    ///       matches articles that were first submitted on or after 2025-10-01
    ///
    ///   title word1 word2 ...
    ///       matches articles whose title contains the given strings (case-insensitive)
    ///
    ///   author name1 name2 ...
    ///       matches articles whose authors include the given names
    ///       Note:
    ///         The same author may sometimes be referred to in different ways, such as "C. F. Gauss", "Gauss, Carl-Friedrich", ...
    ///         The search is literal, so you might have to specify different spellings.
    ///         Accents are latex encoded. Remember to escape quotes and backslashes.
    ///
    ///   acm 11R32
    ///       matches articles with this acm class
    ///
    ///   msc 11R32
    ///       matches articles with this msc class
    ///
    ///   abstract word1 word2 ...
    ///       matches articles whose abstract contains the given strings (case-insensitive)
    ///
    ///   comments word1 word2 ...
    ///       matches articles whose comments contain the given strings (case-insensitive)
    ///
    ///   bookmarked
    ///       matches bookmarked articles
    ///
    ///   seen
    ///       matches articles marked as seen by `arxiv-reader news`
    ///
    ///   notes word1 word2 ...
    ///       matches articles whose notes contain the given strings (case-insensitive)
    #[arg(short, long, conflicts_with = "id", value_hint = clap::ValueHint::Other, verbatim_doc_comment)]
    filter: Option<Filter>,
}

impl Filters {
    fn get(self) -> Filter {
        if self.id.is_empty() {
            let mut res = Filter::True;
            if !self.non_bookmarked {
                res = Filter::And(Box::new(res), Box::new(Filter::Bookmarked));
            }
            if let Some(filter) = self.filter {
                res = Filter::And(Box::new(res), Box::new(filter));
            }
            for w in self.title {
                res = Filter::And(Box::new(res), Box::new(Filter::Title(w)));
            }
            for w in self.author {
                res = Filter::And(Box::new(res), Box::new(Filter::Author(w)));
            }
            for w in self.notes {
                res = Filter::And(Box::new(res), Box::new(Filter::Notes(w)));
            }
            for w in self.word {
                res = Filter::And(Box::new(res), Box::new(Filter::Any(w)));
            }
            res
        } else {
            let mut res = Filter::False;
            for id in self.id {
                res = Filter::Or(Box::new(res), Box::new(Filter::Id(id.to_string())));
            }
            res
        }
    }
}

#[derive(ValueEnum, Copy, Clone)]
pub enum Order {
    /// By the date of submission of the first version.
    Date,
    /// In the order in which the user first saw them.
    Seen,
}

#[derive(ValueEnum, Copy, Clone)]
pub enum LsFormat {
    /// Print their arXiv ids.
    Quiet,
    /// Print one line per article.
    OneLine,
    /// Print two lines per article.
    Short,
    /// Interactively show one article at a time.
    Int,
    /// Open the pdf (if there is only one matching article).
    Pdf,
    /// Open the directory (if there is only one matching article).
    Dir,
    /// Open the webpage (if there is only one matching article).
    Web,
}

fn main() -> anyhow::Result<()> {
    let res = inner_main();
    // Termion does not flush stdout by itself after returning to the main screen.
    // This is needed to ensure that error messages printed on stderr will be displayed.
    stdout().flush()?;
    res
}

fn inner_main() -> anyhow::Result<()> {
    let get_base_dir = || -> anyhow::Result<_> {
        let base_dir = match std::env::var_os("ARXIV_READER_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => PathBuf::from(std::env::var_os("HOME").unwrap()).join("arxiv-reader"),
        };
        Ok(base_dir)
    };

    let prepare = || -> anyhow::Result<_> {
        let base_dir = get_base_dir()?;

        let config_file = base_dir.join("config.toml");
        let config = std::fs::read_to_string(&config_file)
            .with_context(|| format!("reading {config_file:?}"))?;
        let config: Config =
            toml::from_str(&config).with_context(|| format!("parsing {config_file:?}"))?;

        let client = Client::new();
        Ok((base_dir, config, client))
    };

    let run_push_command = |base_dir: &Path, config: &Config| {
        // Run the push command.
        if let Some(push) = &config.hooks.push {
            println!("Running push command");
            let status = Command::new("/usr/bin/bash")
                .arg("-c")
                .arg(push)
                .current_dir(base_dir)
                .status()?;
            if !status.success() {
                bail!("push failed");
            }
        }
        Ok(())
    };

    let cli = Cli::parse();

    match cli.command {
        Commands::Pull => {
            let (base_dir, config, mut client) = prepare()?;
            let mut conn = db::open(&base_dir)?;
            // Upgrade the database version before making any requests.
            // This could also be done later, but it makes sense to me to do
            // it before making the first request.
            db::with_transaction(&mut conn, |_| Ok(()))?;
            // Run the pre-pull command.
            if let Some(pre_pull) = &config.hooks.pre_pull {
                println!("Running pre-pull command");
                let status = Command::new("/usr/bin/bash")
                    .arg("-c")
                    .arg(pre_pull)
                    .current_dir(&base_dir)
                    .status()?;
                if !status.success() {
                    bail!("pre-pull command failed");
                }
            }
            // Update article metadata.
            for categories in config.categories {
                println!("Getting records in category {categories}.");
                oai::download_changes(&base_dir, &mut conn, &categories, &mut client)?;
            }
            // Download pdfs and sources for all bookmarked articles.
            db::with_transaction(&mut conn, |tr| {
                let articles = Article::load(&base_dir, &tr)?;
                for article in articles.values() {
                    if article.is_bookmarked() {
                        if article.last_version().probably_has_pdf() {
                            article.download_pdf(&base_dir, &mut client)?;
                        }
                        if article.last_version().probably_has_src() {
                            article.download_src(&base_dir, &mut client)?;
                        }
                    }
                }
                Ok(())
            })?;
        }
        Commands::Find {
            filters,
            sort_by,
            show: do_,
        } => {
            let (base_dir, config, mut client) = prepare()?;
            db::with_transaction(&mut db::open(&base_dir)?, |conn| {
                let mut filter = filters.get();
                if let Order::Seen = sort_by {
                    filter = Filter::And(Box::new(filter), Box::new(Filter::Seen));
                }
                if let LsFormat::Int = do_ {
                    interact::interact(
                        &base_dir,
                        &conn,
                        &Highlight::default(),
                        &mut client,
                        &filter,
                        None,
                        sort_by,
                    )?;
                    // Run the push command in case some article's state was changed.
                    run_push_command(&base_dir, &config)?;
                } else {
                    let articles = Article::load(&base_dir, &conn)?;
                    // All articles matching the filters.
                    let mut articles: Vec<Article> = articles
                        .into_values()
                        .filter(|a| filter.matches(a))
                        .collect();
                    match sort_by {
                        Order::Date => {
                            articles.sort_by_key(|a| a.first_version().date);
                        }
                        Order::Seen => {
                            articles.sort_by_key(|a| a.last_seen_at());
                        }
                    }
                    fn short(articles: &[Article]) {
                        for article in articles.iter() {
                            println!("{}  {}", article.id(), article.authors());
                            println!("{}", article.title());
                            println!();
                        }
                    }
                    fn do_for_one(
                        articles: &[Article],
                        f: impl FnOnce(&Article) -> anyhow::Result<()>,
                    ) -> anyhow::Result<()> {
                        if articles.len() == 1 {
                            f(&articles[0])
                        } else if articles.is_empty() {
                            println!("No articles found.");
                            Ok(())
                        } else {
                            println!(
                                "Found {} articles. Please make a more specific search.",
                                articles.len()
                            );
                            println!();
                            short(articles);
                            Ok(())
                        }
                    }
                    match do_ {
                        LsFormat::Quiet => {
                            for article in articles.iter() {
                                println!("{}", article.id());
                            }
                        }
                        LsFormat::OneLine => {
                            for article in articles.iter() {
                                println!(
                                    "{} {}: {}",
                                    article.id(),
                                    article.authors(),
                                    article.title()
                                );
                            }
                        }
                        LsFormat::Short => {
                            short(&articles);
                        }
                        LsFormat::Int => panic!("logic error"),
                        LsFormat::Pdf => {
                            do_for_one(&articles, |article| {
                                article.download_pdf(&base_dir, &mut client)?;
                                article.open_pdf(&base_dir)
                            })?;
                        }
                        LsFormat::Dir => {
                            do_for_one(&articles, |article| article.open_dir(&base_dir))?;
                        }
                        LsFormat::Web => {
                            do_for_one(&articles, |article| article.open_abs())?;
                        }
                    }
                }
                Ok(())
            })?
        }
        Commands::News { sort_by } => {
            let (base_dir, config, mut client) = prepare()?;
            db::with_transaction(&mut db::open(&base_dir)?, |conn| {
                interact::interact(
                    &base_dir,
                    &conn,
                    &config.highlight,
                    &mut client,
                    &config.filters.new,
                    Some(&config.filters.update),
                    sort_by,
                )
            })?;
            // Run the push command in case some article's state was changed.
            run_push_command(&base_dir, &config)?;
        }
        Commands::Bibtex(cmd) => match cmd {
            BibtexCommand::Bookmark { file } => {
                let (base_dir, _config, _client) = prepare()?;
                db::with_transaction(&mut db::open(&base_dir)?, |conn| {
                    bibtex::bookmark(&base_dir, &conn, &file)
                })?
            }
            BibtexCommand::Check { file } => {
                let (base_dir, _config, _client) = prepare()?;
                db::with_transaction(&mut db::open(&base_dir)?, |conn| {
                    bibtex::check(&base_dir, &conn, &file)
                })?
            }
        },
        Commands::Init => {
            let base_dir = get_base_dir()?;

            if !base_dir.is_dir() {
                bail!("{:?} is not a directory.", base_dir);
            }

            // Create $BASE_DIR/articles and $BASE_DIR/bookmarks if necessary.
            let dir = base_dir.join("articles");
            create_dir(&dir).with_context(|| format!("creating {dir:?}"))?;
            let dir = base_dir.join("bookmarks");
            create_dir(&dir).with_context(|| format!("creating {dir:?}"))?;

            // Create the sample config file.
            let config_filename = base_dir.join("config.toml");
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&config_filename)
                .with_context(|| format!("opening {config_filename:?}"))?;
            write!(file, include_str!("sample/config.toml"))
                .with_context(|| format!("writing {config_filename:?}"))?;

            // Create the .gitignore file.
            let gitignore_filename = base_dir.join(".gitignore");
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&gitignore_filename)
                .with_context(|| format!("opening {gitignore_filename:?}"))?;
            write!(file, include_str!("sample/.gitignore"))
                .with_context(|| format!("writing {gitignore_filename:?}"))?;

            // Create the database.
            db::create(&base_dir)?;

            println!("Now, please edit the configuration file at {config_filename:?}.");
            println!();
            println!(
                "Then, run `arxiv-reader pull` to download articles from the specified categories."
            );
            println!(
                "Look at new articles with `arxiv-reader news` and find articles with `arxiv-reader find`."
            );
            println!("Run `arxiv-reader help` for more information.");
        }
        Commands::Database(cmd) => match cmd {
            DatabaseCommand::Dump => {
                let (base_dir, _config, _client) = prepare()?;
                db::with_transaction(&mut db::open(&base_dir)?, |conn| db::dump(&conn))?;
            }
            DatabaseCommand::Load => {
                let (base_dir, _config, _client) = prepare()?;
                db::with_write_transaction(&mut db::open(&base_dir)?, db::load)?;
            }
        },
        Commands::GenerateCompletions { generator } => {
            clap_complete::generate(
                generator,
                &mut Cli::command(),
                "arxiv-reader",
                &mut std::io::stdout(),
            );
        }
    }
    Ok(())
}
