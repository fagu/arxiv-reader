use std::{
    collections::{BTreeSet, HashMap},
    ffi::OsStr,
    fmt::Display,
    fs::{File, create_dir},
    io::{BufRead, BufReader, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

use anyhow::{Context, bail};
use chrono::{DateTime, FixedOffset};
use reqwest::header::HeaderValue;
use rusqlite::{Row, Transaction, params};
use serde::{Deserialize, Serialize};

use crate::{
    config::{Highlight, TagName},
    rate_limited_client::Client,
    util::{highlight_matches, read_if_exists, write_then_rename},
};

/// Article metadata as received from arXiv.
#[derive(Serialize, Deserialize)]
pub struct ArticleMetadata {
    pub id: ArxivId,
    pub submitter: String,
    pub versions: Vec<Version>,
    pub title: String,
    pub authors: String,
    // See https://arxiv.org/archive for a list of categories.
    pub categories: Vec<String>,
    pub comments: Option<String>,
    pub proxy: Option<String>,
    pub report_no: Option<String>,
    pub acm_classes: Option<String>,
    pub msc_classes: Option<String>,
    pub journal_ref: Option<String>,
    pub doi: Option<String>,
    pub license: Option<String>,
    #[serde(rename = "abstract")]
    pub abstract_: String,
    pub last_change: Option<String>,
    pub sets: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Hash, PartialEq, Eq, Clone)]
pub struct ArxivId(String);

impl FromStr for ArxivId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let valid_first_chars = |c: char| c.is_ascii_digit() || c.is_ascii_lowercase();
        let valid_chars = |c: char| {
            c.is_ascii_digit() || c == '.' || c.is_ascii_lowercase() || c == '/' || c == '-'
        };
        if s.len() < 20
            && s.chars().next().is_some_and(valid_first_chars)
            && s.chars().all(valid_chars)
        {
            Ok(Self(s.to_string()))
        } else {
            bail!("invalid arXiv identifier: {:?}", s)
        }
    }
}

impl ArxivId {
    /// Parse arXiv id with an optional version specifier, such as "1234.56789" or "1234.56789v3".
    pub fn parse_with_version(s: &str) -> anyhow::Result<(ArxivId, Option<u32>)> {
        if let Some((a, b)) = s.split_once('v') {
            Ok((a.parse()?, Some(b.parse()?)))
        } else {
            Ok((s.parse()?, None))
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::article::ArxivId;

    #[test]
    fn bla() {
        assert!(ArxivId::from_str("1234.56789").is_ok());
        assert!(ArxivId::from_str("math/123456").is_ok());
        assert!(ArxivId::from_str("").is_err());
        assert!(ArxivId::from_str(".").is_err());
        assert!(ArxivId::from_str("ä").is_err());
        assert!(ArxivId::from_str("12345678901234567890").is_err());
    }
}

impl Display for ArxivId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'de> Deserialize<'de> for ArxivId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl ArxivId {
    /// Sanitizes the id so that it can be used as a directory name.
    pub fn dir_name(&self) -> String {
        self.0.replace('/', "_")
    }
    /// The inverse of `dir_name`.
    pub fn from_dir_name(s: &str) -> Option<ArxivId> {
        s.replace('_', "/").parse().ok()
    }
    pub fn from_os_dir_name(s: &OsStr) -> Option<ArxivId> {
        s.to_str().and_then(ArxivId::from_dir_name)
    }
    /// The data directory for this id.
    pub fn directory(&self, base_dir: &Path) -> PathBuf {
        base_dir.join("articles").join(self.dir_name())
    }
    /// Create the article directory if it doesn't exist.
    pub fn mkdir(&self, base_dir: &Path) -> anyhow::Result<()> {
        let path = self.directory(base_dir);
        if !path.is_dir() {
            create_dir(&path).with_context(|| format!("creating {path:?}"))?;
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Version {
    pub number: u32,
    /// The date and time this version was submitted.
    pub date: DateTime<FixedOffset>,
    pub size: String,
    /// Conjectured meanings:
    /// None: normal
    /// Some("A"): some sources available, but maybe no main tex code
    /// Some("AD"): some sources available, but no main tex code
    /// Some("AP"): source is only pdf?
    /// Some("ASD"): ?
    /// Some("AS"): ?
    /// Some("D"): some sources available, but no main tex code
    /// Some("X"): DOCX file
    /// Some("S"): not public
    /// Some("SD"): not public and something else...
    /// Some("P"): also pdf file
    /// Some("H"): html file, probably no pdf available
    /// Some("I"): withdrawn (cf. https://groups.google.com/g/arxiv-api/c/Yda1lMACYzw)
    pub source_type: Option<String>,
    /// The first response_date at which this version was encountered.
    pub first_encounter: String,
}

impl ArticleMetadata {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.versions.is_empty() {
            bail!("article has no versions");
        }
        if self.categories.is_empty() {
            bail!("article has no categories");
        }
        for i in 0..self.versions.len() - 1 {
            let version = &self.versions[i];
            if version.number as usize != i + 1 {
                bail!("unexpected version number");
            }
        }
        Ok(())
    }

    pub fn first_version(&self) -> &Version {
        self.versions.first().unwrap()
    }

    pub fn last_version(&self) -> &Version {
        self.versions.last().unwrap()
    }

    pub fn from_row(row: &Row) -> anyhow::Result<ArticleMetadata> {
        let id: String = row.get(0)?;
        let id: ArxivId = id.parse().context("parsing id")?;
        let submitter = row.get(1)?;
        let versions: String = row.get(2)?;
        let versions = serde_json::from_str(&versions).context("parsing version")?;
        let title = row.get(3)?;
        let authors = row.get(4)?;
        let categories: String = row.get(5)?;
        let categories = serde_json::from_str(&categories).context("parsing categories")?;
        let comments = row.get(6)?;
        let proxy = row.get(7)?;
        let report_no = row.get(8)?;
        let acm_classes = row.get(9)?;
        let msc_classes = row.get(10)?;
        let journal_ref = row.get(11)?;
        let doi = row.get(12)?;
        let license = row.get(13)?;
        let abstract_ = row.get(14)?;
        let last_change = row.get(15)?;
        let sets: Option<String> = row.get(16)?;
        let sets = sets
            .map(|sets| serde_json::from_str(&sets).context("parsing sets"))
            .transpose()?;
        let metadata = ArticleMetadata {
            id,
            submitter,
            versions,
            title,
            authors,
            categories,
            comments,
            proxy,
            report_no,
            acm_classes,
            msc_classes,
            journal_ref,
            doi,
            license,
            abstract_,
            last_change,
            sets,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    /// Loads from the sqlite database a list of all articles.
    pub fn load(tr: &Transaction) -> anyhow::Result<HashMap<ArxivId, ArticleMetadata>> {
        let mut metadatas = HashMap::new();
        let mut get = tr.prepare("SELECT id, submitter, versions, title, authors, categories, comments, proxy, report_no, acm_classes, msc_classes, journal_ref, doi, license, abstract, last_change, sets FROM article")?;
        let mut rows = get.query([])?;
        while let Some(row) = rows.next()? {
            let metadata = ArticleMetadata::from_row(row)?;
            metadatas.insert(metadata.id.clone(), metadata);
        }
        Ok(metadatas)
    }

    /// Loads from the sqlite database a single article.
    pub fn load_one(tr: &Transaction, id: &ArxivId) -> anyhow::Result<Option<ArticleMetadata>> {
        let mut get = tr.prepare_cached("SELECT id, submitter, versions, title, authors, categories, comments, proxy, report_no, acm_classes, msc_classes, journal_ref, doi, license, abstract, last_change, sets FROM article WHERE id = ?1")?;
        let mut rows = get.query([id.to_string()])?;
        let row = rows.next()?;
        match row {
            Some(row) => {
                let metadata = ArticleMetadata::from_row(row)?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    pub fn write(&self, tr: &Transaction) -> anyhow::Result<()> {
        let mut get = tr.prepare_cached("INSERT OR REPLACE INTO article (id, submitter, versions, title, authors, categories, comments, proxy, report_no, acm_classes, msc_classes, journal_ref, doi, license, abstract, last_change, sets) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)")?;
        get.execute(params![
            self.id.to_string(),
            self.submitter,
            serde_json::to_string(&self.versions)?,
            self.title,
            self.authors,
            serde_json::to_string(&self.categories)?,
            self.comments,
            self.proxy,
            self.report_no,
            self.acm_classes,
            self.msc_classes,
            self.journal_ref,
            self.doi,
            self.license,
            self.abstract_,
            self.last_change,
            serde_json::to_string(&self.sets)?,
        ])?;
        Ok(())
    }
}

impl Version {
    pub fn probably_withdrawn(&self) -> bool {
        // See https://groups.google.com/g/arxiv-api/c/Yda1lMACYzw
        // However, there are also withdrawn article versions with source_type == None and size == 0kb.
        // There are also withdrawn article versions with source_type == Some("I") and size != 0kb.
        self.source_type.as_deref() == Some("I") || &self.size == "0kb"
    }

    pub fn probably_src_secret(&self) -> bool {
        self.source_type
            .as_ref()
            .is_some_and(|t| t.starts_with("S"))
    }

    pub fn probably_has_pdf(&self) -> bool {
        !self.probably_withdrawn() && self.source_type.as_deref() != Some("H")
    }

    pub fn probably_has_src(&self) -> bool {
        !self.probably_withdrawn() && !self.probably_src_secret()
    }
}

pub struct ArticleState {
    last_seen_at: usize,
    last_seen_version: u32,
    seen_journal: bool,
    seen_doi: bool,
    /// The names of the bookmark symlinks, relative to the tag directory.
    tags: BTreeSet<TagName>,
    notes: Option<String>,
}

impl ArticleState {
    fn new() -> Self {
        Self {
            last_seen_at: 0,
            last_seen_version: 0,
            seen_journal: false,
            seen_doi: false,
            tags: BTreeSet::new(),
            notes: None,
        }
    }

    fn get_tags(base_dir: &Path, id: &ArxivId) -> anyhow::Result<BTreeSet<TagName>> {
        read_if_exists(id.directory(base_dir).join("tags"), |reader| {
            let mut res = BTreeSet::new();
            for line in reader.lines() {
                let line = line?;
                let tag: TagName = line.parse()?;
                res.insert(tag);
            }
            Ok(res)
        })
        .map(|r| r.unwrap_or_default())
        .with_context(|| format!("reading tags for {}", id))
    }

    fn get_notes(base_dir: &Path, id: &ArxivId) -> anyhow::Result<Option<String>> {
        read_if_exists(id.directory(base_dir).join("notes.txt"), |reader| {
            let mut res = String::new();
            reader.read_to_string(&mut res)?;
            Ok(res)
        })
        .with_context(|| format!("reading notes.txt for {}", id))
    }
}

pub struct Article {
    pub metadata: ArticleMetadata,
    pub state: ArticleState,
}

impl Article {
    pub fn id(&self) -> &ArxivId {
        &self.metadata.id
    }

    #[allow(unused)]
    pub fn submitter(&self) -> &String {
        &self.metadata.submitter
    }

    pub fn versions(&self) -> &Vec<Version> {
        &self.metadata.versions
    }

    pub fn first_version(&self) -> &Version {
        self.metadata.first_version()
    }

    pub fn last_version(&self) -> &Version {
        self.metadata.last_version()
    }

    pub fn title(&self) -> &String {
        &self.metadata.title
    }

    pub fn authors(&self) -> &String {
        &self.metadata.authors
    }

    pub fn categories(&self) -> &Vec<String> {
        &self.metadata.categories
    }

    pub fn primary_category(&self) -> &String {
        self.categories().first().unwrap()
    }

    pub fn comments(&self) -> Option<&String> {
        self.metadata.comments.as_ref()
    }

    #[allow(unused)]
    pub fn proxy(&self) -> Option<&String> {
        self.metadata.proxy.as_ref()
    }

    #[allow(unused)]
    pub fn report_no(&self) -> Option<&String> {
        self.metadata.report_no.as_ref()
    }

    pub fn acm_classes(&self) -> Option<&String> {
        self.metadata.acm_classes.as_ref()
    }

    pub fn msc_classes(&self) -> Option<&String> {
        self.metadata.msc_classes.as_ref()
    }

    pub fn journal_ref(&self) -> Option<&String> {
        self.metadata.journal_ref.as_ref()
    }

    pub fn doi(&self) -> Option<&String> {
        self.metadata.doi.as_ref()
    }

    #[allow(unused)]
    pub fn license(&self) -> Option<&String> {
        self.metadata.license.as_ref()
    }

    pub fn abstract_(&self) -> &String {
        &self.metadata.abstract_
    }

    pub fn last_seen_version(&self) -> u32 {
        self.state.last_seen_version
    }

    pub fn seen_journal(&self) -> bool {
        self.state.seen_journal
    }

    pub fn seen_doi(&self) -> bool {
        self.state.seen_doi
    }

    pub fn last_seen_at(&self) -> usize {
        self.state.last_seen_at
    }

    pub fn is_bookmarked(&self) -> bool {
        !self.state.tags.is_empty()
    }

    pub fn tags(&self) -> &BTreeSet<TagName> {
        &self.state.tags
    }

    pub fn notes(&self) -> Option<&String> {
        self.state.notes.as_ref()
    }

    fn load_state(
        base_dir: &Path,
        metadatas: HashMap<ArxivId, ArticleMetadata>,
    ) -> anyhow::Result<HashMap<ArxivId, Article>> {
        let mut articles: HashMap<ArxivId, Article> = HashMap::new();
        for (id, metadata) in metadatas.into_iter() {
            let state = ArticleState::new();
            articles.insert(id, Article { metadata, state });
        }

        // Read list of seen articles.
        match File::open(base_dir.join("seen-articles")) {
            Ok(file) => {
                let reader = BufReader::new(file);
                for (linenr, line) in reader.lines().enumerate() {
                    let line = line.context("reading seen-articles")?;
                    let mut parts = line.split(' ');
                    let id = parts.next().context("missing id in seen-articles")?;
                    let id: ArxivId = id
                        .parse()
                        .with_context(|| format!("invalid id in seen-articles: {id:?}"))?;
                    let version = parts.next().context("missing version in seen-articles")?;
                    let version = version.parse().with_context(|| {
                        format!("invalid version in seen-articles: {version:?}")
                    })?;
                    let journal = parts.next() == Some("true");
                    let doi = parts.next() == Some("true");
                    if parts.next().is_some() {
                        bail!("too many columns in seen-articles");
                    }
                    // Ignore if there is an unknown article id. (It might have been deleted from the file system.)
                    if let Some(article) = articles.get_mut(&id) {
                        article.state.last_seen_at = linenr;
                        if article.state.last_seen_version < version {
                            article.state.last_seen_version = version;
                        }
                        if journal {
                            article.state.seen_journal = true;
                        }
                        if doi {
                            article.state.seen_doi = true;
                        }
                    }
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                } else {
                    Err(err).context("reading seen-articles")?
                }
            }
        }

        // Read tags and notes. For efficiency, we don't try to load tags and notes for each article,
        // but only for those that have a directory.
        for dir_entry in
            std::fs::read_dir(base_dir.join("articles")).context("reading articles directory")?
        {
            let dir_entry = dir_entry.context("reading articles directory")?;
            let id = dir_entry.file_name();
            let id = ArxivId::from_os_dir_name(&id)
                .with_context(|| "invalid article directory: {id:?}")?;
            if let Some(article) = articles.get_mut(&id) {
                article.state.tags = ArticleState::get_tags(base_dir, &id)?;
                article.state.notes = ArticleState::get_notes(base_dir, &id)?;
            }
        }

        Ok(articles)
    }

    /// Loads from the sqlite database a list of all articles.
    pub fn load(base_dir: &Path, conn: &Transaction) -> anyhow::Result<HashMap<ArxivId, Article>> {
        // Read metadata of all articles.
        let metadatas = ArticleMetadata::load(conn)?;
        Self::load_state(base_dir, metadatas)
    }

    /// Loads from the sqlite database a single article.
    #[allow(unused)]
    pub fn load_one(base_dir: &Path, tr: &Transaction, id: &ArxivId) -> anyhow::Result<Article> {
        // Read metadata.
        let metadata = ArticleMetadata::load_one(tr, id)?
            .with_context(|| format!("found no article with id {}", id))?;
        let mut metadatas: HashMap<ArxivId, ArticleMetadata> = HashMap::new();
        metadatas.insert(id.clone(), metadata);
        Ok(Self::load_state(base_dir, metadatas)?.remove(id).unwrap())
    }

    pub fn mark_as_seen(&mut self, writer: &mut File) -> anyhow::Result<()> {
        if self.state.last_seen_version < self.metadata.last_version().number {
            self.state.last_seen_version = self.metadata.last_version().number;
        }
        if self.journal_ref().is_some() {
            self.state.seen_journal = true;
        }
        if self.doi().is_some() {
            self.state.seen_doi = true;
        }
        writeln!(
            writer,
            "{} {} {} {}",
            self.metadata.id,
            self.metadata.last_version().number,
            self.journal_ref().is_some(),
            self.doi().is_some(),
        )
        .context("writing seen-articles")?;
        writer.flush().context("writing seen-articles")?;
        Ok(())
    }

    fn write_tags(&self, base_dir: &Path) -> anyhow::Result<()> {
        let id = self.id();
        write_then_rename(id.directory(base_dir).join("tags"), |writer| {
            for tag in &self.state.tags {
                writeln!(writer, "{tag}").context("writing tag")?;
            }
            Ok(())
        })
        .with_context(|| format!("writing tags for {id}"))?;
        Ok(())
    }

    pub fn toggle_tag(&mut self, base_dir: &Path, tag_name: &TagName) -> anyhow::Result<()> {
        if self.state.tags.contains(tag_name) {
            self.state.tags.remove(tag_name);
        } else {
            self.state.tags.insert(tag_name.clone());
        }
        self.write_tags(base_dir)
    }

    pub fn set_tag(&mut self, base_dir: &Path, tag_name: &TagName) -> anyhow::Result<()> {
        if !self.state.tags.contains(tag_name) {
            self.state.tags.insert(tag_name.clone());
            self.write_tags(base_dir)?;
        }
        Ok(())
    }

    pub fn pdf_path(&self, base_dir: &Path) -> PathBuf {
        self.id()
            .directory(base_dir)
            .join(format!("v{}.pdf", self.last_version().number))
    }

    fn download_content(
        &self,
        client: &mut Client,
        path: PathBuf,
        description: &str,
        url_dir: &str,
        content_type: &'static str,
    ) -> anyhow::Result<()> {
        if !path.is_file() {
            println!(
                "Downloading {description} for {}v{}...",
                self.id(),
                self.last_version().number
            );
            // Download.
            let mut res = client.with(|client| {
                client
                    .get(format!(
                        "https://arxiv.org/{url_dir}/{}v{}",
                        self.id(),
                        self.last_version().number
                    ))
                    .send()
                    .and_then(|res| res.error_for_status())
                    .with_context(|| {
                        format!(
                            "requesting {description} from arXiv for {}v{}",
                            self.id(),
                            self.last_version().number
                        )
                    })
            })?;
            // Check content type.
            let res_content_type = res.headers().get("Content-Type");
            if res_content_type != Some(&HeaderValue::from_static(content_type)) {
                bail!(
                    "wrong content type (expected {content_type}, received {res_content_type:?})",
                );
            }
            // Write file.
            write_then_rename(path, |writer| {
                std::io::copy(&mut res, writer)?;
                Ok(())
            })
            .with_context(|| {
                format!(
                    "saving {description} from arXiv for {}v{}",
                    self.id(),
                    self.last_version().number
                )
            })?;
        }
        Ok(())
    }

    /// Download the pdf file if necessary.
    pub fn download_pdf(&self, base_dir: &Path, client: &mut Client) -> anyhow::Result<()> {
        self.id().mkdir(base_dir)?;
        self.download_content(
            client,
            self.pdf_path(base_dir),
            "pdf",
            "pdf",
            "application/pdf",
        )
    }

    pub fn src_path(&self, base_dir: &Path) -> PathBuf {
        self.id()
            .directory(base_dir)
            .join(format!("v{}.tar.gz", self.last_version().number))
    }

    /// Download the src file if necessary.
    pub fn download_src(&self, base_dir: &Path, client: &mut Client) -> anyhow::Result<()> {
        self.id().mkdir(base_dir)?;
        self.download_content(
            client,
            self.src_path(base_dir),
            "sources",
            "src",
            "application/gzip",
        )
    }

    /// Open the article's arXiv webpage.
    pub fn open_abs(&self) -> anyhow::Result<()> {
        let status = Command::new("xdg-open")
            .arg(format!("https://arxiv.org/abs/{}", self.id()))
            .output()?
            .status;
        if !status.success() {
            bail!("xdg-open failed");
        }
        Ok(())
    }

    /// Open the (previously downloaded) pdf file.
    pub fn open_pdf(&self, base_dir: &Path) -> anyhow::Result<()> {
        let status = Command::new("xdg-open")
            .arg(self.pdf_path(base_dir))
            .output()?
            .status;
        if !status.success() {
            bail!("xdg-open failed");
        }
        Ok(())
    }

    /// Open the data directory for this article.
    pub fn open_dir(&self, base_dir: &Path) -> anyhow::Result<()> {
        self.id().mkdir(base_dir)?;
        let status = Command::new("xdg-open")
            .arg(self.id().directory(base_dir))
            .output()?
            .status;
        if !status.success() {
            bail!("xdg-open failed");
        }
        Ok(())
    }

    pub fn notes_file(&self, base_dir: &Path) -> PathBuf {
        self.id().directory(base_dir).join("notes.txt")
    }

    /// Open notes file in the default editor.
    pub fn edit_notes(&mut self, base_dir: &Path) -> anyhow::Result<()> {
        self.id().mkdir(base_dir)?;
        let editor = std::env::var_os("EDITOR").unwrap_or_else(|| "vi".to_string().into());
        let status = Command::new(editor)
            .arg(self.notes_file(base_dir))
            .status()?;
        if !status.success() {
            bail!("editor failed");
        }
        self.state.notes = ArticleState::get_notes(base_dir, self.id())?;
        Ok(())
    }

    /// Prints article metadata, bookmarks, and notes.
    /// `show_updates` specifies whether we should highlight unseen versions, journal refs, etc.
    pub fn print(&self, highlight: &Highlight, show_updates: bool, latex_to_unicode: bool) {
        let bold_if_updated = |cond: bool, s: &str| {
            if cond && show_updates {
                println!(
                    "{}{}{}",
                    termion::color::LightRed.fg_str(),
                    s,
                    termion::color::Reset.fg_str()
                );
            } else {
                println!("{}", s);
            }
        };

        let to_unicode = |text: &str| -> String {
            if latex_to_unicode {
                unicodeit::replace(text)
            } else {
                text.to_string()
            }
        };

        println!("{}", self.id());
        for version in self.versions() {
            let mut line = format!(
                "Date (v{}): {}",
                version.number,
                version.date.format("%Y-%m-%d %H:%M %Z")
            );
            if version.probably_withdrawn() {
                line += " (withdrawn?)";
            }
            bold_if_updated(version.number > self.last_seen_version(), &line);
        }
        println!();
        println!(
            "Title: {}",
            highlight_matches(&to_unicode(self.title()), true, &highlight.keywords)
        );
        println!(
            "Authors: {}",
            highlight_matches(&to_unicode(self.authors()), false, &highlight.authors)
        );
        println!(
            "Categories: {}",
            self.categories()
                .iter()
                .map(|c| if highlight.categories.contains(c) {
                    format!(
                        "{}{}{}",
                        termion::color::LightRed.fg_str(),
                        c,
                        termion::color::Reset.fg_str()
                    )
                } else {
                    c.to_string()
                })
                .collect::<Vec<_>>()
                .join(" ")
        );
        if let Some(comments) = self.comments() {
            println!(
                "Comments: {}",
                highlight_matches(&to_unicode(comments), true, &highlight.keywords)
            );
        }
        if let Some(acm_classes) = self.acm_classes() {
            println!(
                "ACM-class: {}",
                highlight_matches(acm_classes, false, &highlight.acm_classes)
            );
        }
        if let Some(msc_classes) = self.msc_classes() {
            println!(
                "MSC-class: {}",
                highlight_matches(msc_classes, false, &highlight.msc_classes)
            );
        }
        if let Some(journal_ref) = self.journal_ref() {
            bold_if_updated(
                !self.seen_journal(),
                &format!("Journal ref: {}", journal_ref),
            );
        }
        if let Some(doi) = self.doi() {
            bold_if_updated(!self.seen_doi(), &format!("DOI: https://doi.org/{}", doi));
        }
        println!();
        println!(
            "{}",
            highlight_matches(&to_unicode(self.abstract_()), true, &highlight.keywords)
        );
        println!();
        println!("------------------------------------------------------------------");
        for tag_name in self.tags() {
            println!("Tag: {tag_name}");
        }
        println!();
        if let Some(notes) = self.notes() {
            println!("{}", notes);
        }
    }
}
