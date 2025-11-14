use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read, Write, stdin, stdout},
    path::Path,
};

use anyhow::{Context, bail};
use biblatex::{Bibliography, Chunk};
use rusqlite::Transaction;

use crate::{
    article::{Article, ArxivId},
    config::TagName,
};

pub fn bookmark(
    base_dir: &Path,
    conn: &Transaction,
    file: &Path,
    tag_name: &TagName,
) -> anyhow::Result<()> {
    // Parse the BibTeX file.
    let file = File::open(file).context("opening bibtex file")?;
    let mut reader = BufReader::new(file);
    let mut s = String::new();
    reader
        .read_to_string(&mut s)
        .context("reading bibtex file")?;
    let bib = Bibliography::parse(&s).context("parsing bibtex")?;
    let arxiv_chunk = Chunk::Normal("arXiv".to_string());

    // Load the articles.
    let mut articles = Article::load(base_dir, conn)?;

    // Map dois to arxiv ids.
    let mut by_doi: HashMap<String, Vec<ArxivId>> = HashMap::new();
    for article in articles.values() {
        if let Some(doi) = article.doi() {
            by_doi
                .entry(doi.clone())
                .or_default()
                .push(article.id().clone());
        }
    }

    // Go through entries in the bibtex file.
    for entry in bib.iter() {
        // Extract the key and make sure it is filename safe.
        let key = &entry.key;
        if !key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            bail!("invalid key of bibtex entry: {key:?}");
        }
        if let Ok(type_) = entry.eprint_type()
            && type_.len() == 1
            && type_[0].v == arxiv_chunk
        {
            // If it's an arXiv entry, look for it by id.
            let id = entry
                .eprint()
                .with_context(|| format!("reading bibtex entry {key}"))?;
            let (id, _) = ArxivId::parse_with_version(&id)
                .with_context(|| format!("reading bibtex entry {key}"))?;
            let article = articles.get_mut(&id);
            // If we know the article and haven't bookmarked it under this name,
            // create a bookmark.
            if let Some(article) = article {
                if !article.tags().contains(tag_name) {
                    println!("Adding bookmark for {id}.");
                    article.set_tag(base_dir, tag_name)?;
                    println!();
                }
            } else {
                println!("Article {id} not found.");
                println!();
            }
        } else if let Ok(doi) = entry.doi() {
            // If the entry has a DOI, try to identify the article that way.
            // This doesn't seem to be entirely reliable.
            // The doi doesn't always link to the published version of the arxiv preprint.
            // Sometimes, there are even multiple preprints with the same related doi.
            let ids = by_doi.get(&doi).cloned().unwrap_or_default();
            let authors: Vec<String> = entry
                .author()
                .with_context(|| format!("reading bibtex entry {key}"))?
                .iter()
                .map(|a| format!("{}", a))
                .collect();
            // If we know articles with this doi and haven't bookmarked any of them
            // under this name, ask for confirmation and then create a bookmark.
            if !ids.is_empty()
                && !ids
                    .iter()
                    .any(|id| articles.get(id).unwrap().tags().contains(tag_name))
            {
                println!("Article https://doi.org/{doi}");
                println!("  by {}", authors.join(" and "));
                let title: Vec<String> = entry
                    .title()
                    .with_context(|| format!("reading bibtex entry {key}"))?
                    .iter()
                    .map(|c| c.v.to_biblatex_string(false))
                    .collect();
                println!("  titled {}", title.join(""));
                println!("could be:");
                for (i, id) in ids.iter().enumerate() {
                    println!("[{}] {id}", i + 1);
                    let article = articles.get(id).unwrap();
                    println!("  by {}", article.authors());
                    println!("  titled {}", article.title());
                }
                let i = loop {
                    print!("Please select one (0 means none): ");
                    stdout().flush()?;
                    let mut response = String::new();
                    stdin().read_line(&mut response)?;
                    let i: Result<usize, _> = response.trim().parse();
                    if let Ok(i) = i
                        && i <= ids.len()
                    {
                        break i;
                    } else {
                        println!("Not a number between 0 and {}", ids.len());
                    }
                };
                if i > 0 {
                    let id = ids.get(i - 1).unwrap();
                    let article = articles.get_mut(id).unwrap();
                    println!("Adding bookmark named {key} for {id}.");
                    article.set_tag(base_dir, tag_name)?;
                }
                println!();
            }
        }
    }
    Ok(())
}

pub fn check(base_dir: &Path, conn: &Transaction, file: &Path) -> anyhow::Result<()> {
    // Parse the BibTeX file.
    let file = File::open(file).context("opening bibtex file")?;
    let mut reader = BufReader::new(file);
    let mut s = String::new();
    reader
        .read_to_string(&mut s)
        .context("reading bibtex file")?;
    let bib = Bibliography::parse(&s).context("parsing bibtex")?;
    let arxiv_chunk = Chunk::Normal("arXiv".to_string());

    // Load the articles.
    let mut articles = Article::load(base_dir, conn)?;

    // Go through entries in the bibtex file.
    for entry in bib.iter() {
        // Extract the key.
        let key = &entry.key;
        if let Ok(type_) = entry.eprint_type()
            && type_.len() == 1
            && type_[0].v == arxiv_chunk
        {
            // If it's an arXiv entry, look for it by id.
            let id = entry
                .eprint()
                .with_context(|| format!("reading bibtex entry {key}"))?;
            let (id, version) = ArxivId::parse_with_version(&id)
                .with_context(|| format!("reading bibtex entry {key}"))?;
            let article = articles.get_mut(&id);
            if let Some(article) = article {
                // If there is a newer version, tell the user.
                if let Some(version) = version
                    && article.last_version().number > version
                {
                    println!(
                        "Entry {key} refers to {id}, version {version}, but there is a newer version {}",
                        article.last_version().number
                    );
                }
                // If the article has an associated doi, tell the user.
                if article.journal_ref().is_some() {
                    println!("Entry {key} refers to {id}, which seems to have been published:");
                    if let Some(journal_ref) = article.journal_ref() {
                        println!("  Journal ref: {}", journal_ref);
                    }
                    if let Some(doi) = article.doi() {
                        println!("  DOI: https://doi.org/{}", doi)
                    }
                    println!();
                }
            } else {
                println!("Article {id} not found.");
                println!();
            }
        }
    }
    Ok(())
}
