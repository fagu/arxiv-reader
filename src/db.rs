use std::{
    collections::HashMap,
    io::{stdin, stdout},
    path::Path,
};

use anyhow::{Context, bail};
use rusqlite::{Connection, Row, Transaction, params};
use serde::{Deserialize, Serialize};

use crate::{article::ArticleMetadata, oai::Continuation};

pub fn open(base_dir: &Path) -> anyhow::Result<Connection> {
    let db_path = base_dir.join("db.sqlite");
    if !db_path.exists() {
        bail!("database file {db_path:?} does not exist");
    }
    Connection::open(db_path.clone()).context("could not open sqlite database")
}

pub fn create(base_dir: &Path) -> anyhow::Result<()> {
    let db_path = base_dir.join("db.sqlite");
    if db_path.exists() {
        bail!("database file {db_path:?} already exists");
    }
    let mut conn = Connection::open(db_path.clone()).context("could not open sqlite database")?;
    // Create the database with schema version 1.
    let tr = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    tr.execute("CREATE TABLE db_version (version TEXT)", ())?;
    tr.execute(
            "CREATE TABLE article (id TEXT PRIMARY KEY, submitter TEXT, versions TEXT, title TEXT, authors TEXT, categories TEXT, comments TEXT, proxy TEXT, report_no TEXT, acm_classes TEXT, msc_classes TEXT, journal_ref TEXT, doi TEXT, license TEXT, abstract TEXT)",
            (),
        )?;
    tr.execute(
        "CREATE TABLE last_update (set_ TEXT PRIMARY KEY, date TEXT)",
        (),
    )?;
    tr.execute(
        "CREATE TABLE resumption_data (set_ TEXT PRIMARY KEY, data TEXT)",
        (),
    )?;
    tr.execute("INSERT INTO db_version (version) VALUES (?1)", params!["1"])?;
    tr.commit()?;
    // Upgrade the database schema.
    with_transaction(&mut conn, |_| Ok(()))?;
    Ok(())
}

/// Creates a transaction, updating the database schema (and committing) if necessary.
/// Then calls the given function with a transaction in which the database schema is
/// guaranteed to have the correct version.
///
/// We use a callback instead of simply returning a Transaction to avoid lifetime issues.
pub fn with_transaction<T, F: FnOnce(Transaction) -> anyhow::Result<T>>(
    conn: &mut Connection,
    f: F,
) -> anyhow::Result<T> {
    loop {
        let tr = conn.transaction()?;
        if let Some(tr) = upgrade_step(tr)? {
            return f(tr);
        }
    }
}

/// Like `with_transaction`, but creates a transaction of IMMEDIATE type.
pub fn with_write_transaction<T, F: FnOnce(Transaction) -> anyhow::Result<T>>(
    conn: &mut Connection,
    f: F,
) -> anyhow::Result<T> {
    loop {
        let tr = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if let Some(tr) = upgrade_step(tr)? {
            return f(tr);
        }
    }
}

fn get_version(conn: &Connection) -> anyhow::Result<String> {
    conn.query_one("SELECT version FROM db_version", (), |row: &Row| row.get(0))
        .context("reading database version")
}

/// Upgrades the database schema by one step if necessary.
/// Returns Ok(None) if the database had to be upgraded and Ok(tr) otherwise.
fn upgrade_step(tr: Transaction) -> anyhow::Result<Option<Transaction>> {
    let old_version = get_version(&tr)?;
    let new_version = match old_version.as_str() {
        "1" => {
            tr.execute(
                "ALTER TABLE last_update ADD COLUMN resumption_data TEXT",
                (),
            )?;
            tr.execute("DROP TABLE resumption_data", ())?;
            "2"
        }
        "2" => {
            tr.execute("ALTER TABLE article ADD COLUMN last_change TEXT", ())?;
            tr.execute("ALTER TABLE article ADD COLUMN sets TEXT", ())?;
            tr.execute("DELETE FROM last_update", ())?;
            "3"
        }
        "3" => {
            tr.execute("ALTER TABLE last_update RENAME TO set_", ())?;
            tr.execute("ALTER TABLE set_ RENAME COLUMN set_ TO name", ())?;
            tr.execute("ALTER TABLE set_ ADD COLUMN category TEXT", ())?;
            let mut get = tr.prepare("SELECT name FROM set_")?;
            let mut upd = tr.prepare("UPDATE set_ SET category = ?2 WHERE name = ?1")?;
            let mut rows = get.query(())?;
            while let Some(row) = rows.next()? {
                let spec: String = row.get(0)?;
                if let Some((_, category)) = spec.split_once(':') {
                    let category = category.replace(':', ".");
                    upd.execute(params![spec, category])?;
                }
            }
            "4"
        }
        "4" => {
            return Ok(Some(tr));
        }
        _ => {
            bail!("unknown database version {old_version}");
        }
    };
    assert_ne!(old_version, new_version);
    tr.execute("UPDATE db_version SET version = ?1", params![new_version])?;
    tr.commit()?;
    Ok(None)
}

#[derive(Serialize, Deserialize)]
struct DbDump {
    articles: Vec<ArticleMetadata>,
    last_update: HashMap<String, String>,
}

pub fn dump(tr: &Transaction) -> anyhow::Result<()> {
    let articles: Vec<_> = ArticleMetadata::load(tr)?.into_values().collect();
    let last_update = Continuation::read_all(tr)?;
    let last_update = last_update
        .into_iter()
        .map(|(set, cont)| (set, cont.last_update.unwrap()))
        .collect();
    let db = DbDump {
        articles,
        last_update,
    };
    serde_json::to_writer_pretty(stdout(), &db)?;
    println!();
    Ok(())
}

pub fn load(tr: Transaction) -> anyhow::Result<()> {
    let db: DbDump = serde_json::from_reader(stdin())?;
    println!("Loading {} articles", db.articles.len());
    for mut article in db.articles.into_iter() {
        let id = article.id.clone();
        if let Some(old_article) = ArticleMetadata::load_one(&tr, &id)? {
            for (i, old_version) in old_article.versions.into_iter().enumerate() {
                if let Some(new_version) = article.versions.get_mut(i)
                    && new_version.first_encounter > old_version.first_encounter
                {
                    new_version.first_encounter = old_version.first_encounter;
                }
            }
        }
        article
            .validate()
            .with_context(|| format!("invalid metadata of article {id}"))?;
        article.write(&tr)?;
    }
    for last_update in db.last_update.values() {
        // We have updated some articles with this response date.
        // Any later record updates may have been overwritten.
        Continuation::reset_last_update(&tr, last_update)?;
    }
    for (set, last_update) in &db.last_update {
        Continuation::update_last_update(&tr, set, last_update)?;
    }
    tr.commit()?;
    Ok(())
}
