use std::{cmp::min, collections::HashMap, fs::remove_file, io::Write, path::Path, time::Instant};

use anyhow::{Context, bail};
use chrono::{DateTime, Days, NaiveDate};
use reqwest::header::HeaderValue;
use rusqlite::{Connection, Transaction, params};
use serde::{Deserialize, Serialize};

use crate::{db, rate_limited_client::Client, util::write_then_rename};

pub struct Continuation {
    pub last_update: Option<String>,
    resumption_data: Option<ResumptionData>,
}

impl Continuation {
    pub fn read_all(tr: &Transaction) -> anyhow::Result<HashMap<String, Self>> {
        let mut get = tr.prepare("SELECT name, date, resumption_data FROM set_")?;
        let mut rows = get.query(())?;
        let mut res = HashMap::new();
        while let Some(row) = rows.next()? {
            let set: String = row.get(0)?;
            let last_update: String = row.get(1)?;
            let resumption_data: Option<String> = row.get(2)?;
            let resumption_data = match resumption_data {
                Some(resumption_data) => Some(serde_json::from_str(&resumption_data)?),
                None => None,
            };
            res.insert(
                set,
                Continuation {
                    last_update: Some(last_update),
                    resumption_data,
                },
            );
        }
        Ok(res)
    }
    fn set_for_category(tr: &Transaction, category: &str) -> anyhow::Result<Option<String>> {
        if category.is_empty() {
            Ok(Some(String::new()))
        } else {
            let mut get = tr.prepare("SELECT name FROM set_ WHERE category = ?1")?;
            let mut rows = get.query(params![category])?;
            match rows.next()? {
                Some(row) => {
                    let name = row.get(0)?;
                    Ok(Some(name))
                }
                None => Ok(None),
            }
        }
    }
    fn read(tr: &Transaction, set: &str) -> anyhow::Result<Self> {
        let mut get = tr.prepare("SELECT date, resumption_data FROM set_ WHERE name = ?1")?;
        let mut rows = get.query(params![set])?;
        match rows.next()? {
            Some(row) => {
                let last_update: Option<String> = row.get(0)?;
                let resumption_data: Option<String> = row.get(1)?;
                let resumption_data = match resumption_data {
                    Some(resumption_data) => Some(serde_json::from_str(&resumption_data)?),
                    None => None,
                };
                Ok(Continuation {
                    last_update: last_update,
                    resumption_data,
                })
            }
            None => Ok(Continuation {
                last_update: None,
                resumption_data: None,
            }),
        }
    }
    /// For every set with last update < date, assign last update = date and clear resumption data.
    pub fn reset_last_update(tr: &Transaction, date: &str) -> anyhow::Result<()> {
        let mut get = tr.prepare("SELECT name, date FROM set_")?;
        let mut upd =
            tr.prepare("UPDATE set_ SET date = ?2, resumption_data = NULL WHERE name = ?1")?;
        let mut rows = get.query(())?;
        while let Some(row) = rows.next()? {
            let set: String = row.get(0)?;
            let prev_date: Option<String> = row.get(1)?;
            if let Some(prev_date) = prev_date
                && *date < *prev_date
            {
                upd.execute(params![set, date])?;
            }
        }
        Ok(())
    }
    /// Set last update field and reset resumption data.
    pub fn update_last_update(
        tr: &Transaction,
        set: &str,
        last_update: &str,
    ) -> anyhow::Result<()> {
        Self::reset_last_update(tr, last_update)?;
        tr.execute(
            "UPDATE set_ SET date = ?2, resumption_data = NULL WHERE name = ?1",
            params![set, last_update],
        )?;
        Ok(())
    }
    /// Set resumption data.
    fn update_resumption_data(
        tr: &Transaction,
        set: &str,
        data: &ResumptionData,
    ) -> anyhow::Result<()> {
        tr.execute(
            "UPDATE set_ SET resumption_data = ?2 WHERE name = ?1",
            params![set, serde_json::to_string(data)?],
        )?;
        Ok(())
    }
    /// Reset resumption data.
    pub fn clear_resumption_data(tr: &Transaction, set: &str) -> anyhow::Result<()> {
        tr.execute(
            "UPDATE set_ SET resumption_data = NULL WHERE name = ?1",
            params![set],
        )?;
        Ok(())
    }
}

/// Data needed to resume an unfinished incomplete download.
#[derive(Serialize, Deserialize)]
struct ResumptionData {
    request_number: usize,
    resumption_request: String,
    /// The response date of the first response.
    response_date: Option<String>,
}

pub fn download_changes(
    base_dir: &Path,
    conn: &mut Connection,
    category: &str,
    client: &mut Client,
) -> anyhow::Result<()> {
    // Keep making requests until done.
    loop {
        // We start a new transaction on each request.
        // This way, intermediate progress will be saved.
        let continue_ = db::with_write_transaction(conn, base_dir, |tr| {
            // Find the name of the set corresponding to this category.
            let set = if let Some(set) = Continuation::set_for_category(&tr, category)? {
                set
            } else {
                // Try downloading a list of all sets.
                update_sets(base_dir, &tr, client)?;
                // Then, look for the category again.
                Continuation::set_for_category(&tr, category)?
                    .with_context(|| format!("category {category:?} not found"))?
            };
            // Check whether there is resumption data for this set.
            let cont = Continuation::read(&tr, &set)?;
            // If not, create a new request.
            let mut resumption_data = if let Some(r) = cont.resumption_data {
                r
            } else {
                let mut resumption_request = "verb=ListRecords&metadataPrefix=arXivRaw".to_string();
                // Restrict to the sets specified in the configuration file.
                if !set.is_empty() {
                    resumption_request += &format!("&set={}", set);
                }
                // Only ask for changes since the previous update.
                if let Some(from) = cont.last_update {
                    let from = NaiveDate::parse_from_str(&from, "%Y-%m-%d")
                        .with_context(|| format!("parsing date {from}"))?;
                    // Subtract one day to ensure an overlap so that we won't lose changes that
                    // occurred around midnight.
                    // See https://www.openarchives.org/OAI/2.0/guidelines-harvester.htm,
                    // which says:
                    //   "[...] to incrementally harvest from a repository, a harvester should
                    //   overlap successive incremental harvests by one datestamp increment [...]"
                    let from = from
                        .checked_sub_days(Days::new(1))
                        .with_context(|| format!("parsing date {from}"))?;
                    println!("Retrieving changes since {}.", from.format("%Y-%m-%d"));
                    resumption_request += &format!("&from={}", from.format("%Y-%m-%d"));
                }
                ResumptionData {
                    request_number: 1,
                    resumption_request,
                    response_date: None,
                }
            };
            // Make the request.
            let res = client.with(|client| {
                println!("Getting changeset {}...", resumption_data.request_number);
                let before_request = Instant::now();
                let res = client
                    .post("https://oaipmh.arxiv.org/oai".to_string())
                    .header(
                        reqwest::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .body(resumption_data.resumption_request.clone())
                    .send()
                    .and_then(|res| res.error_for_status())
                    .context("requesting data from oaipmh.arxiv.org")?;
                let request_duration = Instant::now().duration_since(before_request);
                println!(
                    "Received response after {:.2} seconds.",
                    request_duration.as_secs_f32()
                );
                let content_type = res.headers().get("Content-Type");
                if content_type != Some(&HeaderValue::from_static("text/xml")) {
                    bail!("wrong content type (expected text/xml, received {content_type:?})");
                }
                let res = res
                    .bytes()
                    .context("requesting data from oaipmh.arxiv.org")?;
                Ok(res)
            })?;
            // Save a copy of the response to update.xml for debugging in case something goes wrong.
            let xml_file = base_dir.join("update.xml");
            write_then_rename(xml_file.clone(), |writer| {
                writer.write_all(&res)?;
                Ok(())
            })
            .context("writing update.xml file")?;
            let res =
                str::from_utf8(&res).context("reading data from oaipmh.arxiv.org (non-utf8)")?;
            // Parse the response.
            let oai_pmh: OaipmhListRecords =
                quick_xml::de::from_str(res).context("parsing response from oaipmh.arxiv.org")?;
            // Extract the response date for the first request.
            if resumption_data.response_date.is_none() {
                resumption_data.response_date = Some(
                    oai_pmh
                        .response_date
                        .split_at_checked(10)
                        .context("parsing response from oaipmh.arxiv.org")
                        .context("invalid response date")?
                        .0
                        .to_string(),
                );
            }
            // Abort if there were any errors.
            if !oai_pmh.errors.is_empty() {
                // In case of a bad resumption token, delete it, and ask the user to retry.
                if oai_pmh
                    .errors
                    .iter()
                    .any(|error| error.code == "badResumptionToken")
                {
                    Continuation::clear_resumption_data(&tr, &set)?;
                    tr.commit()?;
                    bail!("Bad or expired resumption token. Please retry.");
                }
                if oai_pmh
                    .errors
                    .iter()
                    .any(|error| error.code == "noRecordsMatch")
                {
                    println!("Received 0 records.");
                    // Nothing went wrong, so we delete update.xml.
                    remove_file(xml_file).context("removing update.xml")?;
                    // Clear the resumption data as we are done.
                    // Save the date of the first response. Only changes on or after this
                    // date need to be taken into account in later requests.
                    Continuation::update_last_update(
                        &tr,
                        &set,
                        &resumption_data.response_date.unwrap(),
                    )?;
                    tr.commit()?;
                    return Ok(false);
                }
                // Otherwise, just print all errors and abort.
                for error in &oai_pmh.errors {
                    println!(
                        "{}: {}",
                        error.code,
                        error.value.clone().unwrap_or_default()
                    );
                }
                bail!("Download failed.");
            }
            let list_records = oai_pmh
                .list_records
                .context("parsing response from oaipmh.arxiv.org")
                .context("missing <ListRecords>")?;
            let records = list_records.records;
            println!("Received {} records.", records.len());
            // Save the records (= articles) from the response.
            for article in records {
                let header = article.header;
                let article = article.metadata.arxiv_raw;
                let id = article
                    .id
                    .parse()
                    .context("parsing response from oaipmh.arxiv.org")
                    .with_context(|| format!("invalid article id {:?}", article.id))?;
                // If this article was already encountered before, retrieve it.
                let old_article = crate::article::ArticleMetadata::load_one(&tr, &id)?;
                let old_versions = old_article.map(|a| a.versions);
                let mut versions = Vec::new();
                // The number of versions should never go down.
                if let Some(old_versions) = old_versions.as_ref()
                    && old_versions.len() > article.versions.len()
                {
                    bail!("more versions in old metadata update");
                }
                for (i, version) in article.versions.into_iter().enumerate() {
                    let old_version = old_versions
                        .as_ref()
                        .and_then(|old_versions| old_versions.get(i));
                    let number = version
                        .version
                        .strip_prefix('v')
                        .context("parsing response from oaipmh.arxiv.org")
                        .with_context(|| format!("invalid version number {:?}", version.version))?
                        .parse()?;
                    let date = DateTime::parse_from_rfc2822(&version.date)
                        .context("parsing response from oaipmh.arxiv.org")
                        .with_context(|| format!("invalid date: {:?}", version.date))?;
                    // Compute the first response date in which we have seen this article version.
                    let first_encounter = match old_version {
                        Some(old_version) => min(
                            old_version.first_encounter.clone(),
                            resumption_data.response_date.clone().unwrap(),
                        ),
                        None => resumption_data.response_date.clone().unwrap(),
                    };
                    versions.push(crate::article::Version {
                        number,
                        date,
                        size: version.size,
                        source_type: version.source_type,
                        first_encounter,
                    });
                }
                let categories = article
                    .categories
                    .split(' ')
                    .map(|s| s.to_string())
                    .collect();
                let article = crate::article::ArticleMetadata {
                    id: id.clone(),
                    submitter: article.submitter,
                    versions,
                    title: article.title,
                    authors: article.authors,
                    categories,
                    comments: article.comments,
                    proxy: article.proxy,
                    report_no: article.report_no,
                    acm_classes: article.acm_classes,
                    msc_classes: article.msc_classes,
                    journal_ref: article.journal_ref,
                    doi: article.doi,
                    license: article.license,
                    abstract_: article.abstract_,
                    last_change: Some(header.datestamp),
                    sets: Some(header.sets),
                };
                // Validate and then save the article metadata.
                article
                    .validate()
                    .with_context(|| format!("invalid metadata of article {id}"))?;
                article.write(&tr)?;
            }
            let response_date = resumption_data.response_date.as_ref().unwrap();
            // Nothing went wrong, so we delete update.xml.
            remove_file(xml_file).context("removing update.xml")?;
            // We have updated some articles with this response date.
            // Any later record updates may have been overwritten.
            Continuation::reset_last_update(&tr, response_date)?;
            // If the response contains a non-empty resumption token element, use
            // it for the next response. Otherwise, stop.
            if let Some(resumption_token) = list_records.resumption_token
                && let Some(resumption_token_value) = resumption_token.value
            {
                resumption_data.request_number += 1;
                resumption_data.resumption_request = format!(
                    "verb=ListRecords&resumptionToken={}",
                    resumption_token_value
                );
                // Write the resumption data in case of problems with the next request.
                Continuation::update_resumption_data(&tr, &set, &resumption_data)?;
                tr.commit()?;
                return Ok(true);
            } else {
                // Clear the resumption data as we are done.
                // Save the date of the first response. Only changes on or after this
                // date need to be taken into account in later requests.
                Continuation::update_last_update(&tr, &set, response_date)?;
                tr.commit()?;
                return Ok(false);
            }
        })?;
        if !continue_ {
            break;
        }
    }
    Ok(())
}

// Below are structs that can be deserialized from the server's responses.
// See the following references for details:
// https://info.arxiv.org/help/oa/index.html
// https://www.openarchives.org/OAI/2.0/openarchivesprotocol.htm
// https://arxiv.org/OAI/arXivRaw.xsd

#[derive(Deserialize)]
struct OaipmhListRecords {
    #[serde(rename = "responseDate")]
    response_date: String,
    #[serde(default, rename = "error")]
    errors: Vec<OaiError>,
    #[serde(rename = "ListRecords")]
    list_records: Option<ListRecords>,
}

#[derive(Deserialize)]
struct ListRecords {
    #[serde(default, rename = "record")]
    records: Vec<Set>,
    #[serde(rename = "resumptionToken")]
    resumption_token: Option<ResumptionToken>,
}

#[derive(Deserialize)]
struct Set {
    header: Header,
    metadata: Metadata,
}

#[derive(Deserialize)]
struct Header {
    datestamp: String,
    #[serde(rename = "setSpec")]
    sets: Vec<String>,
}

#[derive(Deserialize)]
struct Metadata {
    #[serde(rename = "arXivRaw")]
    arxiv_raw: ArXivRaw,
}

/// See https://arxiv.org/OAI/arXivRaw.xsd for the list of fields.
#[derive(Deserialize)]
struct ArXivRaw {
    id: String,
    submitter: String,
    #[serde(rename = "version")]
    versions: Vec<Version>,
    title: String,
    authors: String,
    categories: String,
    comments: Option<String>,
    proxy: Option<String>,
    #[serde(rename = "report-no")]
    report_no: Option<String>,
    #[serde(rename = "acm-class")]
    acm_classes: Option<String>,
    #[serde(rename = "msc-class")]
    msc_classes: Option<String>,
    #[serde(rename = "journal-ref")]
    journal_ref: Option<String>,
    doi: Option<String>,
    license: Option<String>,
    #[serde(rename = "abstract")]
    abstract_: String,
}

#[derive(Deserialize)]
struct Version {
    #[serde(rename = "@version")]
    version: String,
    date: String,
    size: String,
    source_type: Option<String>,
}

#[derive(Deserialize)]
struct ResumptionToken {
    #[allow(unused)]
    #[serde(rename = "@expirationDate")]
    expiration_date: Option<String>,
    #[serde(rename = "$value")]
    value: Option<String>,
}

#[derive(Deserialize)]
struct OaiError {
    #[serde(rename = "@code")]
    code: String,
    #[allow(unused)]
    #[serde(rename = "$value")]
    value: Option<String>,
}

pub fn update_sets(base_dir: &Path, tr: &Transaction, client: &mut Client) -> anyhow::Result<()> {
    // Make the request.
    let res = client.with(|client| {
        println!("Getting list of sets...");
        let before_request = Instant::now();
        let res = client
            .post("https://oaipmh.arxiv.org/oai".to_string())
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body("verb=ListSets")
            .send()
            .and_then(|res| res.error_for_status())
            .context("requesting data from oaipmh.arxiv.org")?;
        let request_duration = Instant::now().duration_since(before_request);
        println!(
            "Received response after {:.2} seconds.",
            request_duration.as_secs_f32()
        );
        let content_type = res.headers().get("Content-Type");
        if content_type != Some(&HeaderValue::from_static("text/xml")) {
            bail!("wrong content type (expected text/xml, received {content_type:?})");
        }
        let res = res
            .bytes()
            .context("requesting data from oaipmh.arxiv.org")?;
        Ok(res)
    })?;

    // Save a copy of the response to update.xml for debugging in case something goes wrong.
    let xml_file = base_dir.join("update.xml");
    write_then_rename(xml_file.clone(), |writer| {
        writer.write_all(&res)?;
        Ok(())
    })
    .context("writing update.xml file")?;
    let res = str::from_utf8(&res).context("reading data from oaipmh.arxiv.org (non-utf8)")?;
    // Parse the response.
    let oai_pmh: OaipmhListSets =
        quick_xml::de::from_str(res).context("parsing response from oaipmh.arxiv.org")?;

    // Abort if there were any errors.
    if !oai_pmh.errors.is_empty() {
        // Print all errors and abort.
        for error in &oai_pmh.errors {
            println!(
                "{}: {}",
                error.code,
                error.value.clone().unwrap_or_default()
            );
        }
        bail!("Download failed.");
    }

    let list_sets = oai_pmh
        .list_sets
        .context("parsing response from oaipmh.arxiv.org")
        .context("missing <ListSets>")?;

    if list_sets.resumption_token.is_some() {
        bail!("resumption tokens for ListSets are currently not implemented by `arxiv-reader`");
    }

    let sets = list_sets.sets;
    println!("Received {} sets.", sets.len());

    let mut ins = tr.prepare("INSERT OR IGNORE INTO set_ (name, category) VALUES (?1, ?2)")?;
    for set in sets.iter() {
        if let Some((_, category)) = set.spec.split_once(':') {
            let category = category.replace(':', ".");
            ins.execute(params![set.spec, category])?;
        }
    }

    Ok(())
}

#[derive(Deserialize)]
struct OaipmhListSets {
    #[allow(unused)]
    #[serde(rename = "responseDate")]
    response_date: String,
    #[serde(default, rename = "error")]
    errors: Vec<OaiError>,
    #[serde(rename = "ListSets")]
    list_sets: Option<ListSets>,
}

#[derive(Deserialize)]
struct ListSets {
    #[serde(default, rename = "set")]
    sets: Vec<Set2>,
    #[serde(rename = "resumptionToken")]
    resumption_token: Option<ResumptionToken>,
}

#[derive(Deserialize)]
struct Set2 {
    #[serde(rename = "setSpec")]
    spec: String,
    #[allow(unused)]
    #[serde(rename = "setName")]
    name: String,
}
