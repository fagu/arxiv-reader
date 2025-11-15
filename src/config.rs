use std::{collections::HashSet, fmt::Display, str::FromStr};

use anyhow::bail;
use serde::Deserialize;

use crate::filter::Filter;

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct TagName(pub String);

impl FromStr for TagName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let valid_first_chars = |c: char| c.is_ascii_alphanumeric();
        let valid_chars = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '-';
        if s.chars().next().is_some_and(valid_first_chars) && s.chars().all(valid_chars) {
            Ok(Self(s.to_string()))
        } else {
            bail!("invalid tag name: {:?}", s)
        }
    }
}

impl<'de> Deserialize<'de> for TagName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl Display for TagName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Which categories to subscribe to. See https://arxiv.org/category_taxonomy for a list of all categories.
    pub categories: Vec<String>,
    #[serde(default)]
    pub latex_to_unicode: bool,
    #[serde(default)]
    pub tags: Vec<(char, TagName)>,
    pub filters: Filters,
    #[serde(default)]
    pub hooks: Hooks,
    #[serde(default)]
    pub highlight: Highlight,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filters {
    /// By default, only consider articles satisfying the given conditions.
    pub new: Filter,
    /// Only show updates (new versions, journals, doi) for articles additionally satisfying these conditions.
    pub update: Filter,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Hooks {
    /// Command to run before pulling.
    pub pre_pull: Option<String>,
    /// Command to run for pushing.
    pub push: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Highlight {
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default)]
    pub authors: Vec<String>,
    #[serde(default)]
    pub categories: HashSet<String>,
    #[serde(default)]
    pub acm_classes: Vec<String>,
    #[serde(default)]
    pub msc_classes: Vec<String>,
}
