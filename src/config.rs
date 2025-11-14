use std::collections::HashSet;

use serde::Deserialize;

use crate::filter::Filter;

fn true_fn() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Which categories to subscribe to. See https://arxiv.org/category_taxonomy for a list of all categories.
    pub categories: Vec<String>,
    #[serde(default = "true_fn")]
    pub latex_to_unicode: bool,
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
