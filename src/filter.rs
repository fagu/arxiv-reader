use anyhow::{Context, anyhow, bail};
use std::{collections::VecDeque, str::FromStr};

use serde::Deserialize;

use crate::config::TagName;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Filter {
    PrimaryCategoryIs(String),
    InCategory(String),
    FirstVersionEncounteredAfter(String),
    FirstVersionSubmittedAfter(String),
    Title(String),
    Author(String),
    ACMClass(String),
    MSCClass(String),
    Abstract(String),
    Comments(String),
    Bookmarked,
    Seen,
    Tag(TagName),
    Notes(String),
    Any(String),
    Not(Box<Filter>),
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
    Id(String),
    True,
    False,
}

impl Filter {
    #[rustfmt::skip]
    pub fn matches(&self, article: &crate::article::Article) -> bool {
        match self {
            Filter::PrimaryCategoryIs(name) => article.primary_category().as_str() == name,
            Filter::InCategory(name) => article.categories().contains(name),
            Filter::FirstVersionEncounteredAfter(date) => article.first_version().first_encounter >= *date,
            Filter::FirstVersionSubmittedAfter(date) => article.first_version().date.naive_utc().date().to_string() >= *date,
            Filter::Title(word) => article.title().to_ascii_lowercase().contains(&word.to_ascii_lowercase()),
            Filter::Author(word) => article.authors().contains(word),
            Filter::ACMClass(pattern) => article.acm_classes().is_some_and(|c| c.contains(pattern)),
            Filter::MSCClass(pattern) => article.msc_classes().is_some_and(|c| c.contains(pattern)),
            Filter::Abstract(word) => article.abstract_().to_ascii_lowercase().contains(&word.to_ascii_lowercase()),
            Filter::Comments(word) => article.comments().is_some_and(|c| c.to_ascii_lowercase().contains(&word.to_ascii_lowercase())),
            Filter::Bookmarked => article.is_bookmarked(),
            Filter::Seen => article.last_seen_version() > 0,
            Filter::Tag(tag) => article.tags().contains(tag),
            Filter::Notes(pattern) => article.notes().is_some_and(|c| c.to_ascii_lowercase().contains(&pattern.to_ascii_lowercase())),
            Filter::Any(word) => {
                article.categories().contains(word)
                    || article.title().to_ascii_lowercase().contains(&word.to_ascii_lowercase())
                    || article.authors().contains(word)
                    || article.acm_classes().is_some_and(|c| c.contains(word))
                    || article.msc_classes().is_some_and(|c| c.contains(word))
                    || article.abstract_().to_ascii_lowercase().contains(&word.to_ascii_lowercase())
                    || article.comments().is_some_and(|c| c.to_ascii_lowercase().contains(&word.to_ascii_lowercase()))
                    || article.notes().is_some_and(|c| c.to_ascii_lowercase().contains(&word.to_ascii_lowercase()))
            }
            Filter::Not(a) => !a.matches(article),
            Filter::And(a, b) => a.matches(article) && b.matches(article),
            Filter::Or(a, b) => a.matches(article) || b.matches(article),
            Filter::Id(id) => article.id().to_string() == *id,
            Filter::True => true,
            Filter::False => false,
        }
    }
}

#[derive(Debug)]
enum Token {
    EscapedString(String),
    UnescapedString,
    OpenParen,
    CloseParen,
    Not,
    And,
    Or,
}

#[derive(Debug)]
struct SpannedToken<'a> {
    text: &'a str,
    token: Token,
    start: usize,
}

fn describe(token: Option<SpannedToken>) -> String {
    if let Some(token) = token {
        format!("{:?} at index {}", token.text, token.start)
    } else {
        "end".to_string()
    }
}

#[allow(unused)]
fn string(input: &mut VecDeque<SpannedToken>) -> anyhow::Result<String> {
    let t = input.pop_front();
    match t.as_ref() {
        Some(t) => match &t.token {
            Token::EscapedString(s) => Some(s.clone()),
            Token::UnescapedString => Some(t.text.to_string()),
            _ => None,
        },
        None => None,
    }
    .with_context(|| anyhow!("expected string, found {}", describe(t)))
}

fn one_or_more_strings(input: &mut VecDeque<SpannedToken>) -> anyhow::Result<Vec<String>> {
    let t = input.pop_front();
    let s = match t.as_ref() {
        Some(t) => match &t.token {
            Token::EscapedString(s) => Some(s.clone()),
            Token::UnescapedString => Some(t.text.to_string()),
            _ => None,
        },
        None => None,
    }
    .with_context(|| anyhow!("expected string, found {}", describe(t)))?;
    let mut res = vec![s];
    while let Some(t) = input.front() {
        match &t.token {
            Token::EscapedString(s) => res.push(s.clone()),
            Token::UnescapedString => res.push(t.text.to_string()),
            _ => {
                break;
            }
        }
        input.pop_front();
    }
    Ok(res)
}

fn unescaped_string(
    input: &mut VecDeque<SpannedToken>,
    expected: &str,
    validator: impl FnOnce(&str) -> bool,
) -> anyhow::Result<String> {
    let t = input.pop_front();
    match t.as_ref() {
        Some(t) => match &t.token {
            Token::UnescapedString => Some(t.text.to_string()),
            _ => None,
        },
        None => None,
    }
    .filter(|s| validator(s))
    .with_context(|| anyhow!("expected {expected}, found {}", describe(t)))
}

fn category_name(input: &mut VecDeque<SpannedToken>) -> anyhow::Result<String> {
    unescaped_string(input, "category name", |s| {
        s.chars()
            .all(|c| c.is_ascii_alphabetic() || c == '.' || c == '-')
    })
}

fn date(input: &mut VecDeque<SpannedToken>) -> anyhow::Result<String> {
    unescaped_string(input, "date", |s| {
        let mut it = s.chars();
        it.next().is_some_and(|c| c.is_ascii_digit())
            && it.next().is_some_and(|c| c.is_ascii_digit())
            && it.next().is_some_and(|c| c.is_ascii_digit())
            && it.next().is_some_and(|c| c.is_ascii_digit())
            && it.next().is_some_and(|c| c == '-')
            && it.next().is_some_and(|c| c.is_ascii_digit())
            && it.next().is_some_and(|c| c.is_ascii_digit())
            && it.next().is_some_and(|c| c == '-')
            && it.next().is_some_and(|c| c.is_ascii_digit())
            && it.next().is_some_and(|c| c.is_ascii_digit())
            && it.next().is_none()
    })
}

fn acm_or_msc_class(input: &mut VecDeque<SpannedToken>) -> anyhow::Result<String> {
    unescaped_string(input, "acm or msc class", |s| {
        s.len() <= 5
            && s.chars()
                .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
    })
}

fn fold_and<T>(cond: impl Fn(T) -> Filter, params: Vec<T>) -> Filter {
    params.into_iter().fold(Filter::True, |res, s| {
        Filter::And(Box::new(res), Box::new(cond(s)))
    })
}

fn fold_or<T>(cond: impl Fn(T) -> Filter, params: Vec<T>) -> Filter {
    params.into_iter().fold(Filter::True, |res, s| {
        Filter::Or(Box::new(res), Box::new(cond(s)))
    })
}

fn term(input: &mut VecDeque<SpannedToken>) -> anyhow::Result<Filter> {
    let t = input.pop_front();
    match t.as_ref() {
        #[rustfmt::skip]
        Some(t) => match &t.token {
            Token::OpenParen => Some(expression(input, true)?),
            Token::Not => Some(Filter::Not(Box::new(term(input)?))),
            Token::UnescapedString => match t.text {
                "primary_category" => Some(Filter::PrimaryCategoryIs(category_name(input)?)),
                "category" => Some(Filter::InCategory(category_name(input)?)),
                "first_version_encountered_after" => Some(Filter::FirstVersionEncounteredAfter(date(input)?)),
                "first_version_submitted_after" => Some(Filter::FirstVersionSubmittedAfter(date(input)?)),
                "title" => Some(fold_and(Filter::Title, one_or_more_strings(input)?)),
                "author" => Some(fold_and(Filter::Author, one_or_more_strings(input)?)),
                "acm" => Some(Filter::ACMClass(acm_or_msc_class(input)?)),
                "msc" => Some(Filter::MSCClass(acm_or_msc_class(input)?)),
                "abstract" => Some(fold_and(Filter::Abstract, one_or_more_strings(input)?)),
                "comments" => Some(fold_and(Filter::Comments, one_or_more_strings(input)?)),
                "bookmarked" => Some(Filter::Bookmarked),
                "seen" => Some(Filter::Seen),
                "tag" => Some(fold_and(Filter::Tag, one_or_more_strings(input)?.iter().map(|s| s.parse::<TagName>()).collect::<Result<_,_>>()?)),
                "notes" => Some(fold_and(Filter::Notes, one_or_more_strings(input)?)),
                "any" => Some(fold_and(Filter::Any, one_or_more_strings(input)?)),
                "id" => Some(fold_or(Filter::Id, one_or_more_strings(input)?)),
                "true" => Some(Filter::True),
                "false" => Some(Filter::False),
                _ => None,
            },
            _ => None,
        },
        None => None,
    }
    .with_context(|| anyhow!("expected condition, found {}", describe(t)))
}

fn expression(
    input: &mut VecDeque<SpannedToken>,
    inside_parenthesis: bool,
) -> anyhow::Result<Filter> {
    let mut res = term(input)?;
    let mut prev_op: Option<&str> = None;
    loop {
        let op = input.pop_front();
        let op = match op.as_ref() {
            Some(op) => match &op.token {
                Token::And if prev_op.is_none_or(|o| o == "&&") => Some("&&"),
                Token::Or if prev_op.is_none_or(|o| o == "||") => Some("||"),
                Token::CloseParen if inside_parenthesis => {
                    break;
                }
                _ => None,
            },
            None if !inside_parenthesis => {
                break;
            }
            _ => None,
        }
        .with_context(|| {
            let ops = match prev_op {
                Some(prev_op) => format!("'{prev_op}'"),
                None => "'&&' or '||'".to_string(),
            };
            let end = if inside_parenthesis { "')'" } else { "end" };
            anyhow!("expected {ops} or {end}, found {}", describe(op))
        })?;
        let term2 = term(input)?;
        res = match op {
            "&&" => Filter::And(Box::new(res), Box::new(term2)),
            "||" => Filter::Or(Box::new(res), Box::new(term2)),
            _ => {
                panic!("unexpected operation");
            }
        };
        prev_op = Some(op);
    }
    Ok(res)
}

#[derive(PartialEq, Eq, Debug)]
struct Input<'a> {
    text: &'a str,
    pos: usize,
}

impl<'a> Input<'a> {
    fn new(text: &'a str) -> Input<'a> {
        Self { text, pos: 0 }
    }
    fn peek(&self) -> Option<char> {
        self.text[self.pos..].chars().next()
    }
    fn take(&mut self) -> Option<char> {
        let r = self.peek();
        if let Some(c) = r {
            self.pos += c.len_utf8();
        }
        r
    }
    fn expect(&mut self, c: char) -> anyhow::Result<()> {
        let j = self.pos;
        match self.take() {
            Some(d) if d == c => Ok(()),
            Some(d) => {
                bail!("expected '{c}', found {d:?} at index {j}");
            }
            None => {
                bail!("expected '{c}', found end");
            }
        }
    }
}

fn tokenize<'a>(text: &'a str) -> anyhow::Result<VecDeque<SpannedToken<'a>>> {
    let mut res = VecDeque::new();
    let mut it = Input::new(text);
    loop {
        let i = it.pos;
        let mut add_token = |it: &Input, token: Token| {
            res.push_back(SpannedToken {
                text: &text[i..it.pos],
                token,
                start: i,
            });
        };
        match it.take() {
            Some(' ') => {}
            Some('(') => add_token(&it, Token::OpenParen),
            Some(')') => add_token(&it, Token::CloseParen),
            Some('!') => add_token(&it, Token::Not),
            Some('&') => {
                it.expect('&')?;
                add_token(&it, Token::And);
            }
            Some('|') => {
                it.expect('|')?;
                add_token(&it, Token::Or);
            }
            Some(c) if c == '\'' || c == '"' => {
                // Quoted string.
                let mut r = String::new();
                loop {
                    let j = it.pos;
                    let d = match it.take() {
                        Some(d) if d == c => {
                            break;
                        }
                        Some('\\') => match it.take() {
                            Some('\'') => '\'',
                            Some('"') => '"',
                            Some('\\') => '\\',
                            Some(e) => {
                                bail!("expected escaped character, found '{e:?}' at index {j}");
                            }
                            None => {
                                bail!("expected escaped character, found end");
                            }
                        },
                        Some(d) => d,
                        None => {
                            bail!("expected '{c}', found end");
                        }
                    };
                    r.push(d);
                }
                add_token(&it, Token::EscapedString(r));
            }
            Some(_) => {
                // Unquoted string.
                loop {
                    match it.peek() {
                        Some(' ') | Some('(') | Some(')') | Some('!') | Some('&') | Some('|')
                        | Some('\'') | Some('"') | None => {
                            break;
                        }
                        _ => {
                            it.take();
                        }
                    }
                }
                add_token(&it, Token::UnescapedString);
            }
            None => {
                break;
            }
        }
    }
    Ok(res)
}

#[allow(unused)]
impl FromStr for Filter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut tokens = tokenize(s)?;
        let filter = expression(&mut tokens, false).map_err(|e| anyhow!("parsing filter: {e}"))?;
        assert!(tokens.is_empty());
        Ok(filter)
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn normal() {
        let a = Filter::from_str(
            "(primary_category math.NT || (primary_category math.AG && category math.NT) || (primary_category math.CO && category math.NT)) && (first_version_encountered_after 2025-10-01 || first_version_submitted_after 2025-09-01)",
        );
        #[rustfmt::skip]
        let b = Filter::And(
            Box::new(Filter::Or(
                Box::new(Filter::Or(
                    Box::new(Filter::PrimaryCategoryIs("math.NT".to_string())),
                    Box::new(Filter::And(
                        Box::new(Filter::PrimaryCategoryIs("math.AG".to_string())),
                        Box::new(Filter::InCategory("math.NT".to_string()))
                    ))
                )),
                Box::new(Filter::And(
                    Box::new(Filter::PrimaryCategoryIs("math.CO".to_string())),
                    Box::new(Filter::InCategory("math.NT".to_string()))
                ))
            )),
            Box::new(Filter::Or(
                Box::new(Filter::FirstVersionEncounteredAfter("2025-10-01".to_string())),
                Box::new(Filter::FirstVersionSubmittedAfter("2025-09-01".to_string())),
            ))
        );
        assert_eq!(a.unwrap(), b);
    }
}
