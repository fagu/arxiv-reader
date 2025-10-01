use std::{
    ffi::OsStr,
    fs::{File, rename},
    io::{BufReader, BufWriter, ErrorKind},
    path::PathBuf,
};

use aho_corasick::{AhoCorasick, MatchKind};

/// Opens `file~`, then lets f write to it, closes the file, and then renames it to `file`.
/// This avoids problems with partially written files.
pub fn write_then_rename<F: FnOnce(&mut BufWriter<File>) -> anyhow::Result<()>>(
    file: PathBuf,
    f: F,
) -> anyhow::Result<()> {
    let mut tmp_file_name = file.file_name().unwrap().to_owned();
    tmp_file_name.push(OsStr::new("~"));
    let mut tmp_file = file.clone();
    tmp_file.set_file_name(tmp_file_name);
    {
        let file = File::create(&tmp_file)?;
        let mut writer = BufWriter::new(file);
        f(&mut writer)?;
    }
    rename(tmp_file, file)?;
    Ok(())
}

pub fn read_if_exists<R, F: FnOnce(&mut BufReader<File>) -> anyhow::Result<R>>(
    file: PathBuf,
    f: F,
) -> anyhow::Result<Option<R>> {
    match File::open(file) {
        Ok(file) => {
            let mut reader = BufReader::new(file);
            Ok(Some(f(&mut reader)?))
        }
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(err)?
            }
        }
    }
}

/// Mark matches in bold.
pub fn highlight_matches(
    line: &str,
    ascii_case_insensitive: bool,
    patterns: &Vec<String>,
) -> String {
    let mut builder = AhoCorasick::builder();
    builder.match_kind(MatchKind::LeftmostLongest);
    builder.ascii_case_insensitive(ascii_case_insensitive);
    let ac = builder.build(patterns).unwrap();
    let mut res = String::new();
    let mut i = 0;
    for mat in ac.find_iter(line) {
        assert!(mat.start() >= i);
        res += &line[i..mat.start()];
        res += termion::color::LightRed.fg_str();
        res += &line[mat.start()..mat.end()];
        res += termion::color::Reset.fg_str();
        i = mat.end();
    }
    res += &line[i..];
    res
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn highlight() {
        assert_eq!(
            highlight_matches(
                "abc def ghidef",
                false,
                &vec!["def".to_string(), "ghi".to_string()]
            ),
            "abc \u{1b}[38;5;9mdef\u{1b}[39m \u{1b}[38;5;9mghi\u{1b}[39m\u{1b}[38;5;9mdef\u{1b}[39m"
        );
    }
}
