# Introduction

arxiv-reader is a command line program to find and read articles on arXiv.

# Features

* Subscribe to arXiv categories, optionally use your own more specific filters.
* Download pdfs and sources.
* Bookmark articles.
* Save your own short notes on articles.
* Search articles by author, title, keywords, your own notes, ...
* Get "new version" or "journal reference" notifications for bookmarked articles.
* Check bibtex files for references that should be updated (new version / journal reference).
* All article metadata is saved locally (in an sqlite database) for fast offline use.
* Optionally keep your data in a git repository, automatically commit, push, pull.

# Installation

## Building from source

You first need to install the following two dependencies:

* rust / cargo: I have tested version 1.91, but other versions are likely to also work.
* sqlite: I have tested version 3.50.4, but other versions are likely to also work.

Run the following command to compile `arxiv-reader`:

```
cargo build -r
```

The resulting executable can be found in `./target/release`.

If you want shell completions, run `arxiv-reader generate-completions SHELL` and copy the output to the appropriate location.

# Setup

1. Create a directory for your data and set the environment variable `$ARXIV_READER_DIR` to this directory. (Default: ~/arxiv-reader)
2. Run `arxiv-reader init` to initialize the database.
3. Edit `$ARXIV_READER_DIR/config.toml`. In particular, pick `categories` to subscribe to and a filter for `new` articles to be notified about.
4. (Optionally, run `git init` to initialize a git repository in `$ARXIV_READER_DIR` and uncomment the corresponding lines in `$ARXIV_READER_DIR/config.toml` to automatically commit, pull, push.)
5. Run `arxiv-reader pull` to download metadata from arXiv.

# Operation

* Run `arxiv-reader pull` to download metadata from arXiv.
* Run `arxiv-reader news` to look at new articles.
* Run `arxiv-reader find` to find articles (locally).
* See `arxiv-reader help` for a list of other commands and `arxiv-reader help SUBCOMMAND` for help.
