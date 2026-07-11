Git Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-git/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-git/actions/workflows/maven.yml)
==========================

## Overview

Git Data Store is an extension for Fess Data Store Crawling.

## Download

See [Maven Repository](https://repo1.maven.org/maven2/org/codelibs/fess/fess-ds-git/).

## Getting Started

### Installation

See [Plugin](https://fess.codelibs.org/13.3/admin/plugin-guide.html) page.

### Sample DataStore Setting

Parameter:

```
uri=https://github.com/codelibs/fess-ds-git.git
base_url=https://github.com/codelibs/fess/blob/master/
extractors=text/.*:textExtractor,application/xml:textExtractor,application/javascript:textExtractor,
prev_commit_id=
```

Script:

```
url=url
host="github.com"
site="github.com/codelibs/fess-ds-git/" + path
title=name
content=content
cache=""
digest=author.toExternalString()
anchor=
content_length=contentLength
last_modified=timestamp
mimetype=mimetype
```

### Persistent Repository (`repository_path`)

By default the crawler clones into a fresh temporary directory on every run and deletes it afterwards.
Set `repository_path` to a persistent directory to keep the local clone between runs:

```
repository_path=/var/lib/fess/git/fess-ds-git
```

With a persistent path, each run only fetches new commits instead of cloning from scratch, which makes
repeated/incremental crawls of large repositories considerably faster. The trade-off is disk usage: the
clone (all fetched branches and their history) stays on disk between runs.

