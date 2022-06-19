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
delete_old_docs=false
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

