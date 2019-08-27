Git Data Store for Fess [![Build Status](https://travis-ci.org/codelibs/fess-ds-git.svg?branch=master)](https://travis-ci.org/codelibs/fess-ds-git)
==========================

## Overview

Git Data Store is an extension for Fess Data Store Crawling.

## Download

See [Maven Repository](http://central.maven.org/maven2/org/codelibs/fess/fess-ds-git/).

## Getting Started

### Installation

See [Plugin](https://fess.codelibs.org/13.3/admin/plugin-guide.html) page.

### Sample DataStore Setting

Parameter:

```
uri=https://github.com/codelibs/fess.git
extractors=text/.*:textExtractor,application/xml:textExtractor,application/javascript:textExtractor,
```

Script:

```
url="https://github.com/codelibs/fess/blob/master/" + path
host="github.com"
site="github.com/codelibs/fess/" + path
title=name
content=content
cache=""
digest=""
anchor=
content_length=contentLength
last_modified=new java.util.Date()
mimetype=mimetype
```

