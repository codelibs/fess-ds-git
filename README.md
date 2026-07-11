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
# delete_old_docs is a framework-level parameter handled by Fess's crawling infrastructure (DataIndexHelper),
# not specific to this plugin. When it is not "false", documents from a previous crawl of this DataConfig
# that were not re-indexed in the current session are deleted after the crawl finishes -- this is what
# prunes files removed/renamed upstream. Setting it to "false" disables that cleanup, which is safer while
# testing a new configuration (or, e.g., during a fail-fast error triggered by the new fetch/checkout
# error paths in this plugin) since a failed/aborted run will not wipe out the previously-indexed documents.
# Note: this plugin's fail-fast checks only stop ITS OWN per-file deletes; Fess's crawling infrastructure
# runs deleteOldDocs() unconditionally after every crawl attempt (success or failure) unless this is
# "false", so "false" is required, not just safer, for a failed/aborted run to leave previously-indexed
# documents intact.
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

Note: the `username`/`password` parameters (used only for Git authentication) are intentionally excluded
from the script-evaluation context, so they cannot be referenced from the `Script` mapping above (e.g.
`digest=username` will evaluate to `null`). This is a security fix -- previously they were available to
custom scripts and could be inadvertently indexed. Scripts that relied on `username`/`password` fields need
to be updated to no longer reference them.

### Persistent Repository (`repository_path`)

By default the crawler clones into a fresh temporary directory on every run and deletes it afterwards.
Set `repository_path` to a persistent directory to keep the local clone between runs:

```
repository_path=/var/lib/fess/git/fess-ds-git
```

With a persistent path, each run only fetches new commits instead of cloning from scratch, which makes
repeated/incremental crawls of large repositories considerably faster. The trade-off is disk usage: the
clone (all fetched branches and their history) stays on disk between runs.

Caveats:

- The advisory lock that protects `repository_path` across overlapping crawls is implemented with
  `FileChannel.tryLock()`, an OS-level file lock. Its behavior on network filesystems (NFS/SMB/etc.) is
  filesystem- and OS-dependent and is not guaranteed to provide exclusion across different hosts. For
  multi-node deployments, prefer local disk for `repository_path`, or independently verify file-locking
  semantics on your target filesystem before relying on it.
- Stale-lock cleanup assumes this plugin's own advisory lock is the only coordination mechanism for the
  directory. Do not point `repository_path` at a directory that other tools (a manual `git gc`/`git fsck`,
  backup scripts, etc.) might also operate on concurrently: once the lock is held, cleanup deletes any
  `*.lock` file found under `.git`, regardless of who created it.
- Windows file-lock and file-deletion semantics for this feature have not been specifically verified.

