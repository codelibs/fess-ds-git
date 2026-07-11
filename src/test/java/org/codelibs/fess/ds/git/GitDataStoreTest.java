/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.git;

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.codelibs.fess.crawler.entity.ExtractData;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.git.UnitDsTestCase;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

public class GitDataStoreTest extends UnitDsTestCase {

    private final List<File> tempDirs = new ArrayList<>();

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        for (final File dir : tempDirs) {
            deleteDirectory(dir);
        }
        tempDirs.clear();
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    /**
     * Creates a local scratch Git repository with the given initial branch and file contents,
     * committing all files in a single commit. The returned directory can be used as a Git {@code uri}.
     */
    private File createLocalRepo(final String initialBranch, final Map<String, String> files) throws Exception {
        final File dir = Files.createTempDirectory("fess-ds-git-src-").toFile();
        tempDirs.add(dir);
        try (Git git = Git.init().setInitialBranch(initialBranch).setDirectory(dir).call()) {
            addAndCommit(git, dir, files, "initial commit");
        }
        return dir;
    }

    /** Writes the given files into the repository working tree and creates a single commit. */
    private RevCommit addAndCommit(final Git git, final File dir, final Map<String, String> files, final String message) throws Exception {
        for (final Map.Entry<String, String> entry : files.entrySet()) {
            final File f = new File(dir, entry.getKey());
            if (f.getParentFile() != null) {
                f.getParentFile().mkdirs();
            }
            try (FileOutputStream out = new FileOutputStream(f)) {
                out.write(entry.getValue().getBytes());
            }
            git.add().addFilepattern(entry.getKey()).call();
        }
        return git.commit()
                .setMessage(message)
                .setAuthor("Test Author", "author@example.com")
                .setCommitter("Test Committer", "committer@example.com")
                .call();
    }

    @Test
    public void test_storeData() {
        DataStoreParams params = new DataStoreParams();
        params.put("uri", "https://github.com/codelibs/fess-ds-git.git");
        params.put("base_url", "https://github.com/codelibs/fess-ds-git/blob/master/");
        params.put("extractors",
                "text/.*:textExtractor,application/xml:textExtractor,application/javascript:textExtractor,application/json:textExtractor,application/x-sh:textExtractor,application/x-bat:textExtractor,audio/.*:filenameExtractor,chemical/.*:filenameExtractor,image/.*:filenameExtractor,model/.*:filenameExtractor,video/.*:filenameExtractor,");
        final List<String> urlList = new ArrayList<>();
        GitDataStore dataStore = new GitDataStore() {
            @Override
            protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
                return new MockUrlFilter();
            }

            @Override
            protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap) {
                final DiffEntry diffEntry = (DiffEntry) configMap.get(DIFF_ENTRY);
                final String path = diffEntry.getNewPath();
                urlList.add(path);
            }
        };
        dataStore.storeData(null, null, params, null, null);

        assertTrue(urlList.stream().anyMatch(s -> s.endsWith("pom.xml")));
        assertTrue(urlList.size() > 0);
    }

    @Test
    public void test_storeData_withoutUri() {
        DataStoreParams params = new DataStoreParams();
        GitDataStore dataStore = new GitDataStore();
        try {
            dataStore.storeData(null, null, params, null, null);
            fail("Expected DataStoreException");
        } catch (DataStoreException e) {
            assertEquals("uri is required.", e.getMessage());
        }
    }

    // Fix #2: the remote's actual default branch (e.g. "main") must be resolved and checked out,
    // instead of blindly trusting the JGit-default local "HEAD" (refs/heads/master), which would
    // otherwise throw RefNotFoundException/NoHeadException on every run.
    @Test
    public void test_storeData_defaultBranchMain() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("README.md", "hello world");
        files.put("pom.xml", "<project/>");
        final File remote = createLocalRepo("main", files);

        final DataStoreParams params = new DataStoreParams();
        params.put("uri", remote.getAbsolutePath());
        final List<String> urlList = new ArrayList<>();
        final GitDataStore dataStore = new GitDataStore() {
            @Override
            protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
                return new MockUrlFilter();
            }

            @Override
            protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap) {
                urlList.add(((DiffEntry) configMap.get(DIFF_ENTRY)).getNewPath());
            }
        };
        dataStore.storeData(null, null, params, null, null);

        assertTrue(urlList.contains("README.md"));
        assertTrue(urlList.contains("pom.xml"));
    }

    /** Creates a local repo on branch "main" with two commits; returns [repoDir, firstCommitSha]. HEAD points at the second commit. */
    private String[] createTwoCommitRepo() throws Exception {
        final File dir = Files.createTempDirectory("fess-ds-git-src-").toFile();
        tempDirs.add(dir);
        final String firstCommit;
        try (Git git = Git.init().setInitialBranch("main").setDirectory(dir).call()) {
            final Map<String, String> first = new LinkedHashMap<>();
            first.put("a.txt", "first");
            firstCommit = addAndCommit(git, dir, first, "c1").name();
            final Map<String, String> second = new LinkedHashMap<>();
            second.put("b.txt", "second");
            addAndCommit(git, dir, second, "c2");
        }
        return new String[] { dir.getAbsolutePath(), firstCommit };
    }

    private GitDataStore newCollectingDataStore(final List<String> urlList) {
        return new GitDataStore() {
            @Override
            protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
                return new MockUrlFilter();
            }

            @Override
            protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap) {
                urlList.add(((DiffEntry) configMap.get(DIFF_ENTRY)).getNewPath());
            }
        };
    }

    // Fix #1: an unresolvable commit_id must fail fast with a clear message, before any diff/delete can run.
    @Test
    public void test_resolveToCommitId() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("a.txt", "a");
        final File repoDir = createLocalRepo("main", files);
        final GitDataStore dataStore = new GitDataStore();
        try (Git git = Git.open(repoDir)) {
            final Repository repo = git.getRepository();
            assertNotNull(dataStore.resolveToCommitId(repo, "HEAD", "refs/heads/main"));
            try {
                dataStore.resolveToCommitId(repo, "ghost-branch", "refs/heads/ghost-branch");
                fail("Expected DataStoreException");
            } catch (final DataStoreException e) {
                assertTrue(e.getMessage().contains("Could not resolve commit_id 'ghost-branch'"));
            }
        }
    }

    // Fix #3: a stale JGit lock file left by a killed crawl under a persistent repository_path must be
    // removed automatically so the next run succeeds instead of failing identically forever.
    @Test
    public void test_storeData_removesStaleLockFile() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("a.txt", "a");
        final File remote = createLocalRepo("main", files);

        final File persistent = Files.createTempDirectory("fess-ds-git-persist-").toFile();
        tempDirs.add(persistent);
        try (Repository repo = FileRepositoryBuilder.create(new File(persistent, ".git"))) {
            repo.create();
        }
        final File refsHeads = new File(persistent, ".git/refs/heads");
        refsHeads.mkdirs();
        final File staleLock = new File(refsHeads, "main.lock");
        assertTrue(staleLock.createNewFile());

        final DataStoreParams params = new DataStoreParams();
        params.put("uri", remote.getAbsolutePath());
        params.put("repository_path", persistent.getAbsolutePath());
        final List<String> urlList = new ArrayList<>();
        newCollectingDataStore(urlList).storeData(null, null, params, null, null);

        assertTrue(urlList.contains("a.txt"));
        assertFalse(staleLock.exists());
    }

    // Fix #3: while another crawl genuinely holds the advisory lock, a concurrent run must fail fast with a
    // clear message rather than corrupting the shared repository_path.
    @Test
    public void test_storeData_concurrentLockFailsFast() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("a.txt", "a");
        final File remote = createLocalRepo("main", files);

        final File persistent = Files.createTempDirectory("fess-ds-git-persist-").toFile();
        tempDirs.add(persistent);
        final File marker = new File(persistent, ".fess-ds-git.lock");
        try (FileChannel channel = FileChannel.open(marker.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock held = channel.lock()) {
            final DataStoreParams params = new DataStoreParams();
            params.put("uri", remote.getAbsolutePath());
            params.put("repository_path", persistent.getAbsolutePath());
            final List<String> urlList = new ArrayList<>();
            try {
                newCollectingDataStore(urlList).storeData(null, null, params, null, null);
                fail("Expected DataStoreException");
            } catch (final DataStoreException e) {
                final String msg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                assertTrue(msg != null && msg.contains("Another crawl appears to be using repository_path"));
            }
        }
    }

    // Fix #4: an unchanged uri/branch (prev_source_ref matches) keeps using prev_commit_id for an incremental diff.
    @Test
    public void test_storeData_incrementalWhenSourceUnchanged() throws Exception {
        final String[] repo = createTwoCommitRepo();
        final String remotePath = repo[0];
        final String firstCommit = repo[1];

        final DataStoreParams params = new DataStoreParams();
        params.put("uri", remotePath);
        params.put("prev_commit_id", firstCommit);
        params.put("prev_source_ref", remotePath + "#refs/heads/main");
        final List<String> urlList = new ArrayList<>();
        newCollectingDataStore(urlList).storeData(null, null, params, null, null);

        assertTrue(urlList.contains("b.txt"));
        assertFalse(urlList.contains("a.txt"));
    }

    // Fix #4: a changed uri (prev_source_ref no longer matches) ignores the stale prev_commit_id and does a full reindex.
    @Test
    public void test_storeData_fullReindexWhenSourceChanged() throws Exception {
        final String[] repo = createTwoCommitRepo();
        final String remotePath = repo[0];
        final String firstCommit = repo[1];

        final DataStoreParams params = new DataStoreParams();
        params.put("uri", remotePath);
        params.put("prev_commit_id", firstCommit);
        params.put("prev_source_ref", "https://other.example.com/repo.git#refs/heads/main");
        final List<String> urlList = new ArrayList<>();
        newCollectingDataStore(urlList).storeData(null, null, params, null, null);

        assertTrue(urlList.contains("a.txt"));
        assertTrue(urlList.contains("b.txt"));
    }

    // Fix #4 (predicate): isSameSource trusts prev_commit_id only when the recorded source ref matches;
    // a blank recorded ref (pre-fix configs / manual prev_commit_id) is treated as a match for compatibility.
    @Test
    public void test_isSameSource() {
        final GitDataStore dataStore = new GitDataStore();
        assertTrue(dataStore.isSameSource(null, "uri#refs/heads/main"));
        assertTrue(dataStore.isSameSource("", "uri#refs/heads/main"));
        assertTrue(dataStore.isSameSource("  ", "uri#refs/heads/main"));
        assertTrue(dataStore.isSameSource("uri#refs/heads/main", "uri#refs/heads/main"));
        assertFalse(dataStore.isSameSource("uri#refs/heads/dev", "uri#refs/heads/main"));
        assertFalse(dataStore.isSameSource("other#refs/heads/main", "uri#refs/heads/main"));
    }

    // Fix #4 (write-back): the persisted prev_source_ref must be BYTE-IDENTICAL to the currentSourceRef the
    // read side (test_storeData_incrementalWhenSourceUnchanged hardcodes "<uri>#refs/heads/main") later compares
    // against; otherwise the two halves each pass their own tests while incremental detection silently breaks.
    // This also exercises the `if (dataConfig != null) updateDataConfig(...)` branch that the null-dataConfig
    // storeData tests skip entirely (DataConfigBhv is never touched because updateDataConfig is overridden).
    @Test
    public void test_storeData_persistedSourceRefMatchesReadSide() throws Exception {
        final String[] repo = createTwoCommitRepo();
        final String remotePath = repo[0];

        final DataStoreParams params = new DataStoreParams();
        params.put("uri", remotePath);

        final AtomicReference<String> capturedSourceRef = new AtomicReference<>();
        final AtomicReference<ObjectId> capturedCommitId = new AtomicReference<>();
        final GitDataStore dataStore = new GitDataStore() {
            @Override
            protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
                return new MockUrlFilter();
            }

            @Override
            protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap) {
                // no-op: this test only cares about the write-back of prev_source_ref.
            }

            @Override
            protected void updateDataConfig(final DataConfig dc, final String sourceRef, final ObjectId toCommitId) {
                capturedSourceRef.set(sourceRef);
                capturedCommitId.set(toCommitId);
            }
        };
        dataStore.storeData(new DataConfig(), null, params, null, null);

        assertEquals(remotePath + "#refs/heads/main", capturedSourceRef.get());
        assertNotNull(capturedCommitId.get());
    }

    // Fix #4 (write-back): first-ever persist appends both prev_commit_id and prev_source_ref, preserving order.
    @Test
    public void test_buildHandlerParameter_appendsWhenAbsent() {
        final GitDataStore dataStore = new GitDataStore();
        final Map<String, String> handlerParameterMap = new LinkedHashMap<>();
        handlerParameterMap.put("uri", "https://example.com/repo.git");
        handlerParameterMap.put("base_url", "https://example.com/repo/blob/main/");

        final String result =
                dataStore.buildHandlerParameter(handlerParameterMap, "abc123", "https://example.com/repo.git#refs/heads/main");

        assertEquals("uri=https://example.com/repo.git\n" + "base_url=https://example.com/repo/blob/main/\n" + "prev_commit_id=abc123\n"
                + "prev_source_ref=https://example.com/repo.git#refs/heads/main", result);
    }

    // Fix #4 (write-back): a repeat run updates existing prev_commit_id/prev_source_ref in place, never duplicating.
    @Test
    public void test_buildHandlerParameter_updatesInPlace() {
        final GitDataStore dataStore = new GitDataStore();
        final Map<String, String> handlerParameterMap = new LinkedHashMap<>();
        handlerParameterMap.put("uri", "https://example.com/repo.git");
        handlerParameterMap.put("prev_commit_id", "OLD_COMMIT");
        handlerParameterMap.put("prev_source_ref", "https://example.com/repo.git#refs/heads/OLD");

        final String result =
                dataStore.buildHandlerParameter(handlerParameterMap, "NEW_COMMIT", "https://example.com/repo.git#refs/heads/main");

        assertEquals("uri=https://example.com/repo.git\n" + "prev_commit_id=NEW_COMMIT\n"
                + "prev_source_ref=https://example.com/repo.git#refs/heads/main", result);
        assertEquals(1L, result.lines().filter(l -> l.startsWith("prev_commit_id=")).count());
        assertEquals(1L, result.lines().filter(l -> l.startsWith("prev_source_ref=")).count());
    }

    // Fix #4 (write-back): the backward-compat upgrade path -- a pre-fix config has prev_commit_id but no
    // prev_source_ref -- must update prev_commit_id in place AND append prev_source_ref (no duplication).
    @Test
    public void test_buildHandlerParameter_mixedUpgrade() {
        final GitDataStore dataStore = new GitDataStore();
        final Map<String, String> handlerParameterMap = new LinkedHashMap<>();
        handlerParameterMap.put("uri", "https://example.com/repo.git");
        handlerParameterMap.put("prev_commit_id", "OLD_COMMIT");

        final String result =
                dataStore.buildHandlerParameter(handlerParameterMap, "NEW_COMMIT", "https://example.com/repo.git#refs/heads/main");

        assertEquals("uri=https://example.com/repo.git\n" + "prev_commit_id=NEW_COMMIT\n"
                + "prev_source_ref=https://example.com/repo.git#refs/heads/main", result);
        assertEquals(1L, result.lines().filter(l -> l.startsWith("prev_commit_id=")).count());
    }

    // Fix #4 (write-back): the persisted string must round-trip back through DataConfig.getHandlerParameterMap()
    // (ParameterUtil.parse) with the embedded '#' preserved, so a later run reads back the exact source ref it
    // wrote and isSameSource returns true. A fresh DataConfig is used because setHandlerParameter does NOT reset
    // the lazily-cached handlerParameterMap.
    @Test
    public void test_buildHandlerParameter_roundTripsThroughDataConfig() {
        final GitDataStore dataStore = new GitDataStore();
        final String sourceRef = "https://github.com/codelibs/fess.git#refs/heads/main";
        final Map<String, String> handlerParameterMap = new LinkedHashMap<>();
        handlerParameterMap.put("uri", "https://github.com/codelibs/fess.git");

        final String result = dataStore.buildHandlerParameter(handlerParameterMap, "abc123", sourceRef);

        final DataConfig dataConfig = new DataConfig();
        dataConfig.setHandlerParameter(result);
        final Map<String, String> parsed = dataConfig.getHandlerParameterMap();
        assertEquals("abc123", parsed.get("prev_commit_id"));
        assertEquals(sourceRef, parsed.get("prev_source_ref"));
        // The value read back is exactly what the read side compares against.
        assertTrue(dataStore.isSameSource(parsed.get("prev_source_ref"), sourceRef));
    }

    // Fix #2 (no regression): an explicitly configured commit_id/branch (non-HEAD) bypasses remote HEAD
    // resolution entirely and is returned unchanged, so the common non-default case behaves exactly as before.
    @Test
    public void test_resolveDefaultBranch_explicitCommitIdBypass() throws Exception {
        final GitDataStore dataStore = new GitDataStore();
        // The early return fires before fetchResult/repository are touched, so null args are safe here.
        assertEquals("refs/heads/dev", dataStore.resolveDefaultBranch(null, null, "refs/heads/dev"));
        assertEquals("v1.2.3", dataStore.resolveDefaultBranch(null, null, "v1.2.3"));
        assertEquals("0123456789abcdef0123456789abcdef01234567",
                dataStore.resolveDefaultBranch(null, null, "0123456789abcdef0123456789abcdef01234567"));
    }

    // Fix #8: a blank base_url must emit a WARN so operators know indexed URLs will be empty and delete/rename
    // tracking is skipped.
    @Test
    public void test_storeData_warnsWhenBaseUrlBlank() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("a.txt", "a");
        final File remote = createLocalRepo("main", files);

        final String loggerName = GitDataStore.class.getName();
        final LoggerContext ctx = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        final Configuration cfg = ctx.getConfiguration();
        final LoggerConfig loggerCfg = cfg.getLoggerConfig(loggerName);
        final Level originalLevel = loggerCfg.getLevel();
        final List<LogEvent> captured = new ArrayList<>();
        final AbstractAppender listAppender =
                new AbstractAppender("git-ds-test-appender", null, PatternLayout.createDefaultLayout(), true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(final LogEvent event) {
                        if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
                            captured.add(event.toImmutable());
                        }
                    }
                };
        listAppender.start();
        loggerCfg.addAppender(listAppender, Level.WARN, null);
        loggerCfg.setLevel(Level.WARN);
        ctx.updateLoggers();
        try {
            final DataStoreParams params = new DataStoreParams();
            params.put("uri", remote.getAbsolutePath());
            // base_url intentionally omitted so it is blank.
            final List<String> urlList = new ArrayList<>();
            newCollectingDataStore(urlList).storeData(null, null, params, null, null);

            final long warnCount = captured.stream()
                    .filter(e -> loggerName.equals(e.getLoggerName()))
                    .filter(e -> Level.WARN.equals(e.getLevel()))
                    .filter(e -> {
                        final String msg = e.getMessage().getFormattedMessage();
                        return msg != null && msg.contains("base_url is blank");
                    })
                    .count();
            assertEquals(1L, warnCount);
        } finally {
            loggerCfg.removeAppender("git-ds-test-appender");
            loggerCfg.setLevel(originalLevel);
            ctx.updateLoggers();
            listAppender.stop();
        }
    }

    // Fix #5: getRevCommit closes the RevWalk internally; author/committer/timestamp must remain readable afterward.
    @Test
    public void test_getRevCommit_readableAfterWalkClosed() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("a.txt", "content");
        final File repoDir = createLocalRepo("main", files);
        final GitDataStore dataStore = new GitDataStore();
        try (Git git = Git.open(repoDir)) {
            final Map<String, Object> configMap = new HashMap<>();
            configMap.put("git", git);
            configMap.put("current_commit_id", git.getRepository().resolve("refs/heads/main"));
            final RevCommit commit = dataStore.getRevCommit(configMap, "a.txt");
            assertNotNull(commit.getAuthorIdent());
            assertEquals("Test Author", commit.getAuthorIdent().getName());
            assertEquals("Test Committer", commit.getCommitterIdent().getName());
            assertTrue(commit.getCommitTime() > 0);
        }
    }

    // Fix #6: credentials embedded in a Git URI must be redacted before logging.
    @Test
    public void test_redactUrl() {
        final GitDataStore dataStore = new GitDataStore();
        assertEquals("https://github.com/codelibs/fess.git", dataStore.redactUrl("https://user:token@github.com/codelibs/fess.git"));
        assertEquals("https://github.com/codelibs/fess.git", dataStore.redactUrl("https://github.com/codelibs/fess.git"));
        // scp-style remotes have no URI authority component, so there is nothing to redact and they are returned as-is.
        assertEquals("git@github.com:codelibs/fess.git", dataStore.redactUrl("git@github.com:codelibs/fess.git"));
        assertEquals("", dataStore.redactUrl(""));
    }

    // Fix #6 (residual): the "uri" placed into resultMap is DEBUG-logged and available to the script
    // mapping (so it can be indexed), therefore it must have any embedded credentials stripped. The raw
    // credential-bearing uri kept in configMap (used earlier by git fetch/checkout) must stay untouched.
    @Test
    public void test_processFile_redactsUriInResultMap() throws Exception {
        final String credentialUri = "https://user:token@github.com/codelibs/fess.git";
        final AtomicReference<Map<String, Object>> capturedResultMap = new AtomicReference<>();
        final DataStoreParams paramMap = new DataStoreParams();

        final Map<String, Object> configMap = runProcessFileCapturingResultMap(credentialUri, paramMap, capturedResultMap);

        final Map<String, Object> resultMap = capturedResultMap.get();
        assertNotNull(resultMap);
        // The logged/indexed uri has its user:token@ credentials stripped, keeping host and path.
        assertEquals("https://github.com/codelibs/fess.git", resultMap.get("uri"));
        assertFalse(resultMap.get("uri").toString().contains("token"));
        // The raw uri that git fetch/checkout rely on is left untouched in configMap.
        assertEquals(credentialUri, configMap.get("uri"));
    }

    // Fix #7: username/password passed as data-store params must be stripped from resultMap so they are
    // never surfaced to the script mapping / indexed document (defense-in-depth alongside uri redaction).
    @Test
    public void test_processFile_stripsCredentialsFromResultMap() throws Exception {
        final String uri = "https://github.com/codelibs/fess.git";
        final AtomicReference<Map<String, Object>> capturedResultMap = new AtomicReference<>();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("username", "alice");
        paramMap.put("password", "s3cr3t-token");

        runProcessFileCapturingResultMap(uri, paramMap, capturedResultMap);

        final Map<String, Object> resultMap = capturedResultMap.get();
        assertNotNull(resultMap);
        // The credential keys themselves must be absent.
        assertFalse(resultMap.containsKey("username"));
        assertFalse(resultMap.containsKey("password"));
        // And neither credential value must leak in under any other key.
        assertFalse(resultMap.values().stream().anyMatch(v -> "alice".equals(v)));
        assertFalse(resultMap.values().stream().anyMatch(v -> "s3cr3t-token".equals(v)));
    }

    /**
     * Drives {@link GitDataStore#processFile} against a fresh single-file local repository, capturing the
     * {@code resultMap} handed to {@code convertValue}. The credential-bearing {@code uri} is deliberately
     * decoupled from the local repository backing REPOSITORY/DIFF_ENTRY, exactly as in production (the uri
     * string and the temp clone are separate objects); driving through {@code storeData} would instead force
     * the fetch uri to equal the resultMap uri. Returns the {@code configMap} so callers can assert the raw
     * uri kept for git fetch/checkout was left untouched.
     */
    private Map<String, Object> runProcessFileCapturingResultMap(final String uri, final DataStoreParams paramMap,
            final AtomicReference<Map<String, Object>> capturedResultMap) throws Exception {
        // The real processFile pipeline uses CrawlerStatsHelper (which in turn uses SystemHelper); register
        // initialized instances so it can run inside this unit-test container.
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");

        final Map<String, String> files = new LinkedHashMap<>();
        files.put("a.txt", "hello world");
        final File repoDir = createLocalRepo("main", files);

        try (Git git = Git.open(repoDir)) {
            final Repository repository = git.getRepository();
            final ObjectId head = repository.resolve("refs/heads/main");

            final DiffEntry addEntry;
            try (DiffFormatter diffFormatter = new DiffFormatter(null)) {
                diffFormatter.setRepository(repository);
                // fromCommitId == null diffs against the empty tree, so every committed file appears as ADD.
                addEntry = diffFormatter.scan(null, head).get(0);
            }

            final Map<String, Object> configMap = new HashMap<>();
            configMap.put("uri", uri);
            configMap.put("diff_entry", addEntry);
            configMap.put("repository", repository);
            configMap.put("git", git);
            configMap.put("current_commit_id", head);
            configMap.put("max_size", 10000000L);
            configMap.put("cache_threshold", 1000000);
            configMap.put("read_interval", 0L);

            final GitDataStore dataStore = new GitDataStore() {
                @Override
                protected String getMimeType(final String filename, final DeferredFileOutputStream out) {
                    return "text/plain";
                }

                @Override
                protected Extractor getExtractor(final String mimeType, final Map<String, Object> configMap) {
                    return (in, params) -> new ExtractData("dummy content");
                }

                @Override
                protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                    capturedResultMap.set(resultMap);
                    return null;
                }
            };

            final IndexUpdateCallback callback = new IndexUpdateCallback() {
                @Override
                public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                    // no-op
                }

                @Override
                public long getExecuteTime() {
                    return 0;
                }

                @Override
                public long getDocumentSize() {
                    return 0;
                }

                @Override
                public void commit() {
                    // no-op
                }
            };

            final Map<String, String> scriptMap = new HashMap<>();
            scriptMap.put("url", "url"); // one entry so convertValue is invoked with the resultMap

            dataStore.processFile(null, callback, paramMap, scriptMap, new HashMap<>(), configMap);
            return configMap;
        }
    }

    @Test
    public void test_getFileName() {
        GitDataStore dataStore = new GitDataStore();

        // Test with path containing directories
        assertEquals("file.txt", dataStore.getFileName("path/to/file.txt"));
        assertEquals("file.txt", dataStore.getFileName("a/b/c/file.txt"));

        // Test with path without directories
        assertEquals("file.txt", dataStore.getFileName("file.txt"));

        // Test with empty string
        assertEquals("", dataStore.getFileName(""));

        // Test with path ending with slash
        assertEquals("", dataStore.getFileName("path/to/"));
    }

    @Test
    public void test_getUrl_withBaseUrl() {
        GitDataStore dataStore = new GitDataStore();
        DataStoreParams params = new DataStoreParams();
        params.put("base_url", "https://github.com/user/repo/blob/main/");

        String url = dataStore.getUrl(params, "src/main/java/Test.java");
        assertEquals("https://github.com/user/repo/blob/main/src/main/java/Test.java", url);
    }

    @Test
    public void test_getUrl_withoutBaseUrl() {
        GitDataStore dataStore = new GitDataStore();
        DataStoreParams params = new DataStoreParams();

        String url = dataStore.getUrl(params, "src/main/java/Test.java");
        assertEquals("", url);
    }

    @Test
    public void test_getUrl_withEmptyBaseUrl() {
        GitDataStore dataStore = new GitDataStore();
        DataStoreParams params = new DataStoreParams();
        params.put("base_url", "");

        String url = dataStore.getUrl(params, "src/main/java/Test.java");
        assertEquals("", url);
    }

    @Test
    public void test_createConfigMap_withDefaultValues() {
        GitDataStore dataStore = new GitDataStore();
        DataStoreParams params = new DataStoreParams();

        Map<String, Object> configMap = dataStore.createConfigMap(params);

        assertNotNull(configMap);
        assertEquals("", configMap.get("base_url"));
        assertEquals(1000000, configMap.get("cache_threshold"));
        assertEquals("tikaExtractor", configMap.get("default_extractor"));
        assertEquals(10000000L, configMap.get("max_size"));
        assertNotNull(configMap.get("repository"));
        assertNotNull(configMap.get("temp_repository_path"));

        // Clean up temporary repository
        File tempRepo = (File) configMap.get("temp_repository_path");
        if (tempRepo != null && tempRepo.exists()) {
            deleteDirectory(tempRepo);
        }
    }

    @Test
    public void test_createConfigMap_withCustomValues() {
        GitDataStore dataStore = new GitDataStore();
        DataStoreParams params = new DataStoreParams();
        params.put("base_url", "https://example.com/");
        params.put("cache_threshold", "2000000");
        params.put("default_extractor", "customExtractor");
        params.put("max_size", "20000000");

        Map<String, Object> configMap = dataStore.createConfigMap(params);

        assertNotNull(configMap);
        assertEquals("https://example.com/", configMap.get("base_url"));
        assertEquals(2000000, configMap.get("cache_threshold"));
        assertEquals("customExtractor", configMap.get("default_extractor"));
        assertEquals(20000000L, configMap.get("max_size"));

        // Clean up temporary repository
        File tempRepo = (File) configMap.get("temp_repository_path");
        if (tempRepo != null && tempRepo.exists()) {
            deleteDirectory(tempRepo);
        }
    }

    @Test
    public void test_createConfigMap_withExtractors() {
        GitDataStore dataStore = new GitDataStore();
        DataStoreParams params = new DataStoreParams();
        params.put("extractors", "text/.*:textExtractor,application/pdf:pdfExtractor");

        Map<String, Object> configMap = dataStore.createConfigMap(params);

        assertNotNull(configMap);
        assertNotNull(configMap.get("extractors"));

        // Clean up temporary repository
        File tempRepo = (File) configMap.get("temp_repository_path");
        if (tempRepo != null && tempRepo.exists()) {
            deleteDirectory(tempRepo);
        }
    }

    @Test
    public void test_getContentInputStream_inMemory() throws IOException {
        GitDataStore dataStore = new GitDataStore();

        // Create a small output stream that stays in memory (threshold = 1000 bytes)
        DeferredFileOutputStream dfos = new DeferredFileOutputStream(1000, "test-", ".tmp", null);
        String testData = "test content";
        dfos.write(testData.getBytes());
        dfos.flush();

        assertTrue(dfos.isInMemory());

        InputStream is = dataStore.getContentInputStream(dfos);
        assertNotNull(is);

        byte[] buffer = new byte[testData.length()];
        int bytesRead = is.read(buffer);
        assertEquals(testData.length(), bytesRead);
        assertEquals(testData, new String(buffer));

        is.close();
    }

    @Test
    public void test_getContentInputStream_onDisk() throws IOException {
        GitDataStore dataStore = new GitDataStore();

        // Create a large output stream that spills to disk (threshold = 10 bytes)
        DeferredFileOutputStream dfos = new DeferredFileOutputStream(10, "test-", ".tmp", null);
        String testData = "This is a test content that exceeds the threshold";
        dfos.write(testData.getBytes());
        dfos.flush();

        assertFalse(dfos.isInMemory());
        assertNotNull(dfos.getFile());

        InputStream is = dataStore.getContentInputStream(dfos);
        assertNotNull(is);

        byte[] buffer = new byte[testData.length()];
        int bytesRead = is.read(buffer);
        assertEquals(testData.length(), bytesRead);
        assertEquals(testData, new String(buffer));

        is.close();

        // Clean up temp file
        File tempFile = dfos.getFile();
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    public void test_getUrlFilter_withIncludePattern() {
        GitDataStore dataStore = new GitDataStore() {
            @Override
            protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
                // Call parent implementation to test it
                UrlFilter filter = new MockUrlFilter();
                final String include = paramMap.getAsString("include_pattern");
                if (include != null && !include.isEmpty()) {
                    filter.addInclude(include);
                }
                return filter;
            }
        };

        DataStoreParams params = new DataStoreParams();
        params.put("include_pattern", ".*\\.java");

        UrlFilter filter = dataStore.getUrlFilter(params);
        assertNotNull(filter);
    }

    @Test
    public void test_getUrlFilter_withExcludePattern() {
        GitDataStore dataStore = new GitDataStore() {
            @Override
            protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
                // Call parent implementation to test it
                UrlFilter filter = new MockUrlFilter();
                final String exclude = paramMap.getAsString("exclude_pattern");
                if (exclude != null && !exclude.isEmpty()) {
                    filter.addExclude(exclude);
                }
                return filter;
            }
        };

        DataStoreParams params = new DataStoreParams();
        params.put("exclude_pattern", ".*\\.class");

        UrlFilter filter = dataStore.getUrlFilter(params);
        assertNotNull(filter);
    }

    @Test
    public void test_getUrlFilter_withBothPatterns() {
        GitDataStore dataStore = new GitDataStore() {
            @Override
            protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
                // Call parent implementation to test it
                UrlFilter filter = new MockUrlFilter();
                final String include = paramMap.getAsString("include_pattern");
                if (include != null && !include.isEmpty()) {
                    filter.addInclude(include);
                }
                final String exclude = paramMap.getAsString("exclude_pattern");
                if (exclude != null && !exclude.isEmpty()) {
                    filter.addExclude(exclude);
                }
                return filter;
            }
        };

        DataStoreParams params = new DataStoreParams();
        params.put("include_pattern", ".*\\.java");
        params.put("exclude_pattern", ".*Test\\.java");

        UrlFilter filter = dataStore.getUrlFilter(params);
        assertNotNull(filter);
    }

    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }

    public static class MockUrlFilter implements UrlFilter {

        @Override
        public void init(String sessionId) {
            // no-op
        }

        @Override
        public boolean match(String url) {
            return true;
        }

        @Override
        public void addInclude(String urlPattern) {
            // no-op
        }

        @Override
        public void addExclude(String urlPattern) {
            // no-op
        }

        @Override
        public void processUrl(String url) {
            // no-op
        }

        @Override
        public void clear() {
            // no-op
        }
    }
}
