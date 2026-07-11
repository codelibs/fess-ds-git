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
import org.eclipse.jgit.lib.Ref;
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

    // Hermetic fixture (no network/GitHub dependency): a local scratch repository standing in for the
    // previous live "https://github.com/codelibs/fess-ds-git.git" clone. Since @Test annotations are fixed
    // (they now actually run in CI), this test must not depend on network/GitHub availability.
    @Test
    public void test_storeData() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("pom.xml", "<project/>");
        files.put("README.md", "hello world");
        files.put("src/main/java/App.java", "public class App {}");
        final File remote = createLocalRepo("main", files);

        DataStoreParams params = new DataStoreParams();
        params.put("uri", remote.getAbsolutePath());
        params.put("base_url", "https://github.com/codelibs/fess-ds-git/blob/master/");
        params.put("extractors",
                "text/.*:textExtractor,application/xml:textExtractor,application/javascript:textExtractor,application/json:textExtractor,application/x-sh:textExtractor,application/x-bat:textExtractor,audio/.*:filenameExtractor,chemical/.*:filenameExtractor,image/.*:filenameExtractor,model/.*:filenameExtractor,video/.*:filenameExtractor,");
        final List<String> urlList = new ArrayList<>();
        GitDataStore dataStore = newCollectingDataStore(urlList);
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
                // The message reports the ref that actually failed to resolve (the resolvedCommitId), and
                // includes the originally configured commit_id for context.
                assertTrue(e.getMessage().contains("Could not resolve commit_id 'refs/heads/ghost-branch'"));
                assertTrue(e.getMessage().contains("configured commit_id was 'ghost-branch'"));
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

    // Bug fix (post-review): deleteStaleLockFiles()'s Files.walk() can throw an UNCHECKED
    // java.io.UncheckedIOException mid-traversal (e.g. AccessDeniedException on a subdirectory another tool
    // is concurrently touching -- exactly the scenario this PR's own README caveat warns about), which is NOT
    // caught by the existing `catch (final IOException e)` clause. Since lockRepositoryPath() ->
    // deleteStaleLockFiles() is called from storeData() with no surrounding try/catch, an uncaught
    // UncheckedIOException here would propagate straight out of storeData(), skipping the try/finally that
    // calls releaseRepositoryLock() -- leaking the just-acquired advisory FileLock for the life of the JVM
    // and blocking every future crawl of this repository_path.
    @Test
    public void test_storeData_staleLockScanIOErrorDoesNotLeakLock() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("a.txt", "a");
        final File remote = createLocalRepo("main", files);

        final File persistent = Files.createTempDirectory("fess-ds-git-persist-").toFile();
        tempDirs.add(persistent);
        try (Repository repo = FileRepositoryBuilder.create(new File(persistent, ".git"))) {
            repo.create();
        }
        // An unreadable/unsearchable subdirectory under .git makes Files.walk() throw UncheckedIOException
        // mid-traversal (AccessDeniedException while trying to list it), rather than at the initial walk()
        // call -- reproducing the real-world "another tool touching .git concurrently" failure mode.
        final File unreadableDir = new File(persistent, ".git/unreadable-by-another-tool");
        assertTrue(unreadableDir.mkdirs());
        unreadableDir.setExecutable(false);
        unreadableDir.setReadable(false);
        // On a platform/user (e.g. root) where directory permission bits are not enforced, this
        // reproduction cannot fire; skip rather than risk a flaky/misleading pass or failure.
        org.junit.jupiter.api.Assumptions.assumeFalse(unreadableDir.canRead(),
                "Directory read permission is not enforced for the current user; skipping.");

        try {
            final DataStoreParams params = new DataStoreParams();
            params.put("uri", remote.getAbsolutePath());
            params.put("repository_path", persistent.getAbsolutePath());
            final List<String> urlList = new ArrayList<>();
            newCollectingDataStore(urlList).storeData(null, null, params, null, null);

            // The scan failure must be swallowed (logged) and the crawl must complete normally.
            assertTrue(urlList.contains("a.txt"));

            // The advisory lock must have been released -- a fresh, independent lock attempt on the same
            // marker file (from this same JVM) must succeed. Pre-fix, the lock leaks and this would throw
            // OverlappingFileLockException.
            final File marker = new File(persistent, ".fess-ds-git.lock");
            try (FileChannel channel = FileChannel.open(marker.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    FileLock lock = channel.tryLock()) {
                assertNotNull(lock);
            }
        } finally {
            // Restore permissions so tearDown's deleteDirectory() can actually remove the temp tree.
            unreadableDir.setReadable(true);
            unreadableDir.setExecutable(true);
        }
    }

    // Fix #5: a first run killed mid-initialization can leave repository_path/.git as a partial skeleton.
    // JGit's repository.create() writes .git/config LAST (cfg.save()) and refuses to re-run once config
    // exists, so an interrupted create() leaves a .git that JGit cannot open (no object database / HEAD /
    // config). The pre-existing check only skipped create() when .git already existed, so every subsequent
    // run reused the unopenable partial repo and failed identically forever. The partial .git must be
    // discarded and re-created so the next run succeeds -- the same "repeated-crawl failure" class the
    // stale-.lock-file fix addresses, but for the narrower interrupted-initial-creation window.
    @Test
    public void test_storeData_recreatesIncompleteRepository() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("a.txt", "a");
        final File remote = createLocalRepo("main", files);

        final File persistent = Files.createTempDirectory("fess-ds-git-persist-").toFile();
        tempDirs.add(persistent);
        // Simulate a create() killed mid-initialization: .git exists with only refs/ (no HEAD, no object
        // database, no config). This is the hardest case for a heal -- a plain re-create() cannot repair it
        // because refs.create() throws (mkdir on the already-existing refs/), so the partial .git must be
        // deleted before re-creating.
        final File refsDir = new File(persistent, ".git/refs");
        assertTrue(refsDir.mkdirs());
        assertFalse(new File(persistent, ".git/config").exists());

        final DataStoreParams params = new DataStoreParams();
        params.put("uri", remote.getAbsolutePath());
        params.put("repository_path", persistent.getAbsolutePath());

        // Run 1: the partial repo must be healed and the crawl must complete (pre-fix, this run throws).
        final List<String> firstRun = new ArrayList<>();
        newCollectingDataStore(firstRun).storeData(null, null, params, null, null);
        assertTrue(firstRun.contains("a.txt"));
        assertTrue(new File(persistent, ".git/config").exists());

        // A sentinel written inside the now-valid .git proves run 2 does NOT re-delete it: because the
        // remote is always reachable here, a wrongful re-delete would still re-fetch and collect a.txt, so
        // "second run succeeds" alone cannot distinguish "left untouched" from "deleted and rebuilt".
        final File sentinel = new File(persistent, ".git/fess-heal-sentinel");
        assertTrue(sentinel.createNewFile());

        // Run 2: the now-valid repository (config present) must be left untouched and reused, not re-deleted.
        final List<String> secondRun = new ArrayList<>();
        newCollectingDataStore(secondRun).storeData(null, null, params, null, null);
        assertTrue(secondRun.contains("a.txt"));
        assertTrue(sentinel.exists());
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

    // Fix #11: for a brand-new (not-yet-existing) repository_path, the advisory lock must be acquired BEFORE
    // any repository creation happens (repository.create() writes HEAD/config/description/refs/objects
    // non-atomically). Before this fix, createConfigMap() (called before lockRepositoryPath()) would already
    // have fully initialized the on-disk repo by the time the lock was even attempted -- meaning the very
    // first concurrent use of a brand-new repository_path by two overlapping crawls was NOT protected by the
    // lock at all. This is verified deterministically (no real thread race, which would be flaky) by
    // instrumenting lockRepositoryPath() to record whether .git already exists at the moment it is called.
    @Test
    public void test_storeData_locksBeforeRepositoryCreate() throws Exception {
        final Map<String, String> files = new LinkedHashMap<>();
        files.put("a.txt", "a");
        final File remote = createLocalRepo("main", files);

        final File persistentParent = Files.createTempDirectory("fess-ds-git-persist-").toFile();
        tempDirs.add(persistentParent);
        final File persistent = new File(persistentParent, "brand-new-repo"); // does not exist at all yet

        final DataStoreParams params = new DataStoreParams();
        params.put("uri", remote.getAbsolutePath());
        params.put("repository_path", persistent.getAbsolutePath());

        final List<String> urlList = new ArrayList<>();
        final AtomicReference<Boolean> gitDirExistedAtLockTime = new AtomicReference<>();
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

            @Override
            protected void lockRepositoryPath(final File repositoryPath, final Map<String, Object> configMap) {
                gitDirExistedAtLockTime.set(new File(repositoryPath, ".git").exists());
                super.lockRepositoryPath(repositoryPath, configMap);
            }
        };
        dataStore.storeData(null, null, params, null, null);

        assertNotNull(gitDirExistedAtLockTime.get());
        assertFalse(gitDirExistedAtLockTime.get());
        assertTrue(urlList.contains("a.txt"));
    }

    // Fix #11 (regression guard): getUrlFilter() is resolved BEFORE the advisory lock is acquired specifically
    // so that a getUrlFilter() failure (e.g. ComponentNotFoundException, or CrawlerSystemException from
    // UrlFilterImpl#init) can never leave the lock held. A lock leaked here would never be released (nothing
    // in storeData's try/finally runs, since the failure happens before either exists), blocking every future
    // crawl of this repository_path until JVM restart -- exactly the repeated-crawl-failure class this PR
    // exists to eliminate. Proven by making getUrlFilter() throw and then confirming the marker lock is still
    // independently acquirable immediately afterward.
    @Test
    public void test_storeData_getUrlFilterFailureDoesNotLeakLock() throws Exception {
        final File persistent = Files.createTempDirectory("fess-ds-git-persist-").toFile();
        tempDirs.add(persistent);

        final DataStoreParams params = new DataStoreParams();
        params.put("uri", "https://example.invalid/repo.git"); // never reached
        params.put("repository_path", persistent.getAbsolutePath());

        final GitDataStore dataStore = new GitDataStore() {
            @Override
            protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
                throw new RuntimeException("simulated getUrlFilter failure");
            }
        };

        try {
            dataStore.storeData(null, null, params, null, null);
            fail("Expected RuntimeException");
        } catch (final RuntimeException e) {
            assertEquals("simulated getUrlFilter failure", e.getMessage());
        }

        final File marker = new File(persistent, ".fess-ds-git.lock");
        try (FileChannel channel = FileChannel.open(marker.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock lock = channel.tryLock()) {
            // Non-null proves the lock was never acquired (and thus never leaked) by the failed storeData call.
            assertNotNull(lock);
        }
    }

    // Fix #10: JGit's own TransportException embeds the target URI in its message with only the PASSWORD
    // stripped (via URIish#setPass(null)) -- the USERNAME (e.g. a PAT-style credential used as username) is
    // left in. storeData's top-level catch must not let that raw message become the DataStoreException's own
    // message, since it feeds logs/error reporting.
    @Test
    public void test_storeData_fetchFailureMessageDoesNotLeakCredential() {
        final DataStoreParams params = new DataStoreParams();
        // Port 1 is a reserved/unassigned port that refuses connections immediately (no timeout wait).
        params.put("uri", "https://leakyuser:leakysecret@127.0.0.1:1/nonexistent.git");
        final List<String> urlList = new ArrayList<>();
        try {
            newCollectingDataStore(urlList).storeData(null, null, params, null, null);
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            final String msg = e.getMessage();
            assertFalse(msg != null && msg.contains("leakyuser"));
            assertFalse(msg != null && msg.contains("leakysecret"));
            // The wrapped cause is what log4j2 prints as "Caused by:" and what failureUrlService persists, so
            // its message must be scrubbed too. JGit's TransportException embeds the username (only the password
            // is stripped) in its own getMessage(), so a raw cause here would still leak "leakyuser".
            final Throwable cause = e.getCause();
            assertNotNull(cause);
            final String causeMsg = cause.getMessage();
            assertFalse(causeMsg != null && causeMsg.contains("leakyuser"));
            assertFalse(causeMsg != null && causeMsg.contains("leakysecret"));
        }
    }

    // Fix #9: when the remote's advertised HEAD is NOT symbolic (some transports/servers don't advertise the
    // git symref capability for HEAD), JGit's own CloneCommand#findBranchToCheckout still identifies a proper
    // branch name by scanning refs/heads/* for one whose objectId matches HEAD's objectId, falling back to a
    // bare SHA only if nothing matches. resolveDefaultBranch must replicate that scan instead of immediately
    // returning the raw SHA -- a raw SHA in prev_source_ref would make isSameSource() flip-flop across runs
    // with no actual branch/uri change (since the SHA moves as the branch advances).
    @Test
    public void test_findAdvertisedBranchByObjectId_matchesRefsHeads() throws Exception {
        final GitDataStore dataStore = new GitDataStore();
        final ObjectId targetId = ObjectId.fromString("0123456789abcdef0123456789abcdef01234567");
        final ObjectId otherId = ObjectId.fromString("fedcba9876543210fedcba9876543210fedcba98");
        final List<Ref> advertisedRefs = new ArrayList<>();
        advertisedRefs.add(new FakeRef("refs/heads/develop", otherId));
        advertisedRefs.add(new FakeRef("refs/heads/main", targetId));
        advertisedRefs.add(new FakeRef("refs/tags/v1.0", targetId)); // a tag must NOT be treated as a branch

        final String result = dataStore.findAdvertisedBranchByObjectId(advertisedRefs, targetId);

        assertEquals("refs/heads/main", result);
    }

    @Test
    public void test_findAdvertisedBranchByObjectId_noMatchReturnsNull() throws Exception {
        final GitDataStore dataStore = new GitDataStore();
        final ObjectId targetId = ObjectId.fromString("0123456789abcdef0123456789abcdef01234567");
        final ObjectId otherId = ObjectId.fromString("fedcba9876543210fedcba9876543210fedcba98");
        final List<Ref> advertisedRefs = new ArrayList<>();
        advertisedRefs.add(new FakeRef("refs/heads/develop", otherId));

        // Nothing under refs/heads/ matches the (detached/anonymous) target -- there is genuinely no branch
        // name to report, so the caller must fall back to the raw SHA.
        assertNull(dataStore.findAdvertisedBranchByObjectId(advertisedRefs, targetId));
    }

    @Test
    public void test_findAdvertisedBranchByObjectId_nullHeadIdReturnsNull() throws Exception {
        final GitDataStore dataStore = new GitDataStore();
        final List<Ref> advertisedRefs = new ArrayList<>();
        advertisedRefs.add(new FakeRef("refs/heads/main", ObjectId.fromString("0123456789abcdef0123456789abcdef01234567")));

        assertNull(dataStore.findAdvertisedBranchByObjectId(advertisedRefs, null));
    }

    /** Minimal {@link Ref} test double: only getName()/getObjectId() are exercised by the code under test. */
    private static final class FakeRef implements Ref {
        private final String name;
        private final ObjectId objectId;

        FakeRef(final String name, final ObjectId objectId) {
            this.name = name;
            this.objectId = objectId;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isSymbolic() {
            return false;
        }

        @Override
        public Ref getLeaf() {
            return this;
        }

        @Override
        public Ref getTarget() {
            return this;
        }

        @Override
        public ObjectId getObjectId() {
            return objectId;
        }

        @Override
        public ObjectId getPeeledObjectId() {
            return null;
        }

        @Override
        public boolean isPeeled() {
            return false;
        }

        @Override
        public Storage getStorage() {
            return Storage.LOOSE;
        }
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

    // Fix #6: credentials embedded in a Git URI must be redacted before logging. redactUrl() must be
    // fail-CLOSED: java.net.URI is a strict RFC-3986 parser that throws on very common real-world Git
    // credential syntax (scp-style user@host:path, unescaped '@'/'/' in passwords), and the old
    // implementation returned the RAW url unchanged whenever that parser choked -- i.e. it failed OPEN.
    @Test
    public void test_redactUrl() {
        final GitDataStore dataStore = new GitDataStore();
        // Normal case: URI authority with user:password -- must already work (regression check).
        assertEquals("https://github.com/codelibs/fess.git", dataStore.redactUrl("https://user:token@github.com/codelibs/fess.git"));

        // scp-style remotes DO carry a credential-bearing user, and it must not survive redaction, even
        // though java.net.URI cannot parse this syntax at all.
        final String scpRedacted = dataStore.redactUrl("tokenuser@gitlab.example.com:group/project.git");
        assertFalse(scpRedacted.contains("tokenuser"));
        assertEquals("gitlab.example.com:group/project.git", scpRedacted);

        // A password containing an unescaped '@' is valid in real-world Git credentials but is illegal
        // per strict RFC-3986 userinfo syntax.
        final String atInPassword = dataStore.redactUrl("https://user:p@ssw0rd@host/repo.git");
        assertFalse(atInPassword.contains("p@ssw0rd"));
        assertFalse(atInPassword.contains("ssw0rd"));

        // A password containing an unescaped '/' likewise breaks strict URI parsing.
        final String slashInPassword = dataStore.redactUrl("https://user:p/ssw0rd@host/repo.git");
        assertFalse(slashInPassword.contains("p/ssw0rd"));
        assertFalse(slashInPassword.contains("ssw0rd"));

        // No credentials at all: must be returned unchanged (no regression).
        assertEquals("https://github.com/codelibs/fess.git", dataStore.redactUrl("https://github.com/codelibs/fess.git"));

        // A conventional non-secret scp-style user (e.g. "git") is still stripped -- redactUrl cannot tell
        // it apart from a credential-bearing user, and stripping it is harmless for a log/index value.
        assertEquals("github.com:codelibs/fess.git", dataStore.redactUrl("git@github.com:codelibs/fess.git"));

        // Blank/null input must be returned unchanged without throwing.
        assertEquals("", dataStore.redactUrl(""));
        assertNull(dataStore.redactUrl(null));

        // Bug fix (post-review): an uppercase/mixed-case scheme (e.g. "HTTPS://") makes JGit's URIish fail
        // its normal FULL_URI parse (its internal SCHEME_P regex is lowercase-only) and silently fall through
        // to the lenient LOCAL_FILE catch-all, which treats the entire input as an opaque local path --
        // scheme/host/user/pass all come back null. Neither existing branch fires on that, so without a fix
        // the raw, credential-bearing url would be returned completely unredacted.
        final String upperScheme = dataStore.redactUrl("HTTPS://user:pass@Host/Repo.Git");
        assertFalse(upperScheme.contains("pass"));

        final String mixedScheme = dataStore.redactUrl("Https://user:token@host/repo.git");
        assertFalse(mixedScheme.contains("token"));

        // No credentials to strip: an uppercase-scheme url with no user-info must still come back essentially
        // unchanged (not corrupted/emptied) even though it is routed through maskConservatively().
        assertEquals("HTTPS://github.com/codelibs/fess.git", dataStore.redactUrl("HTTPS://github.com/codelibs/fess.git"));

        // A genuine schemeless local filesystem path containing a literal '@' (e.g. in a directory name) has
        // no scheme:// prefix at all, so it must NOT be routed through maskConservatively() and must come
        // back completely unchanged -- there is no credential here to strip.
        assertEquals("/home/user@company/repos/x.git", dataStore.redactUrl("/home/user@company/repos/x.git"));
    }

    // Fix #10 (helper): redactCredentials scrubs a scheme://userinfo@ embedded anywhere in free-form text (e.g.
    // a JGit exception message), leaving the scheme and the rest of the text intact; text with no embedded URL
    // is returned unchanged.
    @Test
    public void test_redactCredentials() {
        final GitDataStore dataStore = new GitDataStore();

        // Username-only userinfo embedded mid-message (JGit strips the password but leaves the username).
        assertEquals("https://127.0.0.1:1/nonexistent.git: Connection refused",
                dataStore.redactCredentials("https://leakyuser@127.0.0.1:1/nonexistent.git: Connection refused"));

        // Userinfo embedded with text BEFORE the URL: only the userinfo is stripped, surrounding text is kept.
        assertEquals("failed to fetch https://host/repo.git for crawl",
                dataStore.redactCredentials("failed to fetch https://tokenuser@host/repo.git for crawl"));

        // Full user:pass userinfo embedded mid-message.
        assertEquals("https://host/repo.git: auth failed", dataStore.redactCredentials("https://user:pass@host/repo.git: auth failed"));

        // No embedded URL at all: returned completely unchanged.
        assertEquals("plain error message with no url", dataStore.redactCredentials("plain error message with no url"));

        // Blank/null returned unchanged without throwing.
        assertEquals("", dataStore.redactCredentials(""));
        assertNull(dataStore.redactCredentials(null));
    }

    // Fix #10 (helper): redactCredentialsInChain scrubs credentials from EVERY level of a cause chain (this is
    // what log4j2 prints as "Caused by:" and what failureUrlService persists), returns the SAME instance when
    // nothing needs redaction, and passes null through.
    @Test
    public void test_redactCredentialsInChain() {
        final GitDataStore dataStore = new GitDataStore();

        // Both levels carry an embedded credential; both must be scrubbed.
        final Throwable leaky = new RuntimeException("outer https://leakyuser:leakysecret@host/repo.git: boom",
                new IllegalStateException("inner https://leakyuser@host2/other.git: nope"));
        final Throwable sanitized = dataStore.redactCredentialsInChain(leaky);
        assertFalse(sanitized.getMessage().contains("leakyuser"));
        assertFalse(sanitized.getMessage().contains("leakysecret"));
        assertNotNull(sanitized.getCause());
        assertFalse(sanitized.getCause().getMessage().contains("leakyuser"));
        assertFalse(sanitized.getCause().getMessage().contains("leakysecret"));
        // The replacement message embeds the original class name so logs still show the error type.
        assertTrue(sanitized.getMessage().contains("java.lang.RuntimeException"));
        // The original chain is left untouched (a sanitized copy is returned, not an in-place mutation).
        assertTrue(leaky.getMessage().contains("leakyuser"));

        // A leak only in the deeper cause must still be scrubbed, and the wrapper is rebuilt (different instance).
        final Throwable cleanOuterLeakyInner =
                new RuntimeException("clean outer message", new IllegalStateException("inner https://leakyuser@host/r.git: x"));
        final Throwable sanitized2 = dataStore.redactCredentialsInChain(cleanOuterLeakyInner);
        assertTrue(sanitized2 != cleanOuterLeakyInner);
        assertFalse(sanitized2.getCause().getMessage().contains("leakyuser"));

        // A chain with no embedded credentials anywhere is returned as the SAME instance (no pointless wrapping).
        final Throwable clean = new RuntimeException("no url here", new IllegalStateException("still nothing"));
        assertSame(clean, dataStore.redactCredentialsInChain(clean));

        // Null passes through.
        assertNull(dataStore.redactCredentialsInChain(null));
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

    // These three tests exercise the REAL GitDataStore#getUrlFilter (no override): it looks up a UrlFilter
    // component via ComponentUtil, so a RecordingUrlFilter test double is registered as that component
    // (the same "fake the collaborator, exercise the real method" pattern runProcessFileCapturingResultMap
    // uses for SystemHelper/CrawlerStatsHelper). This replaces the previous versions, which claimed to "call
    // parent implementation to test it" but actually overrode getUrlFilter with a hand-rolled reimplementation
    // that never called super, so the real method was never exercised and only assertNotNull(filter) was
    // checked.
    @Test
    public void test_getUrlFilter_withIncludePattern() {
        final RecordingUrlFilter fakeFilter = new RecordingUrlFilter();
        ComponentUtil.register(fakeFilter, UrlFilter.class.getCanonicalName());
        final GitDataStore dataStore = new GitDataStore();

        final DataStoreParams params = new DataStoreParams();
        params.put("include_pattern", ".*\\.java");

        final UrlFilter filter = dataStore.getUrlFilter(params);

        assertSame(fakeFilter, filter);
        assertEquals(1, fakeFilter.includePatterns.size());
        assertTrue(filter.match("src/main/java/App.java"));
        assertFalse(filter.match("README.md"));
    }

    @Test
    public void test_getUrlFilter_withExcludePattern() {
        final RecordingUrlFilter fakeFilter = new RecordingUrlFilter();
        ComponentUtil.register(fakeFilter, UrlFilter.class.getCanonicalName());
        final GitDataStore dataStore = new GitDataStore();

        final DataStoreParams params = new DataStoreParams();
        params.put("exclude_pattern", ".*\\.class");

        final UrlFilter filter = dataStore.getUrlFilter(params);

        assertSame(fakeFilter, filter);
        assertEquals(1, fakeFilter.excludePatterns.size());
        assertTrue(filter.match("App.java"));
        assertFalse(filter.match("App.class"));
    }

    @Test
    public void test_getUrlFilter_withBothPatterns() {
        final RecordingUrlFilter fakeFilter = new RecordingUrlFilter();
        ComponentUtil.register(fakeFilter, UrlFilter.class.getCanonicalName());
        final GitDataStore dataStore = new GitDataStore();

        final DataStoreParams params = new DataStoreParams();
        params.put("include_pattern", ".*\\.java");
        params.put("exclude_pattern", ".*Test\\.java");

        final UrlFilter filter = dataStore.getUrlFilter(params);

        assertSame(fakeFilter, filter);
        assertEquals(1, fakeFilter.includePatterns.size());
        assertEquals(1, fakeFilter.excludePatterns.size());
        // Matches the include pattern and not the exclude pattern.
        assertTrue(filter.match("src/main/java/App.java"));
        // Matches the include pattern but is also excluded.
        assertFalse(filter.match("src/main/java/AppTest.java"));
        // Does not even match the include pattern.
        assertFalse(filter.match("README.md"));
    }

    /**
     * A {@link UrlFilter} test double that implements real include/exclude matching (mirroring
     * {@code UrlFilterImpl#match}), used as the component {@link GitDataStore#getUrlFilter} looks up so the
     * real method's wiring (addInclude/addExclude/init calls) is exercised without needing a full crawler
     * container (the real {@code UrlFilterImpl} depends on a DI-injected {@code CrawlerContainer} that isn't
     * available in this lightweight unit-test container).
     */
    private static final class RecordingUrlFilter implements UrlFilter {
        private final List<java.util.regex.Pattern> includePatterns = new ArrayList<>();
        private final List<java.util.regex.Pattern> excludePatterns = new ArrayList<>();

        @Override
        public void init(final String sessionId) {
            // no-op: init() being called at all (without throwing) is implicitly verified by every test
            // in this class reaching its assertions, since GitDataStore#getUrlFilter calls it unconditionally.
        }

        @Override
        public boolean match(final String url) {
            if (!includePatterns.isEmpty() && includePatterns.stream().noneMatch(p -> p.matcher(url).matches())) {
                return false;
            }
            return excludePatterns.stream().noneMatch(p -> p.matcher(url).matches());
        }

        @Override
        public void addInclude(final String urlPattern) {
            includePatterns.add(java.util.regex.Pattern.compile(urlPattern));
        }

        @Override
        public void addExclude(final String urlPattern) {
            excludePatterns.add(java.util.regex.Pattern.compile(urlPattern));
        }

        @Override
        public void processUrl(final String url) {
            // no-op: not exercised by getUrlFilter.
        }

        @Override
        public void clear() {
            includePatterns.clear();
            excludePatterns.clear();
        }
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
