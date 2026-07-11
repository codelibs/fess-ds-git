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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.io.CopyUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.misc.Pair;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.entity.ExtractData;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.crawler.helper.MimeTypeHelper;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.opensearch.config.exbhv.DataConfigBhv;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectStream;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.FetchResult;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

/**
 * A data store for crawling Git repositories.
 */
public class GitDataStore extends AbstractDataStore {

    /**
     * Default constructor.
     */
    public GitDataStore() {
        super();
    }

    private static final Logger logger = LogManager.getLogger(GitDataStore.class);

    /** Configuration parameter key for Git repository authentication password. */
    protected static final String PASSWORD = "password";

    /** Parameter key for the username. */
    protected static final String USERNAME = "username";

    /** Parameter key for the commit ID. */
    protected static final String COMMIT_ID = "commit_id";

    /** Parameter key for the ref specs. */
    protected static final String REF_SPECS = "ref_specs";

    /** Parameter key for the default extractor. */
    protected static final String DEFAULT_EXTRACTOR = "default_extractor";

    /** Parameter key for the cache threshold. */
    protected static final String CACHE_THRESHOLD = "cache_threshold";

    /** Parameter key for the extractors. */
    protected static final String EXTRACTORS = "extractors";

    /** Parameter key for the read interval. */
    protected static final String READ_INTERVAL = "read_interval";

    /** Parameter key for the tree walk. */
    protected static final String TREE_WALK = "tree_walk";

    /** Parameter key for the rev commit. */
    protected static final String REV_COMMIT = "rev_commit";

    /** Parameter key for the repository. */
    protected static final String REPOSITORY = "repository";

    /** Parameter key for the URI. */
    protected static final String URI = "uri";

    /** Parameter key for the base URL. */
    protected static final String BASE_URL = "base_url";

    /** Parameter key for the diff entry. */
    protected static final String DIFF_ENTRY = "diff_entry";

    /** Parameter key for the Git instance. */
    protected static final String GIT = "git";

    /** Parameter key for the current commit ID. */
    protected static final String CURRENT_COMMIT_ID = "current_commit_id";

    /** Parameter key for the previous commit ID. */
    protected static final String PREV_COMMIT_ID = "prev_commit_id";

    /** Parameter key for the previous source ref (uri + resolved commit ref) used to detect a changed source configuration. */
    protected static final String PREV_SOURCE_REF = "prev_source_ref";

    /** Parameter key for the temporary repository path. */
    protected static final String TEMP_REPOSITORY_PATH = "temp_repository_path";

    /** Parameter key for the repository path. */
    protected static final String REPOSITORY_PATH = "repository_path";

    /** File name of the advisory lock marker placed next to a persistent {@code repository_path}. */
    protected static final String LOCK_FILE_NAME = ".fess-ds-git.lock";

    /** Configuration map key for the repository lock channel. */
    protected static final String REPOSITORY_LOCK_CHANNEL = "repository_lock_channel";

    /** Configuration map key for the repository lock. */
    protected static final String REPOSITORY_LOCK = "repository_lock";

    /** Parameter key for the max size. */
    protected static final String MAX_SIZE = "max_size";

    /** Parameter key for the include pattern. */
    protected static final String INCLUDE_PATTERN = "include_pattern";

    /** Parameter key for the exclude pattern. */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";

    /** Parameter key for the URL filter. */
    protected static final String URL_FILTER = "url_filter";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final String uri = paramMap.getAsString(URI);
        if (StringUtil.isBlank(uri)) {
            throw new DataStoreException("uri is required.");
        }
        final String refSpec = paramMap.getAsString(REF_SPECS, "+refs/heads/*:refs/heads/*");
        final String commitId = paramMap.getAsString(COMMIT_ID, org.eclipse.jgit.lib.Constants.HEAD);
        final String username = paramMap.getAsString(USERNAME);
        final String password = paramMap.getAsString(PASSWORD);
        final String prevCommit = paramMap.getAsString(PREV_COMMIT_ID);
        final String prevSourceRef = paramMap.getAsString(PREV_SOURCE_REF);
        final String baseUrl = paramMap.getAsString(BASE_URL);
        final String repositoryPath = paramMap.getAsString(REPOSITORY_PATH);
        CredentialsProvider credentialsProvider = null;
        if (username != null && password != null) {
            credentialsProvider = new UsernamePasswordCredentialsProvider(username, password);
        }

        if (StringUtil.isBlank(baseUrl)) {
            logger.warn("base_url is blank: indexed document URLs will be empty and delete/rename tracking will be skipped.");
        }

        final Map<String, Object> configMap = createConfigMap(paramMap);
        configMap.put(URI, uri);

        final UrlFilter urlFilter = getUrlFilter(paramMap);

        logger.info("Git: {}", redactUrl(uri));

        final Repository repository = (Repository) configMap.get(REPOSITORY);
        try (final Git git = new Git(repository)) {
            configMap.put(GIT, git);
            if (StringUtil.isNotBlank(repositoryPath)) {
                lockRepositoryPath(new File(repositoryPath), configMap);
            }
            final FetchResult fetchResult = git.fetch()
                    .setForceUpdate(true)
                    .setRemote(uri)
                    .setRefSpecs(new RefSpec(refSpec))
                    .setInitialBranch(commitId)
                    .setCredentialsProvider(credentialsProvider)
                    .call();
            if (logger.isDebugEnabled()) {
                logger.debug("Fetch Result: {}", fetchResult.getMessages());
            }
            final String resolvedCommitId = resolveDefaultBranch(repository, fetchResult, commitId);
            if (!hasCommitLogs(configMap)) {
                final Ref ref = git.checkout().setName(resolvedCommitId).call();
                if (logger.isDebugEnabled()) {
                    logger.debug("Checked out {}", ref.getName());
                }
            }
            final String currentSourceRef = redactUrl(uri) + "#" + resolvedCommitId;
            final ObjectId fromCommitId;
            if (StringUtil.isNotBlank(prevCommit) && isSameSource(prevSourceRef, currentSourceRef)) {
                fromCommitId = repository.resolve(prevCommit);
            } else {
                if (StringUtil.isNotBlank(prevCommit) && StringUtil.isNotBlank(prevSourceRef)) {
                    logger.info("Source configuration changed ('{}' -> '{}'); performing a full reindex instead of an incremental diff.",
                            prevSourceRef, currentSourceRef);
                }
                fromCommitId = null;
            }
            final ObjectId toCommitId = resolveToCommitId(repository, commitId, resolvedCommitId);
            configMap.put(CURRENT_COMMIT_ID, toCommitId);
            try (DiffFormatter diffFormatter = new DiffFormatter(null)) {
                diffFormatter.setRepository(repository);
                logger.info("Rev: {} -> {}", fromCommitId, toCommitId);
                diffFormatter.scan(fromCommitId, toCommitId).forEach(entry -> {
                    final String path;
                    if (entry.getChangeType() == DiffEntry.ChangeType.DELETE) {
                        path = entry.getOldPath();
                    } else {
                        path = entry.getNewPath();
                    }
                    if (urlFilter != null && !urlFilter.match(path)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Not matched: {}", path);
                        }
                        return;
                    }
                    configMap.put(DIFF_ENTRY, entry);
                    switch (entry.getChangeType()) {
                    case ADD, MODIFY:
                        processFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap);
                        break;
                    case DELETE:
                        if (StringUtil.isNotBlank(baseUrl)) {
                            deleteDocument(paramMap, configMap);
                        }
                        break;
                    case RENAME:
                        if (StringUtil.isNotBlank(baseUrl)) {
                            deleteDocument(paramMap, configMap);
                        }
                        processFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap);
                        break;
                    default:
                        break;
                    }
                });
            }
            if (dataConfig != null) {
                updateDataConfig(dataConfig, currentSourceRef, toCommitId);
            }
        } catch (final Exception e) {
            throw new DataStoreException(e);
        } finally {
            try {
                repository.close();
            } finally {
                try {
                    releaseRepositoryLock(configMap);
                } finally {
                    final File gitRepoPath = (File) configMap.get(TEMP_REPOSITORY_PATH);
                    if (gitRepoPath != null) {
                        try (Stream<Path> walk = Files.walk(gitRepoPath.toPath())) {
                            walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
                        } catch (final IOException e) {
                            logger.warn("Failed to delete {}.", gitRepoPath.getAbsolutePath(), e);
                        }
                    }
                }
            }
        }
    }

    /**
     * Deletes a document from the index.
     *
     * @param paramMap The parameter map.
     * @param configMap The configuration map.
     */
    protected void deleteDocument(final DataStoreParams paramMap, final Map<String, Object> configMap) {
        final DiffEntry entry = (DiffEntry) configMap.get(DIFF_ENTRY);
        try {
            final String url = getUrl(paramMap, entry.getOldPath());
            ComponentUtil.getIndexingHelper().deleteDocumentByUrl(ComponentUtil.getSearchEngineClient(), url);
        } catch (final Exception e) {
            logger.warn("Failed to delete the document {}.", entry);
        }
    }

    /**
     * Updates the data configuration with the new commit ID and source ref.
     * <p>
     * Both {@link #PREV_COMMIT_ID} and {@link #PREV_SOURCE_REF} are written back using the same
     * "update in place if the key already exists, otherwise append" strategy so that a repeat run
     * can detect when the {@code uri}/branch has changed and fall back to a full reindex.
     * </p>
     *
     * @param dataConfig The data configuration.
     * @param sourceRef The current source ref ({@code uri + "#" + resolved commit ref}).
     * @param toCommitId The new commit ID.
     */
    protected void updateDataConfig(final DataConfig dataConfig, final String sourceRef, final ObjectId toCommitId) {
        final String paramStr = buildHandlerParameter(dataConfig.getHandlerParameterMap(), toCommitId.name(), sourceRef);
        dataConfig.setHandlerParameter(paramStr);
        if (logger.isDebugEnabled()) {
            logger.debug("Updating data config by {}.", paramStr);
        }
        ComponentUtil.getComponent(DataConfigBhv.class).update(dataConfig);
        logger.info("Updated DataConfig: {}", dataConfig.getId());
    }

    /**
     * Builds the {@code handlerParameter} string with {@link #PREV_COMMIT_ID} and {@link #PREV_SOURCE_REF}
     * applied using an "update in place if the key already exists, otherwise append" strategy so that
     * existing keys keep their position and are never duplicated.
     *
     * @param handlerParameterMap The current handler parameter map.
     * @param prevCommitId The commit ID to persist as {@link #PREV_COMMIT_ID}.
     * @param prevSourceRef The source ref to persist as {@link #PREV_SOURCE_REF}.
     * @return The rebuilt {@code handlerParameter} string.
     */
    protected String buildHandlerParameter(final Map<String, String> handlerParameterMap, final String prevCommitId,
            final String prevSourceRef) {
        final Map<String, String> newValues = new LinkedHashMap<>();
        newValues.put(PREV_COMMIT_ID, prevCommitId);
        newValues.put(PREV_SOURCE_REF, prevSourceRef);

        final StringBuilder buf = new StringBuilder();
        handlerParameterMap.forEach((key, value) -> {
            if (buf.length() > 0) {
                buf.append('\n');
            }
            buf.append(key).append('=').append(newValues.getOrDefault(key, value));
        });
        newValues.forEach((key, value) -> {
            if (!handlerParameterMap.containsKey(key)) {
                if (buf.length() > 0) {
                    buf.append('\n');
                }
                buf.append(key).append('=').append(value);
            }
        });
        return buf.toString();
    }

    /**
     * Resolves the commit reference to use for checkout and diffing.
     * <p>
     * When {@code commitId} defaults to {@link org.eclipse.jgit.lib.Constants#HEAD} (or is blank), the
     * remote's advertised {@code HEAD} is inspected to determine the actual default branch (e.g.
     * {@code refs/heads/main}). {@code FetchCommand} does not update the local {@code HEAD}, so if the
     * advertised {@code HEAD} is symbolic the local {@code HEAD} is linked to that branch (mirroring
     * {@code CloneCommand}); this lets {@link #hasCommitLogs(Map)} resolve correctly on subsequent runs
     * against a persistent {@code repository_path}.
     * </p>
     *
     * @param repository The Git repository.
     * @param fetchResult The result of the fetch operation.
     * @param commitId The configured commit ID.
     * @return The resolved commit reference to use for checkout and resolution.
     * @throws IOException If updating the local {@code HEAD} fails.
     */
    protected String resolveDefaultBranch(final Repository repository, final FetchResult fetchResult, final String commitId)
            throws IOException {
        if (StringUtil.isNotBlank(commitId) && !org.eclipse.jgit.lib.Constants.HEAD.equals(commitId)) {
            return commitId;
        }
        final Ref headRef = fetchResult.getAdvertisedRef(org.eclipse.jgit.lib.Constants.HEAD);
        if (headRef == null) {
            return commitId;
        }
        if (headRef.isSymbolic()) {
            final String targetName = headRef.getTarget().getName();
            final RefUpdate newHead = repository.updateRef(org.eclipse.jgit.lib.Constants.HEAD);
            newHead.disableRefLog();
            newHead.link(targetName);
            return targetName;
        }
        return headRef.getObjectId().name();
    }

    /**
     * Resolves the target commit and fails fast if it cannot be resolved.
     * <p>
     * Resolving must happen before any diff/delete work: a {@code null} target would otherwise be treated
     * by JGit's diff as an empty tree, marking every previously-tracked file as a deletion.
     * </p>
     *
     * @param repository The Git repository.
     * @param commitId The configured commit ID (used only for the error message).
     * @param resolvedCommitId The resolved commit reference to look up.
     * @return The resolved target commit ID (never {@code null}).
     * @throws IOException If resolution fails at the I/O level.
     */
    protected ObjectId resolveToCommitId(final Repository repository, final String commitId, final String resolvedCommitId)
            throws IOException {
        final ObjectId toCommitId = repository.resolve(resolvedCommitId);
        if (toCommitId == null) {
            throw new DataStoreException("Could not resolve commit_id '" + commitId
                    + "'. The branch/tag may not exist, or may have been renamed/deleted upstream.");
        }
        return toCommitId;
    }

    /**
     * Determines whether the recorded source ref matches the current run's source ref.
     * <p>
     * A blank {@code prevSourceRef} (e.g. a config that predates this tracking param, or a manually-set
     * {@code prev_commit_id}) is treated as a match so that existing incremental behavior is preserved.
     * </p>
     *
     * @param prevSourceRef The source ref recorded on the previous run.
     * @param currentSourceRef The source ref of the current run.
     * @return {@code true} if the previous {@code prev_commit_id} can be trusted for an incremental diff.
     */
    protected boolean isSameSource(final String prevSourceRef, final String currentSourceRef) {
        if (StringUtil.isBlank(prevSourceRef)) {
            return true;
        }
        return prevSourceRef.equals(currentSourceRef);
    }

    /**
     * Acquires an OS-level advisory lock on a persistent {@code repository_path} and removes any stale
     * JGit lock files.
     * <p>
     * The marker file sits next to {@code .git} so it is never touched by the stale-lock scan. Because the
     * OS releases the {@link FileLock} automatically when the holding process dies (including via SIGKILL),
     * a run killed a moment ago will not block the next run, whereas a genuinely concurrent live run will.
     * Once the lock is held we are the sole owner of the directory, so any {@code *.lock} file under
     * {@code .git} is guaranteed stale and is deleted.
     * </p>
     *
     * @param repositoryPath The persistent repository directory.
     * @param configMap The configuration map; the lock and channel are stored here for later release.
     */
    protected void lockRepositoryPath(final File repositoryPath, final Map<String, Object> configMap) {
        final File markerFile = new File(repositoryPath, LOCK_FILE_NAME);
        FileChannel channel = null;
        FileLock lock = null;
        try {
            channel = FileChannel.open(markerFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            lock = channel.tryLock();
        } catch (final OverlappingFileLockException e) {
            // tryLock() throws instead of returning null when this JVM already holds an overlapping lock.
            lock = null;
        } catch (final IOException e) {
            closeChannelQuietly(channel);
            throw new DataStoreException("Failed to acquire a lock on repository_path " + repositoryPath.getAbsolutePath(), e);
        }
        if (lock == null) {
            closeChannelQuietly(channel);
            throw new DataStoreException("Another crawl appears to be using repository_path '" + repositoryPath.getAbsolutePath()
                    + "'. If no crawl is actually running, a stale lock file may need manual cleanup: " + markerFile.getAbsolutePath());
        }
        configMap.put(REPOSITORY_LOCK_CHANNEL, channel);
        configMap.put(REPOSITORY_LOCK, lock);
        deleteStaleLockFiles(new File(repositoryPath, ".git"));
    }

    /**
     * Deletes stale JGit {@code *.lock} files under the given {@code .git} directory. Must only be called
     * while holding the {@link #lockRepositoryPath} advisory lock, which guarantees the files are stale.
     *
     * @param gitDir The {@code .git} directory to scan.
     */
    protected void deleteStaleLockFiles(final File gitDir) {
        if (gitDir == null || !gitDir.isDirectory()) {
            return;
        }
        try (Stream<Path> walk = Files.walk(gitDir.toPath())) {
            walk.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".lock")).forEach(p -> {
                final File lockFile = p.toFile();
                logger.warn("Removing stale Git lock file (likely left by a previously interrupted crawl): {}", lockFile.getAbsolutePath());
                if (!lockFile.delete()) {
                    logger.warn("Failed to delete stale Git lock file: {}", lockFile.getAbsolutePath());
                }
            });
        } catch (final IOException e) {
            logger.warn("Failed to scan for stale Git lock files under {}.", gitDir.getAbsolutePath(), e);
        }
    }

    /**
     * Releases the repository lock and closes its channel, if present. Best-effort: the OS releases the
     * lock anyway if the process is killed.
     *
     * @param configMap The configuration map holding the lock and channel.
     */
    protected void releaseRepositoryLock(final Map<String, Object> configMap) {
        final FileLock lock = (FileLock) configMap.get(REPOSITORY_LOCK);
        if (lock != null) {
            try {
                lock.release();
            } catch (final IOException e) {
                logger.debug("Failed to release the repository lock.", e);
            }
        }
        closeChannelQuietly((FileChannel) configMap.get(REPOSITORY_LOCK_CHANNEL));
    }

    private void closeChannelQuietly(final FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (final IOException e) {
                logger.debug("Failed to close the repository lock channel.", e);
            }
        }
    }

    /**
     * Redacts any user-info (e.g. {@code user:token@}) embedded in a Git URI so it is safe to log.
     * Falls back to the original string if it cannot be parsed as a URI.
     *
     * @param url The Git URI.
     * @return The URI with any user-info component removed.
     */
    protected String redactUrl(final String url) {
        if (StringUtil.isBlank(url)) {
            return url;
        }
        try {
            final URI parsed = new URI(url);
            if (parsed.getUserInfo() == null) {
                return url;
            }
            return new URI(parsed.getScheme(), null, parsed.getHost(), parsed.getPort(), parsed.getPath(), parsed.getQuery(),
                    parsed.getFragment()).toString();
        } catch (final URISyntaxException e) {
            return url;
        }
    }

    /**
     * Returns the file name from the given path.
     *
     * @param path The path.
     * @return The file name.
     */
    protected String getFileName(final String path) {
        final int pos = path.lastIndexOf('/');
        if (pos == -1) {
            return path;
        }
        return path.substring(pos + 1);
    }

    /**
     * Processes a file from a Git repository.
     * <p>
     * This method extracts content and metadata from the specified file
     * for the purpose of indexing it into the search engine. It uses the
     * provided data configuration, callback, and parameter maps to perform
     * the processing and indexing.
     * </p>
     *
     * @param dataConfig The data configuration containing repository settings.
     * @param callback The callback used for indexing the extracted data.
     * @param paramMap The parameter map containing additional settings.
     * @param scriptMap The script map for custom processing logic.
     * @param defaultDataMap The default data map for fallback values.
     * @param configMap The configuration map containing file-specific settings.
     */
    protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final String uri = (String) configMap.get(URI);
        final DiffEntry diffEntry = (DiffEntry) configMap.get(DIFF_ENTRY);
        final String path = diffEntry.getNewPath();
        final StatsKeyObject statsKey = new StatsKeyObject(redactUrl(uri));
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            crawlerStatsHelper.begin(statsKey);
            final RevCommit revCommit = getRevCommit(configMap, path);

            final String name = getFileName(path);
            logger.info("Crawling Path: {}", path);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            resultMap.remove(USERNAME);
            resultMap.remove(PASSWORD);
            final Repository repository = (Repository) configMap.get(REPOSITORY);
            final ObjectLoader objectLoader = repository.open(diffEntry.getNewId().toObjectId());
            final long size = objectLoader.getSize();
            if (size > ((Long) configMap.get(MAX_SIZE)).longValue()) {
                throw new MaxLengthExceededException(
                        "The content length (" + size + " byte) is over " + configMap.get(MAX_SIZE) + " byte. The path is " + path);
            }
            resultMap.put("contentLength", size);
            DeferredFileOutputStream dfos = null;
            try (ObjectStream in = objectLoader.openStream();
                    DeferredFileOutputStream out =
                            new DeferredFileOutputStream((Integer) configMap.get(CACHE_THRESHOLD), "fess-ds-git-", ".out", null)) {
                dfos = out;
                CopyUtil.copy(in, out);
                out.flush();

                final String mimeType = getMimeType(name, out);
                resultMap.put("mimetype", mimeType);
                final Extractor extractor = getExtractor(mimeType, configMap);

                final Map<String, String> params = new HashMap<>();
                params.put(ExtractData.RESOURCE_NAME_KEY, name);
                try (InputStream is = getContentInputStream(out)) {
                    String content = extractor.getText(is, params).getContent();
                    if (content == null) {
                        content = StringUtil.EMPTY;
                    }
                    resultMap.put("content", content);
                } catch (final Exception e) {
                    if (!ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                        throw e;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.warn("Could not get a text from {}.", redactUrl(uri), e);
                    } else {
                        logger.warn("Could not get a text from {}. {}", redactUrl(uri), e.getMessage());
                    }
                }

                resultMap.put("url", getUrl(paramMap, path));
                resultMap.put("uri", redactUrl(uri));
                resultMap.put("path", path);
                resultMap.put("name", name);
                resultMap.put("crawlingConfig", dataConfig);
                resultMap.put("author", revCommit.getAuthorIdent());
                resultMap.put("committer", revCommit.getCommitterIdent());
                resultMap.put("timestamp", new Date(revCommit.getCommitTime() * 1000L));

                crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

                if (logger.isDebugEnabled()) {
                    logger.debug("resultMap: {}", resultMap);
                }

                final String scriptType = getScriptType(paramMap);
                for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                    final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                    if (convertValue != null) {
                        dataMap.put(entry.getKey(), convertValue);
                    }
                }

                crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

                if (logger.isDebugEnabled()) {
                    logger.debug("dataMap: {}", dataMap);
                }

                if (dataMap.get("url") instanceof String statsUrl) {
                    statsKey.setUrl(statsUrl);
                }

                callback.store(paramMap, dataMap);
                crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
            } finally {
                if (dfos != null && !dfos.isInMemory()) {
                    final File file = dfos.getFile();
                    if (!file.delete()) {
                        logger.warn("Failed to delete {}.", file.getAbsolutePath());
                    }
                }
            }
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : {}", dataMap, e);

            Throwable target = e;
            if (target instanceof MultipleCrawlingAccessException) {
                final Throwable[] causes = ((MultipleCrawlingAccessException) target).getCauses();
                if (causes.length > 0) {
                    target = causes[causes.length - 1];
                }
            }

            String errorName;
            final Throwable cause = target.getCause();
            if (cause != null) {
                errorName = cause.getClass().getCanonicalName();
            } else {
                errorName = target.getClass().getCanonicalName();
            }

            String url;
            if (target instanceof DataStoreCrawlingException dce) {
                url = dce.getUrl();
                if (dce.aborted()) {
                    throw e;
                }
            } else {
                url = redactUrl(uri) + ":" + path;
            }
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, url, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final String url = redactUrl(uri) + ":" + path;
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), url, t);

            final long readInterval = (Long) configMap.get(READ_INTERVAL);
            if (readInterval > 0) {
                sleep(readInterval);
            }
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Checks if the repository has commit logs.
     *
     * @param configMap The configuration map.
     * @return true if the repository has commit logs, false otherwise.
     */
    protected boolean hasCommitLogs(final Map<String, Object> configMap) {
        final Git git = (Git) configMap.get(GIT);
        try {
            git.log().call();
            return true;
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Could not find commit logs.", e);
            }
            return false;
        }
    }

    /**
     * Returns the revision commit for the given path.
     *
     * @param configMap The configuration map.
     * @param path The path.
     * @return The revision commit.
     * @throws GitAPIException If an error occurs while accessing the Git repository.
     * @throws IOException If an I/O error occurs while reading the Git repository.
     */
    protected RevCommit getRevCommit(final Map<String, Object> configMap, final String path) throws GitAPIException, IOException {
        final Git git = (Git) configMap.get(GIT);
        final ObjectId currentCommitId = (ObjectId) configMap.get(CURRENT_COMMIT_ID);
        final Iterable<RevCommit> revCommits = git.log().add(currentCommitId).addPath(path).setMaxCount(1).call();
        // LogCommand.call() returns its own internal RevWalk (it ends with "return walk;") and never closes it,
        // so the returned Iterable can be cast back to that RevWalk and closed to avoid leaking pack-file handles.
        try (RevWalk revWalk = (RevWalk) revCommits) {
            final Iterator<RevCommit> revCommitIter = revCommits.iterator();
            if (!revCommitIter.hasNext()) {
                throw new DataStoreException("Failed to parse git log for " + path);
            }
            final RevCommit revCommit = revCommitIter.next();
            // Eagerly load the commit body so author/committer/timestamp remain readable after the walk is closed.
            revWalk.parseBody(revCommit);
            return revCommit;
        }
    }

    /**
     * Returns the URL for the given path.
     *
     * @param paramMap The parameter map.
     * @param path The path.
     * @return The URL.
     */
    protected String getUrl(final DataStoreParams paramMap, final String path) {
        final String baseUrl = paramMap.getAsString(BASE_URL);
        if (StringUtil.isNotBlank(baseUrl)) {
            return baseUrl + path;
        }
        return StringUtil.EMPTY;
    }

    /**
     * Creates a configuration map.
     *
     * @param paramMap The parameter map.
     * @return The configuration map.
     */
    protected Map<String, Object> createConfigMap(final DataStoreParams paramMap) {
        final Map<String, Object> configMap = new HashMap<>();
        @SuppressWarnings("unchecked")
        final Pair<Pattern, String>[] extractors = StreamUtil.split(paramMap.getAsString(EXTRACTORS), ",").get(stream -> stream.map(s -> {
            final String[] values = s.split(":");
            if (values.length != 2) {
                return null;
            }
            return new Pair<>(Pattern.compile(values[0]), values[1]);
        }).filter(Objects::nonNull).toArray(n -> new Pair[n]));
        configMap.put(EXTRACTORS, extractors);
        configMap.put(BASE_URL, paramMap.getAsString(BASE_URL, StringUtil.EMPTY));
        configMap.put(CACHE_THRESHOLD, Integer.parseInt(paramMap.getAsString(CACHE_THRESHOLD, "1000000")));
        configMap.put(DEFAULT_EXTRACTOR, paramMap.getAsString(DEFAULT_EXTRACTOR, "tikaExtractor"));
        configMap.put(READ_INTERVAL, getReadInterval(paramMap));
        final String maxSize = paramMap.getAsString(MAX_SIZE);
        configMap.put(MAX_SIZE, StringUtil.isNotBlank(maxSize) ? Long.parseLong(maxSize) : 10000000L);

        final String repositoryPath = paramMap.getAsString(REPOSITORY_PATH);
        if (StringUtil.isBlank(repositoryPath)) {
            try {
                final File gitRepoPath = Files.createTempDirectory("fess-ds-git-").toFile();
                final Repository repository = FileRepositoryBuilder.create(new File(gitRepoPath, ".git"));
                repository.create();
                configMap.put(REPOSITORY, repository);
                configMap.put(TEMP_REPOSITORY_PATH, gitRepoPath);
            } catch (final IOException e) {
                throw new DataStoreException("Failed to create a repository.", e);
            }
        } else {
            try {
                final File repoFile = new File(repositoryPath);
                final boolean exists = repoFile.exists();
                if (!exists) {
                    repoFile.mkdirs();
                }
                final Repository repository = FileRepositoryBuilder.create(new File(repositoryPath, ".git"));
                if (!exists) {
                    repository.create();
                }
                configMap.put(REPOSITORY, repository);
            } catch (final IOException e) {
                throw new DataStoreException("Failed to load " + repositoryPath, e);
            }
        }
        return configMap;
    }

    /**
     * Returns the extractor for the given MIME type.
     *
     * @param mimeType The MIME type.
     * @param configMap The configuration map.
     * @return The extractor.
     */
    protected Extractor getExtractor(final String mimeType, final Map<String, Object> configMap) {
        @SuppressWarnings("unchecked")
        final Pair<Pattern, String>[] extractors = (Pair<Pattern, String>[]) configMap.get(EXTRACTORS);
        for (final Pair<Pattern, String> pair : extractors) {
            if (pair.getFirst().matcher(mimeType).matches()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("use {} from {}", pair.getSecond(), mimeType);
                }
                final Extractor extractor = ComponentUtil.getComponent(pair.getSecond());
                if (extractor != null) {
                    return extractor;
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("use a default extractor from {}", mimeType);
        }
        Extractor extractor = ComponentUtil.getExtractorFactory().getExtractor(mimeType);
        if (extractor == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("use a defautl extractor as tikaExtractor by {}", mimeType);
            }
            extractor = ComponentUtil.getComponent((String) configMap.get(DEFAULT_EXTRACTOR));
        }
        return extractor;
    }

    /**
     * Returns the MIME type for the given file name.
     *
     * @param filename The file name.
     * @param out The deferred file output stream.
     * @return The MIME type.
     * @throws IOException If an I/O error occurs.
     */
    protected String getMimeType(final String filename, final DeferredFileOutputStream out) throws IOException {
        final MimeTypeHelper mimeTypeHelper = ComponentUtil.getComponent(MimeTypeHelper.class);
        try (InputStream is = getContentInputStream(out)) {
            return mimeTypeHelper.getContentType(is, filename);
        }
    }

    /**
     * Returns the content input stream from the deferred file output stream.
     *
     * @param out The deferred file output stream.
     * @return The content input stream.
     * @throws IOException If an I/O error occurs.
     */
    protected InputStream getContentInputStream(final DeferredFileOutputStream out) throws IOException {
        if (out.isInMemory()) {
            return new ByteArrayInputStream(out.getData());
        }
        return new FileInputStream(out.getFile());
    }

    /**
     * Returns the URL filter.
     *
     * @param paramMap The parameter map.
     * @return The URL filter.
     */
    protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
        final UrlFilter urlFilter = ComponentUtil.getComponent(UrlFilter.class);
        final String include = paramMap.getAsString(INCLUDE_PATTERN);
        if (StringUtil.isNotBlank(include)) {
            urlFilter.addInclude(include);
        }
        final String exclude = paramMap.getAsString(EXCLUDE_PATTERN);
        if (StringUtil.isNotBlank(exclude)) {
            urlFilter.addExclude(exclude);
        }
        urlFilter.init(paramMap.getAsString(Constants.CRAWLING_INFO_ID));
        if (logger.isDebugEnabled()) {
            logger.debug("urlFilter: {}", urlFilter);
        }
        return urlFilter;
    }
}
