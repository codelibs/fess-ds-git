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
import java.io.UncheckedIOException;
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
import java.util.regex.Matcher;
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
import org.eclipse.jgit.transport.URIish;
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

    /**
     * Parameter key for Fess's framework-level "delete old documents" behavior. This is handled by Fess's
     * crawling infrastructure ({@code DataIndexHelper}), not by this plugin; it is read here only to warn when
     * a persistent {@link #REPOSITORY_PATH} is used without disabling it (see {@link #storeData}).
     */
    protected static final String DELETE_OLD_DOCS = "delete_old_docs";

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

        // A persistent repository_path exists to preserve state across runs, but the fail-fast checks this
        // plugin adds (unresolvable commit_id, lock contention, etc.) only stop ITS OWN per-file deletes:
        // Fess's crawling infrastructure still runs deleteOldDocs() after every crawl attempt -- success OR
        // failure -- unless delete_old_docs=false. So a failed/aborted run would otherwise prune every
        // previously-indexed document of this DataConfig. Warn so the "required, not just safer" constraint is
        // visible at runtime, not just in the README.
        if (StringUtil.isNotBlank(repositoryPath) && !Constants.FALSE.equals(paramMap.getAsString(DELETE_OLD_DOCS))) {
            logger.warn("repository_path is set but delete_old_docs is not 'false': a failed or aborted crawl will let Fess "
                    + "prune all previously-indexed documents for this DataConfig. Set delete_old_docs=false to keep the "
                    + "existing index intact across failed/aborted runs.");
        }

        // getUrlFilter()/logger.info() only depend on paramMap/uri (not on configMap/the lock below), so they
        // are deliberately resolved BEFORE the lock is acquired: getUrlFilter() can throw (e.g.
        // ComponentNotFoundException, or CrawlerSystemException from UrlFilterImpl#init), and if that
        // happened between lock acquisition and the try/finally that releases it, the lock would leak for the
        // life of the JVM -- blocking every future crawl of this repository_path, i.e. exactly the
        // repeated-crawl-failure class this PR exists to eliminate.
        final UrlFilter urlFilter = getUrlFilter(paramMap);

        logger.info("Git: {}", redactUrl(uri));

        // The advisory lock must be acquired BEFORE createConfigMap() has a chance to initialize a brand-new
        // repository_path (repository.create() writes HEAD/config/description/refs/objects non-atomically):
        // otherwise the very first concurrent use of a not-yet-existing repository_path by two overlapping
        // crawls would race unprotected. A lock-holder map is used (rather than configMap directly) because
        // configMap does not exist until createConfigMap() returns; it is merged in below once available so
        // the normal cleanup path (releaseRepositoryLock(configMap) in the finally block) still applies.
        final Map<String, Object> lockHolder = new HashMap<>();
        if (StringUtil.isNotBlank(repositoryPath)) {
            final File repoDir = new File(repositoryPath);
            // Idempotent and harmless even under a genuine race: unlike JGit's multi-file repository.create(),
            // creating an empty directory concurrently has no corruption risk. It only exists to give the
            // lock marker file (below) somewhere to live.
            repoDir.mkdirs();
            lockRepositoryPath(repoDir, lockHolder);
        }
        final Map<String, Object> configMap;
        try {
            configMap = createConfigMap(paramMap);
        } catch (final Exception e) {
            releaseRepositoryLock(lockHolder);
            throw new DataStoreException("Failed to initialize Git repository " + redactUrl(uri), e);
        } catch (final Error e) {
            // An Error (e.g. NoClassDefFoundError) raised after the advisory lock was acquired must not leak
            // it; release the lock and rethrow the Error unchanged so it still propagates.
            releaseRepositoryLock(lockHolder);
            throw e;
        }
        configMap.putAll(lockHolder);
        configMap.put(URI, uri);

        final Repository repository = (Repository) configMap.get(REPOSITORY);
        try (final Git git = new Git(repository)) {
            configMap.put(GIT, git);
            final RefSpec fetchRefSpec = new RefSpec(refSpec);
            final FetchResult fetchResult = git.fetch()
                    .setForceUpdate(true)
                    .setRemote(uri)
                    .setRefSpecs(fetchRefSpec)
                    .setInitialBranch(resolveInitialBranch(commitId))
                    .setCredentialsProvider(credentialsProvider)
                    .call();
            if (logger.isDebugEnabled()) {
                logger.debug("Fetch Result: {}", fetchResult.getMessages());
            }
            final String resolvedCommitId = resolveDefaultBranch(repository, fetchResult, fetchRefSpec, commitId);
            if (!hasCommitLogs(configMap)) {
                final Ref ref = git.checkout().setName(resolvedCommitId).call();
                if (logger.isDebugEnabled()) {
                    // CheckoutCommand.call() returns null when the checkout detaches HEAD: it looks the name up
                    // with findRef() and keeps only a refs/heads/* match, so a commit SHA (an explicit commit_id,
                    // or resolveDefaultBranch()'s bare-SHA result for anonymous history) yields null while HEAD is
                    // force-updated to the commit. Log the resolved ref rather than dereferencing that null.
                    logger.debug("Checked out {}", ref != null ? ref.getName() : resolvedCommitId);
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
            // Redact both the DataStoreException's own top-level message AND the wrapped cause chain: JGit's
            // TransportException (thrown by TransportHttp/TransportGitSsh on auth/network failures) embeds the
            // target URI in its own getMessage() with only the password stripped (via URIish#setPass(null)) --
            // the username (e.g. a PAT-style credential used as username) is left in. That cause message is what
            // log4j2 prints as "Caused by:" and what failure-url reporting persists, so redactCredentialsInChain
            // scrubs every level of the chain before it is attached.
            throw new DataStoreException("Failed to crawl Git repository " + redactUrl(uri), redactCredentialsInChain(e));
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
        // Pass the RAW handlerParameter string, not getHandlerParameterMap() (which ParameterUtil.parse has
        // already decrypted): buildHandlerParameter edits only prev_commit_id/prev_source_ref and preserves
        // every other line verbatim, so encrypted values such as password={cipher}... keep their stored form
        // instead of being rewritten decrypted on every crawl.
        final String paramStr = buildHandlerParameter(dataConfig.getHandlerParameter(), toCommitId.name(), sourceRef);
        dataConfig.setHandlerParameter(paramStr);
        if (logger.isDebugEnabled()) {
            // Do NOT log paramStr: it is the full handlerParameter string and can still carry the raw uri
            // userinfo (the uri parameter is not an encrypted key). Log only non-sensitive identifiers;
            // sourceRef is already redacted (the currentSourceRef built with redactUrl(uri) in storeData).
            logger.debug("Updating data config {} to commit {} (source_ref={}).", dataConfig.getId(), toCommitId.name(), sourceRef);
        }
        ComponentUtil.getComponent(DataConfigBhv.class).update(dataConfig);
        logger.info("Updated DataConfig: {}", dataConfig.getId());
    }

    /**
     * Rebuilds the {@code handlerParameter} string with {@link #PREV_COMMIT_ID} and {@link #PREV_SOURCE_REF}
     * applied using an "update in place if the key already exists, otherwise append" strategy so that existing
     * keys keep their position and are never duplicated.
     * <p>
     * The <em>raw</em> stored string is edited line by line rather than being rebuilt from
     * {@link DataConfig#getHandlerParameterMap()}: that map is already decrypted (by {@code ParameterUtil.parse}),
     * so rebuilding from it would rewrite secret values such as {@code password={cipher}...} in their decrypted
     * form. Every line other than the two keys updated here is preserved verbatim, keeping any encrypted values
     * exactly as stored.
     * </p>
     *
     * @param handlerParameter The current raw {@code handlerParameter} string (may be {@code null}).
     * @param prevCommitId The commit ID to persist as {@link #PREV_COMMIT_ID}.
     * @param prevSourceRef The source ref to persist as {@link #PREV_SOURCE_REF}.
     * @return The rebuilt {@code handlerParameter} string.
     */
    protected String buildHandlerParameter(final String handlerParameter, final String prevCommitId, final String prevSourceRef) {
        final Map<String, String> pending = new LinkedHashMap<>();
        pending.put(PREV_COMMIT_ID, prevCommitId);
        pending.put(PREV_SOURCE_REF, prevSourceRef);

        final StringBuilder buf = new StringBuilder();
        if (handlerParameter != null) {
            for (final String line : handlerParameter.split("[\r\n]")) {
                if (StringUtil.isBlank(line)) {
                    continue;
                }
                final int pos = line.indexOf('=');
                final String key = (pos >= 0 ? line.substring(0, pos) : line).trim();
                if (buf.length() > 0) {
                    buf.append('\n');
                }
                if (pending.containsKey(key)) {
                    // Update PREV_COMMIT_ID / PREV_SOURCE_REF in place, keeping their original position.
                    buf.append(key).append('=').append(pending.remove(key));
                } else {
                    // Preserve every other line verbatim so encrypted values (password={cipher}...) survive.
                    buf.append(line);
                }
            }
        }
        pending.forEach((key, value) -> {
            if (buf.length() > 0) {
                buf.append('\n');
            }
            buf.append(key).append('=').append(value);
        });
        return buf.toString();
    }

    /**
     * Determines the value to pass to JGit's
     * {@link org.eclipse.jgit.api.FetchCommand#setInitialBranch(String)}.
     * <p>
     * JGit validates the initial branch as a concrete branch/tag name against the refs advertised by the
     * remote ({@code FetchProcess.isInitialBranchMissing}). The default {@code commit_id} is the literal
     * string {@code "HEAD"}, which is <em>not</em> a branch name: under protocol v2 the remote {@code HEAD}
     * is advertised only when JGit itself requests it (an unborn local {@code HEAD}), so on a re-crawl of a
     * persistent {@code repository_path} -- where the local {@code HEAD} is already born -- it is absent and
     * the fetch fails with {@code "Remote branch 'HEAD' not found in upstream origin"}. Returning
     * {@code null} for the default {@code HEAD}/blank case selects JGit's documented "use the branch pointed
     * to by HEAD" behavior (equivalent to not calling {@code setInitialBranch} at all), while an explicitly
     * configured branch/tag is still pinned unchanged so the common non-default case behaves exactly as
     * before.
     * </p>
     * <p>
     * A {@code commit_id} that is a full 40-character object id is mapped to {@code null} for the same
     * reason: a SHA is not a branch or tag name either, so JGit's validation rejects it and the fetch fails
     * with {@code "Remote branch '<sha>' not found in upstream origin"} on <em>every</em> run. Skipping the
     * validation is safe because the refspec fetches the branches regardless, and
     * {@link #resolveToCommitId(Repository, String, String)} then resolves the SHA against the local
     * repository (failing with a clear message if it is not reachable from any fetched ref).
     * </p>
     * <p>
     * Known limitation: an <em>abbreviated</em> SHA (e.g. {@code 6973e6a}) is not recognized here --
     * {@link ObjectId#isId(String)} matches only the full 40-character form -- and is therefore still passed
     * through and still rejected by JGit's validation. It is indistinguishable from a legitimate branch name
     * at this point (a branch may literally be named {@code 6973e6a}), so it is left to the existing
     * branch/tag handling rather than guessed at.
     * </p>
     *
     * @param commitId The configured {@code commit_id}.
     * @return The concrete branch/tag to pin, or {@code null} to use the remote's default branch.
     */
    protected String resolveInitialBranch(final String commitId) {
        if (StringUtil.isBlank(commitId) || org.eclipse.jgit.lib.Constants.HEAD.equals(commitId) || ObjectId.isId(commitId)) {
            return null;
        }
        return commitId;
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
     * <p>
     * Some transports/servers don't advertise the git symref capability for {@code HEAD}, in which case the
     * advertised {@code HEAD} ref is not symbolic. Mirroring JGit's own
     * {@code CloneCommand#findBranchToCheckout(FetchResult)}, {@code refs/heads/*} is scanned for a ref whose
     * object id matches {@code HEAD}'s object id so a real branch name can still be identified by content; a
     * bare commit SHA is used only if genuinely nothing matches (detached/anonymous history). A raw SHA here
     * would otherwise make {@link #isSameSource(String, String)} flip-flop across runs with no actual
     * branch/uri change, since the SHA moves as the branch advances.
     * </p>
     * <p>
     * On a re-crawl of a persistent {@code repository_path} the remote {@code HEAD} is often not advertised
     * at all: under protocol v2, JGit only requests {@code HEAD} when the local {@code HEAD} is unborn, so
     * once the first crawl has borne it the ref advertisement is filtered to the fetch refspec's prefixes
     * (e.g. {@code refs/heads/}) and carries no {@code HEAD}. In that case the local {@code HEAD}'s symref
     * target -- established by the first crawl -- is used via
     * {@link #resolveLocalHeadTarget(Repository, FetchResult, RefSpec, String)} instead of falling back to the
     * literal {@code "HEAD"} string, which would otherwise make {@code prev_source_ref} flip-flop between
     * {@code refs/heads/main} and {@code HEAD} and force a spurious full reindex.
     * </p>
     *
     * @param repository The Git repository.
     * @param fetchResult The result of the fetch operation.
     * @param refSpec The refspec used for the fetch, used to tell a branch deleted upstream apart from one the
     *            refspec simply does not cover; may be {@code null} to skip that check.
     * @param commitId The configured commit ID.
     * @return The resolved commit reference to use for checkout and resolution.
     * @throws IOException If updating the local {@code HEAD} fails.
     */
    protected String resolveDefaultBranch(final Repository repository, final FetchResult fetchResult, final RefSpec refSpec,
            final String commitId) throws IOException {
        if (StringUtil.isNotBlank(commitId) && !org.eclipse.jgit.lib.Constants.HEAD.equals(commitId)) {
            return commitId;
        }
        final Ref headRef = fetchResult.getAdvertisedRef(org.eclipse.jgit.lib.Constants.HEAD);
        if (headRef == null) {
            return resolveLocalHeadTarget(repository, fetchResult, refSpec, commitId);
        }
        if (headRef.isSymbolic()) {
            final String targetName = headRef.getTarget().getName();
            linkLocalHead(repository, targetName);
            return targetName;
        }
        final String matchedBranch = findAdvertisedBranchByObjectId(fetchResult.getAdvertisedRefs(), headRef.getObjectId());
        if (matchedBranch != null) {
            linkLocalHead(repository, matchedBranch);
            return matchedBranch;
        }
        return headRef.getObjectId().name();
    }

    /**
     * Resolves the local {@code HEAD}'s symbolic target (e.g. {@code refs/heads/main}). Used as a fallback by
     * {@link #resolveDefaultBranch(Repository, FetchResult, RefSpec, String)} when the remote {@code HEAD} is
     * not advertised on a fetch (a protocol-v2 re-crawl of a persistent {@code repository_path} whose local
     * {@code HEAD} is already born). Returning the symref target keeps the resolved ref -- and therefore
     * {@code prev_source_ref} -- identical to what the first crawl recorded, so incremental crawling keeps
     * working across runs. When the local {@code HEAD} is missing or detached (no symref, e.g.
     * detached/anonymous history), {@code commitId} is returned unchanged, preserving the prior behavior.
     * <p>
     * The local {@code HEAD} is a cached copy of what the remote's default branch was on the <em>first</em>
     * crawl, so it is verified against the current advertisement before being trusted. If the remote's default
     * branch is renamed or deleted (e.g. {@code master} -> {@code main}), the stale local branch is not pruned
     * by the fetch (no {@code setRemoveDeletedRefs(true)}, and it is no longer advertised to be updated), so it
     * still resolves -- to its last-known commit. Silently trusting it would diff that unchanged commit against
     * itself, index nothing, and report success, which (unless {@code delete_old_docs=false}) additionally lets
     * Fess prune every previously-indexed document of this config. Failing with an actionable message is
     * strictly more diagnosable than that.
     * </p>
     * <p>
     * The check is gated on {@code refSpec} actually covering the target: with a narrowed {@code ref_specs}
     * (e.g. {@code +refs/heads/foo:refs/heads/foo}) the advertisement is filtered to that prefix, so a local
     * {@code HEAD} pointing outside it is legitimately absent and must not be mistaken for one deleted upstream.
     * </p>
     *
     * @param repository The Git repository.
     * @param fetchResult The result of the fetch operation, whose advertised refs the target is verified
     *            against; may be {@code null} to skip that verification.
     * @param refSpec The refspec used for the fetch; the verification is skipped unless it covers the target.
     * @param commitId The configured commit ID, returned unchanged when the local {@code HEAD} is not symbolic.
     * @return The local {@code HEAD}'s symref target, or {@code commitId} if it is missing or detached.
     * @throws IOException If reading the local {@code HEAD} fails.
     */
    protected String resolveLocalHeadTarget(final Repository repository, final FetchResult fetchResult, final RefSpec refSpec,
            final String commitId) throws IOException {
        final Ref localHead = repository.exactRef(org.eclipse.jgit.lib.Constants.HEAD);
        if (localHead == null || !localHead.isSymbolic()) {
            return commitId;
        }
        final String targetName = localHead.getTarget().getName();
        if (fetchResult != null && refSpec != null && refSpec.matchSource(targetName) && fetchResult.getAdvertisedRef(targetName) == null) {
            throw new DataStoreException("The branch '" + targetName
                    + "' that a previous crawl resolved as this repository's default branch is no longer advertised by the remote; "
                    + "it was most likely renamed or deleted upstream. Set commit_id to the new branch name, or remove the "
                    + "repository_path directory so the remote's current default branch is picked up again.");
        }
        return targetName;
    }

    /**
     * Links the local {@code HEAD} to the given target ref name, without writing a reflog entry.
     *
     * @param repository The Git repository.
     * @param targetName The ref name to link {@code HEAD} to (e.g. {@code refs/heads/main}).
     * @throws IOException If updating the local {@code HEAD} fails.
     */
    private void linkLocalHead(final Repository repository, final String targetName) throws IOException {
        final RefUpdate newHead = repository.updateRef(org.eclipse.jgit.lib.Constants.HEAD);
        newHead.disableRefLog();
        newHead.link(targetName);
    }

    /**
     * Scans the given advertised refs for one under {@code refs/heads/} whose object id matches {@code headId}.
     * Used by {@link #resolveDefaultBranch(Repository, FetchResult, RefSpec, String)} to identify a branch name by
     * content when the advertised {@code HEAD} is not symbolic. This is similar in intent to JGit's own
     * {@code CloneCommand#findBranchToCheckout(FetchResult)}, but does NOT replicate its tiebreak: when several
     * advertised branches share {@code HEAD}'s object id, JGit specially prefers {@code refs/heads/master},
     * whereas this returns the first match in advertised-ref order. That is fine here because advertised-ref
     * order is deterministic (server-advertised), not a source of run-to-run flapping; it only changes which
     * branch name is picked on a genuine tie.
     *
     * @param advertisedRefs The refs advertised by the remote.
     * @param headId The object id that the remote's {@code HEAD} points at, or {@code null}.
     * @return The name of a matching {@code refs/heads/*} ref, or {@code null} if none matches.
     */
    protected String findAdvertisedBranchByObjectId(final Iterable<Ref> advertisedRefs, final ObjectId headId) {
        if (headId == null) {
            return null;
        }
        for (final Ref ref : advertisedRefs) {
            final String name = ref.getName();
            if (name != null && name.startsWith(org.eclipse.jgit.lib.Constants.R_HEADS) && headId.equals(ref.getObjectId())) {
                return name;
            }
        }
        return null;
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
            // Report the ref that actually failed to resolve (resolvedCommitId), not the configured commit_id:
            // e.g. a configured "HEAD" resolves to "refs/heads/main", and it is that resolved ref which was
            // unresolvable. The original configured value is still included for context.
            throw new DataStoreException("Could not resolve commit_id '" + resolvedCommitId + "' (configured commit_id was '" + commitId
                    + "'). The branch/tag may not exist, or may have been renamed/deleted upstream.");
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
        try {
            deleteStaleLockFiles(new File(repositoryPath, ".git"));
        } catch (final RuntimeException | Error e) {
            // deleteStaleLockFiles already catches the IOException/UncheckedIOException that Files.walk can
            // throw; any OTHER throwable (e.g. a SecurityException under a SecurityManager) would otherwise
            // escape before the lock is merged into the caller's finally-guarded configMap, leaking the lock
            // (and its channel) for the life of the JVM -- exactly the permanent crawl-blocking DoS this
            // locking exists to prevent. Release on any throw and rethrow.
            releaseRepositoryLock(configMap);
            throw e;
        }
    }

    /**
     * Deletes stale JGit {@code *.lock} files under the given {@code .git} directory. Must only be called
     * while holding the {@link #lockRepositoryPath} advisory lock, which guarantees the files are stale.
     * <p>
     * This scan is best-effort and must never propagate a failure: it runs immediately after the advisory
     * lock has already been acquired and stored by the caller, with no surrounding try/catch, so an
     * uncaught exception here would skip the caller's cleanup and leak that lock for the life of the JVM.
     * Individual file {@code delete()} failures are already just logged; both {@link IOException} (thrown by
     * {@link Files#walk(Path, java.nio.file.FileVisitOption...)} itself) and {@link UncheckedIOException}
     * (which {@code Files.walk()}'s returned {@link Stream} can throw mid-traversal, e.g. on a
     * permission-denied or concurrently-deleted subdirectory) are therefore caught and logged rather than
     * allowed to escape.
     * </p>
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
        } catch (final UncheckedIOException e) {
            // Thrown by the Stream during traversal (not by Files.walk() itself), so it is not an IOException
            // and would otherwise bypass the catch above. Log the wrapped cause, same as the IOException case.
            logger.warn("Failed to scan for stale Git lock files under {}.", gitDir.getAbsolutePath(),
                    e.getCause() != null ? e.getCause() : e);
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

    /** Matches a scheme + {@code "://"} prefix (e.g. {@code https://}); used by {@link #maskConservatively(String)}
     *  to separate the scheme from the remainder of an otherwise-unparseable URL. */
    private static final Pattern SCHEME_PREFIX_PATTERN = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]*://");

    /**
     * Redacts any user-info (e.g. {@code user:token@}) embedded in a Git URI so it is safe to log, persist in
     * {@link #PREV_SOURCE_REF}, and index (see {@code resultMap.put("uri", ...)} in {@link #processFile}).
     * <p>
     * This method is fail-CLOSED: on any parse ambiguity it masks conservatively rather than ever returning
     * credential-bearing input unchanged. {@link java.net.URI} is deliberately NOT used here: it is a strict
     * RFC-3986 parser that throws {@link URISyntaxException} on scp-style Git remotes ({@code user@host:path})
     * and on unescaped reserved characters (e.g. {@code @}, {@code /}) that commonly appear in real Git
     * tokens/passwords -- and a naive implementation would return the raw, unredacted url on any such parse
     * failure. {@link URIish} is used instead: it is already a project dependency (used elsewhere in this file
     * for the actual git operations) and is far more lenient with Git's real-world remote URL syntax.
     * </p>
     * <p>
     * Known residual limitation: {@link URIish} itself can misparse an authority containing an unescaped
     * {@code /} inside the password by folding the whole authority into the path instead of throwing; this is
     * detected here (a scheme was recognized but no host was) and handled by {@link #maskConservatively(String)}.
     * The equivalent failure for a schemeless scp-style URL (e.g. {@code user:pa/ss@host:path}) is not
     * specifically detected, since real scp-style Git remotes authenticate via SSH keys rather than
     * URL-embedded passwords, making that combination exotic in practice.
     * </p>
     *
     * @param url The Git URI.
     * @return The URI with any user-info component removed, or a conservatively-masked string if the input
     *         could not be confidently parsed.
     */
    protected String redactUrl(final String url) {
        if (StringUtil.isBlank(url)) {
            return url;
        }
        try {
            final URIish parsed = new URIish(url);
            if (parsed.getUser() != null || parsed.getPass() != null) {
                return parsed.setUser(null).setPass(null).toString();
            }
            if ((parsed.getScheme() != null || SCHEME_PREFIX_PATTERN.matcher(url).find()) && parsed.getHost() == null) {
                // A scheme was recognized but no host was -- typically because an unescaped '/' inside a
                // password broke authority parsing and the whole authority (including any credentials) was
                // folded into the path instead of being reported as user/pass. Treat this as unparseable.
                //
                // The SCHEME_PREFIX_PATTERN check additionally covers an uppercase/mixed-case scheme (e.g.
                // "HTTPS://"): URIish's internal SCHEME_P regex is lowercase-only, so it fails to match the
                // normal FULL_URI pattern for such input and silently falls through to the lenient
                // LOCAL_FILE catch-all, which treats the whole string as an opaque local path -- scheme,
                // host, user and pass all come back null. Matching the original input against
                // SCHEME_PREFIX_PATTERN (case-insensitive by construction) detects that case too, without
                // misrouting a genuine schemeless local path (which never matches the pattern).
                return maskConservatively(url);
            }
            return url;
        } catch (final URISyntaxException | RuntimeException e) {
            // RuntimeException covers cases like NumberFormatException on an out-of-range port, which
            // URIish's constructor can throw without wrapping it as a URISyntaxException. Never fall through
            // to returning the raw url.
            return maskConservatively(url);
        }
    }

    /**
     * Conservatively masks anything that looks like embedded user-info in a URL that {@link URIish} could not
     * confidently parse. Strips everything between an optional {@code scheme://} prefix and the last {@code @}
     * found afterward (or, for scp-style/schemeless input, the last {@code @} anywhere in the string), on the
     * assumption that a Git remote URL never legitimately needs a bare {@code @} in that position other than as
     * a user-info separator. If no {@code @} is found there, there is nothing recognizable to strip and the
     * input is returned unchanged.
     *
     * @param url The url to mask.
     * @return The masked url.
     */
    protected String maskConservatively(final String url) {
        final Matcher schemeMatcher = SCHEME_PREFIX_PATTERN.matcher(url);
        final String prefix;
        final String rest;
        if (schemeMatcher.find()) {
            prefix = schemeMatcher.group();
            rest = url.substring(prefix.length());
        } else {
            prefix = StringUtil.EMPTY;
            rest = url;
        }
        final int lastAt = rest.lastIndexOf('@');
        if (lastAt < 0) {
            return prefix + rest;
        }
        return prefix + rest.substring(lastAt + 1);
    }

    /** Matches a scheme + {@code "://"} + userinfo + {@code "@"} occurring anywhere in free text (e.g. inside a
     *  JGit exception message), so the userinfo can be stripped without disturbing the rest of the message.
     *  Used by {@link #redactCredentials(String)}. */
    private static final Pattern EMBEDDED_CREDENTIAL_PATTERN = Pattern.compile("([A-Za-z][A-Za-z0-9+.-]*://)[^\\s/@]+@");

    /** Safety bound on the cause-chain recursion in {@link #redactCredentialsInChain(Throwable)}: real JGit
     *  chains are only a handful deep, but a cyclic chain (constructible via {@link Throwable#initCause(Throwable)},
     *  e.g. {@code a -> b -> a}, which rejects only direct self-reference) would otherwise recurse forever. At
     *  this depth the remaining cause is dropped rather than attached raw, so the result stays safe to
     *  log/persist even in that never-seen-in-practice case. */
    private static final int MAX_CAUSE_CHAIN_DEPTH = 20;

    /**
     * Strips any {@code scheme://userinfo@} occurring anywhere within free-form text (e.g. an exception
     * message), leaving the scheme and the rest of the text untouched. Unlike {@link #redactUrl(String)}, this
     * does not require the whole input to be a single URL: JGit exception messages typically embed the target
     * URI (with the password already stripped but the username left in) inside a larger sentence such as
     * {@code "https://user@host/repo.git: Connection refused"}.
     *
     * @param message The text to scrub.
     * @return The text with any embedded {@code scheme://userinfo@} stripped, or the input unchanged if it
     *         contained none (or was blank/{@code null}).
     */
    protected String redactCredentials(final String message) {
        if (StringUtil.isBlank(message)) {
            return message;
        }
        return EMBEDDED_CREDENTIAL_PATTERN.matcher(message).replaceAll("$1");
    }

    /**
     * Returns a copy of the given throwable's cause chain with any embedded Git-URL user-info (see
     * {@link #redactCredentials(String)}) stripped from every message in the chain, so it is safe to log or
     * persist to a failure-url record. Preserves each level's original stack trace and embeds the original
     * class name in the replacement message (so logs still show what type of error it was). A chain that needed
     * no redaction anywhere is returned as the same instance (no pointless wrapping).
     *
     * @param throwable The throwable (possibly with a cause chain) to sanitize.
     * @return A sanitized copy, or the original instance if nothing needed redaction, or {@code null} if the
     *         input was {@code null}.
     */
    protected Throwable redactCredentialsInChain(final Throwable throwable) {
        return redactCredentialsInChain(throwable, 0);
    }

    private Throwable redactCredentialsInChain(final Throwable throwable, final int depth) {
        if (throwable == null) {
            return null;
        }
        final String message = throwable.getMessage();
        final String redactedMessage = redactCredentials(message);
        final Throwable originalCause = throwable.getCause();
        // Stop recursing at MAX_CAUSE_CHAIN_DEPTH and drop the remaining cause: a cyclic chain would otherwise
        // recurse forever, and dropping the deeper cause keeps the result safe to log/persist even then.
        final Throwable redactedCause = depth >= MAX_CAUSE_CHAIN_DEPTH ? null : redactCredentialsInChain(originalCause, depth + 1);
        if (Objects.equals(message, redactedMessage) && redactedCause == originalCause) {
            return throwable;
        }
        final RuntimeException sanitized = new RuntimeException(
                throwable.getClass().getName() + (redactedMessage != null ? ": " + redactedMessage : StringUtil.EMPTY), redactedCause);
        sanitized.setStackTrace(throwable.getStackTrace());
        return sanitized;
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
                    // resultMap holds the raw DataConfig under "crawlingConfig"; DataConfig.toString() emits the
                    // un-redacted handlerParameter (uri userinfo, and a cleartext password after a write-back), so
                    // log a copy without it -- the other credential-bearing entries (username/password/uri) were
                    // already removed/redacted above.
                    final Map<String, Object> logMap = new LinkedHashMap<>(resultMap);
                    logMap.remove("crawlingConfig");
                    logger.debug("resultMap: {}", logMap);
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
            // The exception message may embed a Git URL with credentials (see redactCredentials); scrub the
            // chain before it is logged or persisted. The original objects are still used below for the
            // instanceof/errorName checks, which only read class names (never sensitive).
            final Throwable redactedException = redactCredentialsInChain(e);
            logger.warn("Crawling Access Exception at : {}", dataMap, redactedException);

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
            failureUrlService.store(dataConfig, errorName, url, redactCredentialsInChain(target));
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            // Scrub any credential-bearing Git URL out of the message chain before logging/persisting it.
            final Throwable redactedThrowable = redactCredentialsInChain(t);
            logger.warn("Crawling Access Exception at : {}", dataMap, redactedThrowable);
            final String url = redactUrl(uri) + ":" + path;
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), url, redactedThrowable);

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
        // LogCommand.call() returns its own internal RevWalk (it ends with "return walk;") and never closes it,
        // so the returned Iterable can be cast back to that RevWalk and closed to avoid leaking pack-file handles.
        try (RevWalk revWalk = (RevWalk) git.log().call()) {
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
                if (!repoFile.exists()) {
                    repoFile.mkdirs();
                }
                // Checked on the .git directory itself, not the parent repoFile: storeData() may already have
                // created the parent directory (to have somewhere to put the advisory lock marker file) before
                // this method runs, so repoFile.exists() alone would no longer reliably indicate whether this
                // is a brand-new repository that still needs repository.create().
                final File gitDir = new File(repositoryPath, ".git");
                // A run killed mid-initialization can leave .git as a partial skeleton. JGit's
                // repository.create() writes .git/config LAST (its final cfg.save()) and refuses to re-run
                // once config exists, so config's presence reliably marks a completed initialization and its
                // absence marks an interrupted one. Because create() also runs BEFORE any fetch, a .git without
                // config can never contain fetched data, so discarding it loses nothing. Delete the partial
                // directory (only .git, never repositoryPath, whose advisory lock marker must survive) so the
                // repository is re-created below -- a plain re-create() alone cannot repair it, as refs.create()
                // throws on the already-existing refs/. A completed repo keeps its config and is left untouched.
                if (gitDir.exists() && !new File(gitDir, org.eclipse.jgit.lib.Constants.CONFIG).exists()) {
                    logger.warn("Removing an incomplete Git repository left by a previously interrupted crawl: {}",
                            gitDir.getAbsolutePath());
                    org.eclipse.jgit.util.FileUtils.delete(gitDir,
                            org.eclipse.jgit.util.FileUtils.RECURSIVE | org.eclipse.jgit.util.FileUtils.SKIP_MISSING);
                }
                final boolean gitDirExists = gitDir.exists();
                final Repository repository = FileRepositoryBuilder.create(gitDir);
                if (!gitDirExists) {
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
