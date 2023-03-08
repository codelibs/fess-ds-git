/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.output.DeferredFileOutputStream;
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
import org.codelibs.fess.es.config.exbhv.DataConfigBhv;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.util.ComponentUtil;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectStream;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.FetchResult;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GitDataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(GitDataStore.class);

    protected static final String PASSWORD = "password";

    protected static final String USERNAME = "username";

    protected static final String COMMIT_ID = "commit_id";

    protected static final String REF_SPECS = "ref_specs";

    protected static final String DEFAULT_EXTRACTOR = "default_extractor";

    protected static final String CACHE_THRESHOLD = "cache_threshold";

    protected static final String EXTRACTORS = "extractors";

    protected static final String READ_INTERVAL = "read_interval";

    protected static final String TREE_WALK = "tree_walk";

    protected static final String REV_COMMIT = "rev_commit";

    protected static final String REPOSITORY = "repository";

    protected static final String URI = "uri";

    protected static final String BASE_URL = "base_url";

    protected static final String DIFF_ENTRY = "diff_entry";

    protected static final String GIT = "git";

    protected static final String CURRENT_COMMIT_ID = "current_commit_id";

    protected static final String PREV_COMMIT_ID = "prev_commit_id";

    protected static final String TEMP_REPOSITORY_PATH = "temp_repository_path";

    protected static final String REPOSITORY_PATH = "repository_path";

    protected static final String MAX_SIZE = "max_size";

    protected static final String INCLUDE_PATTERN = "include_pattern";

    protected static final String EXCLUDE_PATTERN = "exclude_pattern";

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
        final boolean isUpdateCommitId = prevCommit != null;
        final String baseUrl = paramMap.getAsString(BASE_URL);
        CredentialsProvider credentialsProvider = null;
        if (username != null && password != null) {
            credentialsProvider = new UsernamePasswordCredentialsProvider(username, password);
        }

        final Map<String, Object> configMap = createConfigMap(paramMap);
        configMap.put(URI, uri);

        final UrlFilter urlFilter = getUrlFilter(paramMap);

        logger.info("Git: {}", uri);

        final Repository repository = (Repository) configMap.get(REPOSITORY);
        configMap.put(REPOSITORY, repository);
        try (final Git git = new Git(repository)) {
            configMap.put(GIT, git);
            final FetchResult fetchResult = git.fetch().setForceUpdate(true).setRemote(uri).setRefSpecs(new RefSpec(refSpec))
                    .setInitialBranch(commitId).setCredentialsProvider(credentialsProvider).call();
            if (logger.isDebugEnabled()) {
                logger.debug("Fetch Result: {}", fetchResult.getMessages());
            }
            if (!hasCommitLogs(configMap)) {
                final Ref ref = git.checkout().setName(commitId).call();
                if (logger.isDebugEnabled()) {
                    logger.debug("Checked out {}", ref.getName());
                }
            }
            final ObjectId fromCommitId;
            if (StringUtil.isNotBlank(prevCommit)) {
                fromCommitId = repository.resolve(prevCommit);
            } else {
                fromCommitId = null;
            }
            final ObjectId toCommitId = repository.resolve(commitId);
            configMap.put(CURRENT_COMMIT_ID, toCommitId);
            try (DiffFormatter diffFormatter = new DiffFormatter(null)) {
                diffFormatter.setRepository(repository);
                logger.info("Rev: {} -> {}", fromCommitId, toCommitId);
                diffFormatter.scan(fromCommitId, toCommitId).forEach(entry -> {
                    final String path = entry.getNewPath();
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
            if (isUpdateCommitId) {
                updateDataConfig(dataConfig, toCommitId);
            }
        } catch (final Exception e) {
            throw new DataStoreException(e);
        } finally {
            try {
                repository.close();
            } finally {
                final File gitRepoPath = (File) configMap.get(TEMP_REPOSITORY_PATH);
                if (gitRepoPath != null) {
                    try (Stream<Path> walk = Files.walk(gitRepoPath.toPath())) {
                        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
                    } catch (final IOException e) {
                        logger.warn("Failed to delete " + gitRepoPath.getAbsolutePath(), e);
                    }
                }
            }
        }
    }

    protected void deleteDocument(final DataStoreParams paramMap, final Map<String, Object> configMap) {
        final DiffEntry entry = (DiffEntry) configMap.get(DIFF_ENTRY);
        try {
            final String url = getUrl(paramMap, entry.getOldPath());
            ComponentUtil.getIndexingHelper().deleteDocumentByUrl(ComponentUtil.getSearchEngineClient(), url);
        } catch (final Exception e) {
            logger.warn("Failed to delete the document {}.", entry);
        }
    }

    protected void updateDataConfig(final DataConfig dataConfig, final ObjectId toCommitId) {
        final String paramStr = dataConfig.getHandlerParameterMap().entrySet().stream().map(e -> {
            if (PREV_COMMIT_ID.equals(e.getKey())) {
                return e.getKey() + "=" + toCommitId.name();
            }
            return e.getKey() + "=" + e.getValue();
        }).collect(Collectors.joining("\n"));
        dataConfig.setHandlerParameter(paramStr);
        if (logger.isDebugEnabled()) {
            logger.debug("Updating data config by {}.", paramStr);
        }
        ComponentUtil.getComponent(DataConfigBhv.class).update(dataConfig);
        logger.info("Updated DataConfig: {}", dataConfig.getId());
    }

    protected String getFileName(final String path) {
        final int pos = path.lastIndexOf('/');
        if (pos == -1) {
            return path;
        }
        return path.substring(pos + 1);
    }

    protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final String uri = (String) configMap.get(URI);
        final DiffEntry diffEntry = (DiffEntry) configMap.get(DIFF_ENTRY);
        final String path = diffEntry.getNewPath();
        final StatsKeyObject statsKey = new StatsKeyObject(uri);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            crawlerStatsHelper.begin(statsKey);
            final RevCommit revCommit = getRevCommit(configMap, path);

            final String name = getFileName(path);
            logger.info("Crawling Path: {}", path);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
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
                        logger.warn("Could not get a text from {}.", uri, e);
                    } else {
                        logger.warn("Could not get a text from {}. {}", uri, e.getMessage());
                    }
                }

                resultMap.put("url", getUrl(paramMap, path));
                resultMap.put("uri", uri);
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
            logger.warn("Crawling Access Exception at : " + dataMap, e);

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
                url = uri + ":" + path;
            }
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, url, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : " + dataMap, t);
            final String url = uri + ":" + path;
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

    protected RevCommit getRevCommit(final Map<String, Object> configMap, final String path) throws GitAPIException {
        final Git git = (Git) configMap.get(GIT);
        final Iterator<RevCommit> revCommitIter = git.log().addPath(path).setMaxCount(1).call().iterator();
        if (!revCommitIter.hasNext()) {
            throw new DataStoreException("Failed to parse git log for " + path);
        }
        return revCommitIter.next();
    }

    protected String getUrl(final DataStoreParams paramMap, final String path) {
        final String baseUrl = paramMap.getAsString(BASE_URL);
        if (StringUtil.isNotBlank(baseUrl)) {
            return baseUrl + path;
        }
        return StringUtil.EMPTY;
    }

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
                final File gitRepoPath = File.createTempFile("fess-ds-git-", "");
                if (!gitRepoPath.delete()) {
                    throw new DataStoreException("Could not delete temporary file " + gitRepoPath);
                }
                gitRepoPath.mkdirs();
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

    protected String getMimeType(final String filename, final DeferredFileOutputStream out) throws IOException {
        final MimeTypeHelper mimeTypeHelper = ComponentUtil.getComponent(MimeTypeHelper.class);
        try (InputStream is = getContentInputStream(out)) {
            return mimeTypeHelper.getContentType(is, filename);
        }
    }

    protected InputStream getContentInputStream(final DeferredFileOutputStream out) throws IOException {
        if (out.isInMemory()) {
            return new ByteArrayInputStream(out.getData());
        }
        return new FileInputStream(out.getFile());
    }

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
