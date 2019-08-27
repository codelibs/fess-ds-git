/*
 * Copyright 2012-2018 CodeLibs Project and the Others.
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.codelibs.core.io.CopyUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.misc.Pair;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.helper.MimeTypeHelper;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectStream;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GitDataStore extends AbstractDataStore {
    private static final Logger logger = LoggerFactory.getLogger(GitDataStore.class);

    private static final String DEFAULT_EXTRACTOR = "default_extractor";

    private static final String CACHE_THRESHOLD = "cache_threshold";

    private static final String EXTRACTORS = "extractors";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final long readInterval = getReadInterval(paramMap);
        final String uri = paramMap.get("uri");
        if (StringUtil.isBlank(uri)) {
            throw new DataStoreException("uri is required.");
        }
        final String refSpec = paramMap.getOrDefault("ref_specs", "+refs/heads/*:refs/heads/*");
        final String commitId = paramMap.getOrDefault("commit_id", "refs/heads/master");

        final Map<String, Object> configMap = createConfigMap(paramMap);

        logger.info("Git: {}", uri);
        final InMemoryRepository repo = new InMemoryRepository(new DfsRepositoryDescription());
        try (final Git git = new Git(repo)) {
            git.fetch().setRemote(uri).setRefSpecs(new RefSpec(refSpec)).call();
            final ObjectId lastCommitId = repo.resolve(commitId);
            try (RevWalk revWalk = new RevWalk(repo)) {
                final RevCommit commit = revWalk.parseCommit(lastCommitId);
                final RevTree tree = commit.getTree();
                try (TreeWalk treeWalk = new TreeWalk(repo)) {
                    treeWalk.addTree(tree);
                    treeWalk.setRecursive(true);
                    boolean running = true;
                    while (treeWalk.next() && running) {
                        final String name = treeWalk.getNameString();
                        final String path = treeWalk.getPathString();
                        logger.info("Crawling Path: {}", path);

                        final Map<String, Object> dataMap = new HashMap<>();
                        dataMap.putAll(defaultDataMap);
                        final Map<String, Object> resultMap = new LinkedHashMap<>();
                        resultMap.putAll(paramMap);
                        try {
                            ObjectLoader objectLoader = repo.open(treeWalk.getObjectId(0));
                            DeferredFileOutputStream dfos = null;
                            try (ObjectStream in = objectLoader.openStream();
                                    DeferredFileOutputStream out = new DeferredFileOutputStream((Integer) configMap.get(CACHE_THRESHOLD),
                                            "fess-ds-git-", ".out", null)) {
                                dfos = out;
                                CopyUtil.copy(in, out);
                                out.flush();

                                String mimeType = getMimeType(name, out);
                                resultMap.put("mimetype", mimeType);
                                final Extractor extractor = getExtractor(mimeType, configMap);

                                final Map<String, String> params = new HashMap<>();
                                params.put(TikaMetadataKeys.RESOURCE_NAME_KEY, name);
                                try (ObjectStream os = objectLoader.openStream()) {
                                    String content = extractor.getText(os, params).getContent();
                                    if (content == null) {
                                        content = StringUtil.EMPTY;
                                    }
                                    resultMap.put("content", content);
                                    resultMap.put("contentLength", content.length());
                                }
                            } finally {
                                if (dfos != null && !dfos.isInMemory()) {
                                    if (!dfos.getFile().delete()) {
                                        logger.warn("Failed to delete {}.", dfos.getFile().getAbsolutePath());
                                    }
                                }
                            }

                            resultMap.put("path", path);
                            resultMap.put("attributes", treeWalk.getAttributes());
                            resultMap.put("depth", treeWalk.getDepth());
                            resultMap.put("fileMode", treeWalk.getFileMode());
                            resultMap.put("name", name);
                            resultMap.put("operationType", treeWalk.getOperationType());
                            resultMap.put("pathLength", treeWalk.getPathLength());
                            resultMap.put("treeCount", treeWalk.getTreeCount());
                            resultMap.put("crawlingConfig", dataConfig);

                            if (logger.isDebugEnabled()) {
                                logger.debug("resultMap: {}", resultMap);
                            }

                            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                                final Object convertValue = convertValue(entry.getValue(), resultMap);
                                if (convertValue != null) {
                                    dataMap.put(entry.getKey(), convertValue);
                                }
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug("dataMap: {}", dataMap);
                            }

                            callback.store(paramMap, dataMap);
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
                            if (target instanceof DataStoreCrawlingException) {
                                final DataStoreCrawlingException dce = (DataStoreCrawlingException) target;
                                url = dce.getUrl();
                                if (dce.aborted()) {
                                    running = false;
                                }
                            } else {
                                url = uri + ":" + path;
                            }
                            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                            failureUrlService.store(dataConfig, errorName, url, target);
                        } catch (final Throwable t) {
                            logger.warn("Crawling Access Exception at : " + dataMap, t);
                            final String url = uri + ":" + path;
                            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), url, t);

                            if (readInterval > 0) {
                                sleep(readInterval);
                            }

                        }
                    }
                }
            }
        } catch (final Exception e) {
            throw new DataStoreException(e);
        }
    }

    protected Map<String, Object> createConfigMap(final Map<String, String> paramMap) {
        final Map<String, Object> configMap = new HashMap<>();
        @SuppressWarnings("unchecked")
        final Pair<Pattern, String>[] extractors = StreamUtil.split(paramMap.get(EXTRACTORS), ",").get(stream -> stream.map(s -> {
            final String[] values = s.split(":");
            if (values.length != 2) {
                return null;
            }
            return new Pair<>(Pattern.compile(values[0]), values[1]);
        }).filter(Objects::nonNull).toArray(n -> new Pair[n]));
        configMap.put(EXTRACTORS, extractors);
        configMap.put(CACHE_THRESHOLD, Integer.parseInt(paramMap.getOrDefault(CACHE_THRESHOLD, "1000000")));
        configMap.put(DEFAULT_EXTRACTOR, paramMap.getOrDefault(DEFAULT_EXTRACTOR, "tikaExtractor"));
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
        } else {
            return new FileInputStream(out.getFile());
        }
    }

}
