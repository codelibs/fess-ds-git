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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

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
import org.jsoup.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GitDataStore extends AbstractDataStore {
    private static final Logger logger = LoggerFactory.getLogger(GitDataStore.class);

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
                        try {
                            final String content = getContent(name, repo.open(treeWalk.getObjectId(0)));
                            dataMap.putAll(defaultDataMap);
                            final Map<String, Object> resultMap = new LinkedHashMap<>();
                            resultMap.putAll(paramMap);
                            resultMap.put("path", path);
                            resultMap.put("attributes", treeWalk.getAttributes());
                            resultMap.put("depth", treeWalk.getDepth());
                            resultMap.put("fileMode", treeWalk.getFileMode());
                            resultMap.put("name", name);
                            resultMap.put("operationType", treeWalk.getOperationType());
                            resultMap.put("pathLength", treeWalk.getPathLength());
                            resultMap.put("treeCount", treeWalk.getTreeCount());
                            resultMap.put("crawlingConfig", dataConfig);
                            resultMap.put("content", content);
                            resultMap.put("contentLength", content.length());

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

    protected String getContent(final String name, final ObjectLoader objectLoader) throws IOException {
        final String mimeType = getMimeType(name, objectLoader);
        Extractor extractor = ComponentUtil.getExtractorFactory().getExtractor(mimeType);
        if (extractor == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("use a defautl extractor as tikaExtractor by {}", mimeType);
            }
            extractor = ComponentUtil.getComponent("tikaExtractor");
        }
        try (ObjectStream os = objectLoader.openStream()) {
            return extractor.getText(os, null).getContent();
        }
    }

    protected String getMimeType(final String name, final ObjectLoader objectLoader) throws IOException {
        final MimeTypeHelper mimeTypeHelper = ComponentUtil.getComponent(MimeTypeHelper.class);
        try (ObjectStream os = objectLoader.openStream()) {
            return mimeTypeHelper.getContentType(os, name);
        }
    }
}
