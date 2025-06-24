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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;
import org.eclipse.jgit.diff.DiffEntry;

public class GitDataStoreTest extends LastaFluteTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

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
