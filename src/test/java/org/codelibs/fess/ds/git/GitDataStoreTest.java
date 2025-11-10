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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
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

    public void test_getUrl_withBaseUrl() {
        GitDataStore dataStore = new GitDataStore();
        DataStoreParams params = new DataStoreParams();
        params.put("base_url", "https://github.com/user/repo/blob/main/");

        String url = dataStore.getUrl(params, "src/main/java/Test.java");
        assertEquals("https://github.com/user/repo/blob/main/src/main/java/Test.java", url);
    }

    public void test_getUrl_withoutBaseUrl() {
        GitDataStore dataStore = new GitDataStore();
        DataStoreParams params = new DataStoreParams();

        String url = dataStore.getUrl(params, "src/main/java/Test.java");
        assertEquals("", url);
    }

    public void test_getUrl_withEmptyBaseUrl() {
        GitDataStore dataStore = new GitDataStore();
        DataStoreParams params = new DataStoreParams();
        params.put("base_url", "");

        String url = dataStore.getUrl(params, "src/main/java/Test.java");
        assertEquals("", url);
    }

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
