package io.logz.benchmarks.elasticsearch.elasticsearch;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import io.logz.benchmarks.elasticsearch.configuration.ElasticsearchConfiguration;
import io.logz.benchmarks.elasticsearch.exceptions.CouldNotCompleteBulkOperationException;
import io.logz.benchmarks.elasticsearch.exceptions.CouldNotExecuteSearchException;
import io.logz.benchmarks.elasticsearch.exceptions.CouldNotOptimizeException;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * Created by roiravhon on 9/19/16.
 */
public class ElasticsearchController {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchController.class);
    private static final int INDEX_LENGTH = 8;
    private static final int FIELD_CARDINALITY = 100;
    private static final int MAX_CHARS_IN_FIELD = 10;
    private static final int MIN_CHARS_IN_FIELD = 5;
    private static final String TEMPLATE_DOCUMENTS_RESOURCE_PATTERN = ".*templates/documents.*";
    private static final String TEMPLATE_SEARCHES_RESOURCE_PATTERN = ".*templates/searches.*";
    private static final String TIMESTAMP_PLACEHOLDER = "TIMESTAMP";
    private static final String RANDOM_STR_PLACEHOLDER = "RANDSTR";
    private static final String RANDOM_INT_PLACEHOLDER = "RANDINT";
    private static final String DEFAULT_TYPE = "benchmark";

    private final ElasticsearchConfiguration esConfig;
    private final ArrayList<String> randomFieldsList;
    private final ArrayList<String> rawDocumentsList;
    private final ArrayList<String> searchesList;
    private final RestHighLevelClient client;
    private final String indexName;
    private final Optional<String> indexPrefix;
    private final AtomicInteger lastSelectedDocument;
    private final AtomicInteger lastSelectedSearch;

    private boolean indexCreated = false;

    public ElasticsearchController(ElasticsearchConfiguration esConfig) {
        this.esConfig = esConfig;
        lastSelectedDocument = new AtomicInteger(0);
        lastSelectedSearch = new AtomicInteger(0);

        indexPrefix = Optional.ofNullable(esConfig.getIndexPrefix());

        indexName = indexPrefix.orElse("") + getRandomString(INDEX_LENGTH);

        logger.info("Random test index name set to: {}", indexName);

        client = initializeRestHighLevelClient(esConfig);
        randomFieldsList = generateRandomFieldList();
        rawDocumentsList = loadDocuments(esConfig.getDocumentsPath(), TEMPLATE_DOCUMENTS_RESOURCE_PATTERN);
        searchesList = loadDocuments(esConfig.getSearchesPath(), TEMPLATE_SEARCHES_RESOURCE_PATTERN);
    }

    private ArrayList<String> loadDocuments(String path, String resource) {
        if (path != null) {
            try {
                return getExternalDirectoryContent(path);
            } catch (RuntimeException e) {
                logger.error("failed to load documents from path " + path + ". Fallback, going to load default documents");
            }
        }

        return getResourceDirectoryContent(resource);
    }

    public void createIndex() {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put("index.number_of_shards", esConfig.getNumberOfShards())
                .put("index.number_of_replicas", esConfig.getNumberOfReplicas())
        );
        String mappings = "{ \"properties\" : { \"@timestamp\" : {\"type\" : \"date\", \"format\" : \"epoch_millis\"}}}";
        request.mapping(mappings, XContentType.JSON);
        try {
            logger.info("Creating test index on ES, named {} with {} shards and {} replicas", indexName, esConfig.getNumberOfShards(), esConfig.getNumberOfReplicas());
            client.indices().create(request, RequestOptions.DEFAULT);

            indexCreated = true;
        } catch (IOException e) {
            throw new RuntimeException("RestHighLevelClient: Could not create index in elasticsearch!", e);
        }
    }

    public void deleteIndex() {
        try {
            if (indexCreated) {
                logger.info("Deleting test index {} from ES", indexName);
                DeleteIndexRequest request = new DeleteIndexRequest(indexName);
                client.indices().delete(request, RequestOptions.DEFAULT);
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not delete index from ES!", e);
        }
    }

    public ArrayList<String> getMultipleDocuments(int numberOfDocument) {

        ArrayList<String> tempList = new ArrayList<>(numberOfDocument + 1);
        IntStream.range(0, numberOfDocument).forEach((i) -> tempList.add(getDocument()));
        return tempList;
    }

    public String getDefaultType() {
        return DEFAULT_TYPE;
    }

    public String getIndexName() {
        return indexName;
    }

    public Optional<String> getIndexPrefix() {
        return indexPrefix;
    }

    public String getSearchIndex() {
        return getIndexPrefix().isPresent() ? getIndexPrefix().get() + "*" : getIndexName();
    }

    // Return the number of failed documents
    public int executeBulk(List<String> documents) throws CouldNotCompleteBulkOperationException {
        int numberOfFailures = 0;
        try {
            BulkRequest bulkRequest = new BulkRequest();
            IntStream.range(0, documents.size()).forEach((index) -> {
                String doc = documents.get(index);
                bulkRequest.add(new IndexRequest(indexName).source(doc, XContentType.JSON));
            });
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if(bulkResponse.hasFailures()) {
                for(BulkItemResponse response: bulkResponse.getItems()) {
                    if(response.isFailed()) {
                        numberOfFailures = numberOfFailures + 1;
                    }
                }
            }

        } catch (IOException e) {
            throw new CouldNotCompleteBulkOperationException(e);
        } catch (Exception e) {
            logger.debug("Got interrupt, stopping indexing thread #{}", Thread.currentThread().getName());
            logger.trace("Got Interrupted while Bulk Operation!!", e);
        }
        return numberOfFailures;
    }

    // Returns the number of documents found
    public int executeSearch(String searchJSONQuery) throws CouldNotExecuteSearchException {
        int docsFound = 0;
        try {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                    new NamedXContentRegistry(searchModule.getNamedXContents()),
                    DeprecationHandler.IGNORE_DEPRECATIONS, searchJSONQuery)){
                sourceBuilder.parseXContent(parser);
            }
            SearchRequest request = new SearchRequest(indexName);
            request.source(sourceBuilder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            if (isSucceeded(response.status())) {
                docsFound = response.getHits().getHits().length;
                return docsFound;
            } else {
                logger.debug(response.status().name());
                throw new CouldNotExecuteSearchException();
            }
        } catch (IOException e) {
            throw new CouldNotExecuteSearchException(e);
        } catch (Exception e) {
            logger.debug("Got interrupt, stopping search thread #{}", Thread.currentThread().getName());
            logger.trace("Got Interrupted while search Operation!!", e);
        }
        return docsFound;
    }

    private boolean isSucceeded(RestStatus status) {
        return (status.getStatus() / 100) == 2;
    }

    public void executeForceMerge(int numberOfSegments) throws CouldNotOptimizeException {
        try {
            ForceMergeRequest forceMergeRequest = new ForceMergeRequest();
            forceMergeRequest.maxNumSegments(numberOfSegments);
            ForceMergeResponse response = client.indices()
                    .forcemerge(forceMergeRequest, RequestOptions.DEFAULT);
            RestStatus status = response.getStatus();
            if(!isSucceeded(status)) {
                throw new CouldNotOptimizeException(status.toString() + ":" + status.getStatus());
            }
        } catch (IOException e) {
            throw new CouldNotOptimizeException(e);
        }
    }

    public String getSearch() {
        String currSearch = searchesList.get(lastSelectedSearch.get() % searchesList.size());
        lastSelectedSearch.incrementAndGet();
        return currSearch;
    }

    private String getDocument() {

        String currDocument = rawDocumentsList.get(lastSelectedDocument.get() % rawDocumentsList.size());
        currDocument = currDocument.replace(TIMESTAMP_PLACEHOLDER, String.valueOf(System.currentTimeMillis()));

        // Replacing all marks with values
        while (true) {

            currDocument = currDocument.replaceFirst(RANDOM_STR_PLACEHOLDER, getRandomField());
            currDocument = currDocument.replaceFirst(RANDOM_INT_PLACEHOLDER, String.valueOf(getRandomFieldInt()));

            // Breaking out if we are done replacing
            if (!currDocument.contains(RANDOM_STR_PLACEHOLDER) && !currDocument.contains(RANDOM_INT_PLACEHOLDER))
                break;
        }

        lastSelectedDocument.incrementAndGet();
        return currDocument;
    }

    private String getRandomField() {
        int index = ThreadLocalRandom.current().nextInt(0, FIELD_CARDINALITY);
        return randomFieldsList.get(index);
    }

    private int getRandomFieldInt() {
        return ThreadLocalRandom.current().nextInt(0, 10000 + 1);
    }

    private String getRandomString(int length) {
        try {
            return UUID.randomUUID().toString().substring(0, length);
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("Cannot create random string of size " + length + "! this is probably due to internal parameters changes you made.", e);
        }
    }

    private String getRandomStringInRandomSize(int minSize, int maxSize) {
        int length = ThreadLocalRandom.current().nextInt(minSize, maxSize + 1);
        return getRandomString(length);
    }

    private RestHighLevelClient initializeRestHighLevelClient(ElasticsearchConfiguration esConfig) {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(esConfig.getElasticsearchAddress(), esConfig.getElasticsearchPort()))
                .setRequestConfigCallback(
                        builder -> builder.setConnectTimeout(5000)
                                .setSocketTimeout(60000))
        );
    }

    private ArrayList<String> generateRandomFieldList() {
        ArrayList<String> tempRandomFieldList = new ArrayList<>(FIELD_CARDINALITY);

        IntStream.range(0, FIELD_CARDINALITY).forEach((i) ->
                tempRandomFieldList.add(getRandomStringInRandomSize(MIN_CHARS_IN_FIELD, MAX_CHARS_IN_FIELD)));

        return tempRandomFieldList;
    }

    private ArrayList<String> getResourceDirectoryContent(String resourcePattern) {
        ArrayList<String> tempFilesContentList = new ArrayList<>();

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                                                        .setUrls(ClasspathHelper.forPackage("io.logz"))
                                                        .setScanners(new ResourcesScanner())
                                                        .filterInputsBy(new FilterBuilder().include(resourcePattern)));
        Set<String> properties = reflections.getResources(Pattern.compile(".*\\.json"));

        properties.forEach((resourceName) -> {

            URL resourceUrl = Resources.getResource(resourceName);
            try {
                tempFilesContentList.add(Resources.toString(resourceUrl, Charset.forName("utf-8")).replace("\n", ""));

            } catch (IOException e) {
                logger.info("Could not read file {}", resourceUrl.toString());
            }
        });

        if (tempFilesContentList.isEmpty())
            throw new RuntimeException("Did not find any files under "+ resourcePattern +"!");

        return tempFilesContentList;
    }

    private ArrayList<String> getExternalDirectoryContent(String path) {
        ArrayList<String> tempFilesContentList = new ArrayList<>();

        File directory = new File(path);
        if (!directory.isDirectory()) {
            throw new RuntimeException(path +" is not a directory!");
        }

        File[] documents = directory.listFiles();

        if (documents == null || documents.length == 0) {
            throw new RuntimeException("Did not find any files under "+ path +"!");
        }

        for (File doc : documents) {
            try {
                tempFilesContentList.add(String.join("", Files.readLines(doc, StandardCharsets.UTF_8)));
            } catch (IOException e) {
                logger.info("Could not read file {}", doc.getAbsolutePath());
            }
        }

        if (tempFilesContentList.isEmpty()) {
            throw new RuntimeException("Failed to load all files under " + path);
        }

        return tempFilesContentList;
    }

    public void stopClient() {
        try {
            client.close();
            logger.info("shutdown elasticsearch Rest High Level Client: Done!!");
        } catch (IOException e) {
            throw new RuntimeException("Failed to close Rest High Level Client");
        }
    }
}
