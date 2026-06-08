package datawave.query.tables.keyword;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.tables.chained.strategy.FullChainStrategy;
import datawave.query.tables.keyword.extractor.TagCloudInputExtractor;

/**
 * Strategy for chaining UUID lookup and keyword extraction queries together, delegates to StatefulKeywordUUIDChainStrategy to handle individual batches of
 * lookups.
 */
public class KeywordUUIDChainStrategy extends FullChainStrategy<Entry<Key,Value>,Entry<Key,Value>> {
    public static final String CATEGORY_PARAMETER = "tag.cloud.category";
    private static final String KEYWORD_CATEGORY = "keyword";
    private static final String DEFAULT_CATEGORIES = KEYWORD_CATEGORY;

    /**
     * configurable batch size in the chain, -1 is no batching
     */
    private int batchSize = -1;
    private List<TagCloudInputExtractor> extractors = Collections.emptyList();

    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<Entry<Key,Value>> initialQueryResults, String latterLogicName) {
        throw new UnsupportedOperationException("Should be delegating to StatefulKeywordUUIDChainStrategy");
    }

    @Override
    public Iterator<Entry<Key,Value>> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths,
                    Iterator<Entry<Key,Value>> initialQueryResults, QueryLogic<Entry<Key,Value>> latterQueryLogic) {

        final List<TagCloudInputExtractor> activeExtractors = getActiveExtractors(initialQuery);
        final boolean buildKeywordCloud = isKeywordCloudRequested(initialQuery);

        Iterator<Entry<Key,Value>> wrapped = new Iterator<>() {
            private Iterator<Entry<Key,Value>> batchIterator;

            @Override
            public boolean hasNext() {
                while (batchIterator == null || (!batchIterator.hasNext() && initialQueryResults.hasNext())) {
                    try {
                        StatefulKeywordUUIDChainStrategy statefulChainStrategy = new StatefulKeywordUUIDChainStrategy(initialQuery, latterQueryLogic,
                                        activeExtractors, buildKeywordCloud);
                        statefulChainStrategy.setBatchSize(batchSize);
                        batchIterator = statefulChainStrategy.runChainedQuery(client, initialQuery, auths, initialQueryResults, latterQueryLogic);
                    } catch (Exception e) {
                        throw new DatawaveFatalQueryException("Failed to create next chained query: " + e.getMessage(), e);
                    }
                }

                // the iterator exists and has more, so always true
                return batchIterator.hasNext();
            }

            @Override
            public Entry<Key,Value> next() {
                return batchIterator.next();
            }
        };

        // prime the iterator to make sure latterQueryLogic is configured
        // noinspection ResultOfMethodCallIgnored
        wrapped.hasNext();

        return wrapped;
    }

    /**
     * Extract configured categories from the query, or default if empty
     *
     * @param settings
     * @return
     */
    private String[] getCategories(Query settings) {
        String categoryParam = settings.findParameter(CATEGORY_PARAMETER).getParameterValue();
        if (categoryParam.isEmpty()) {
            categoryParam = DEFAULT_CATEGORIES;
        }
        return categoryParam.split(",");
    }

    /**
     * Check if the keyword cloud should be constructed
     *
     * @param settings
     * @return
     */
    private boolean isKeywordCloudRequested(Query settings) {
        String[] categories = getCategories(settings);
        for (String category : categories) {
            if (category.equals(KEYWORD_CATEGORY)) {
                return true;
            }
        }

        return false;
    }

    /**
     * pull parameters to determine which type of tag cloud we are generating. All extraction is triggered beyond this point for the given ids
     *
     * @param settings
     * @return
     */
    private List<TagCloudInputExtractor> getActiveExtractors(Query settings) {
        List<TagCloudInputExtractor> activeExtractors = new ArrayList<>();

        for (String name : getCategories(settings)) {
            boolean found = false;
            for (TagCloudInputExtractor extractor : extractors) {
                if (extractor.getName().equals(name) && !activeExtractors.contains(extractor)) {
                    activeExtractors.add(extractor);
                    found = true;
                }
            }
            if (!found && !name.equals(KEYWORD_CATEGORY)) {
                throw new IllegalArgumentException("invalid category specified: " + name + " for property " + CATEGORY_PARAMETER);
            }
        }

        return activeExtractors;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public List<TagCloudInputExtractor> getExtractors() {
        return extractors;
    }

    public void setExtractors(List<TagCloudInputExtractor> extractors) {
        this.extractors = extractors;
    }
}
