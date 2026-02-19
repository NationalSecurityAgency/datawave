package datawave.query.tables.keyword;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.tables.chained.strategy.FullChainStrategy;
import datawave.query.tables.keyword.extractor.TagCloudInputExtractor;
import datawave.query.tables.keyword.extractor.TagCloudInputExtractorException;
import datawave.query.tables.keyword.transform.TagCloudInputTransformer;
import datawave.util.keyword.TagCloudPartition;

/**
 * Strategy for chaining UUID lookup and keyword extraction queries together. Grabs the results of the lookupUUID query and uses these to generate the query
 * terms that are fed into a keyword extraction query.
 */
public class StatefulKeywordUUIDChainStrategy extends FullChainStrategy<Entry<Key,Value>,Entry<Key,Value>> {
    private static final Logger log = Logger.getLogger(StatefulKeywordUUIDChainStrategy.class);

    private int batchSize = -1;
    protected DocumentDeserializer deserializer;
    private final QueryLogic<Entry<Key,Value>> nextLogic;
    // configured extractors to use on the data from nextLogic
    private final List<TagCloudInputExtractor> extractors;
    // will be true when a keyword query should be run, false otherwise
    private final boolean runKeywordQuery;
    private boolean addedExtractedData = false;

    public StatefulKeywordUUIDChainStrategy(Query settings, QueryLogic<Entry<Key,Value>> nextLogic, List<TagCloudInputExtractor> extractors,
                    boolean runKeywordQuery) {
        this.deserializer = DocumentSerialization.getDocumentDeserializer(settings);
        this.nextLogic = nextLogic;
        this.extractors = extractors;
        this.runKeywordQuery = runKeywordQuery;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<Entry<Key,Value>> initialQueryResults, String latterLogicName) {
        log.debug("buildLatterQuery() called...");

        log.debug("initializing extractors");
        for (TagCloudInputExtractor tagCloudInputExtractor : extractors) {
            tagCloudInputExtractor.initialize(initialQuery);
        }

        log.debug("building query and extracting data");
        String queryString = captureResultsAndBuildQuery(initialQueryResults, batchSize);

        if (log.isDebugEnabled()) {
            log.debug("latter query is " + queryString);
        }

        // if there was no extracted data and no query there is nothing to do
        if (!addedExtractedData && StringUtils.isBlank(queryString)) {
            return null;
        }

        Query q = new QueryImpl(); // todo need to use a factory? consider not hardcoding this.
        q.setQuery(queryString);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE); // todo choose something reasonable.
        q.setQueryAuthorizations(initialQuery.getQueryAuthorizations());
        q.setUserDN(initialQuery.getUserDN());

        q.setParameters(initialQuery.getParameters());
        return q;
    }

    // TODO-crwill9 better refine this javadoc
    /**
     * Generates queries for the KeywordQueryLogic and run extractors
     *
     * @param initialQueryResults
     *            the raw results from the lookup uuid query, pre-transformation.
     * @param batchSize
     *            the number of entries we'll put in a single query for the keyword query logic. We expect this to be called multiple times until the input
     *            iterator is exhausted.
     * @return the next query to run against the KeywordQueryLogic, null if there were no query results.
     */
    public String captureResultsAndBuildQuery(Iterator<Entry<Key,Value>> initialQueryResults, int batchSize) {
        int count = 0;

        Set<String> queryTerms = new HashSet<>();
        while (initialQueryResults.hasNext() && (batchSize == -1 || count < batchSize)) {
            Entry<Key,Value> entry = initialQueryResults.next();
            Entry<Key,Document> documentEntry = deserializer.apply(entry);

            Key documentKey = documentEntry.getKey();

            String row = documentKey.getRow().toString();
            String colf = documentKey.getColumnFamily().toString();

            int index = colf.indexOf("\0");
            Preconditions.checkArgument(-1 != index);

            String dataType = colf.substring(0, index);
            String uid = colf.substring(index + 1);

            Document document = documentEntry.getValue();
            final Map<String,Attribute<? extends Comparable<?>>> documentData = document.getDictionary();

            String docId = row + "/" + dataType + "/" + uid;

            // apply all extractors to the document
            for (TagCloudInputExtractor extractor : extractors) {
                try {
                    extractor.extract(documentKey, documentData);
                } catch (TagCloudInputExtractorException e) {
                    throw new RuntimeException("Failed to extractor failed to extract: " + extractor, e);
                }
            }

            if (runKeywordQuery) {
                // run query term extraction for next logic if needed
                queryTerms.add(extractKeywordQueryTerm(docId, documentData));
            }

            count++;
        }

        if (nextLogic instanceof KeywordQueryLogic) {
            // get all partitions from configured extractors
            List<Entry<Key,Value>> encodedData = new ArrayList<>();
            Set<TagCloudInputTransformer<?>> transformers = new HashSet<>();
            for (TagCloudInputExtractor extractor : extractors) {
                TagCloudInputTransformer<TagCloudPartition> transformer = extractor.getInputTransformer();
                transformers.add(transformer);
                TagCloudPartition partition = extractor.get();
                if (partition != null && !partition.getInputs().isEmpty()) {
                    Entry<Key,Value> transformed = transformer.encode(partition);

                    encodedData.add(transformed);
                    extractor.clear();
                }
            }

            KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) nextLogic;
            if (!encodedData.isEmpty()) {
                // pass the extracted partitions on to the keyword query logic
                keywordQueryLogic.setExternalData(encodedData, transformers);
                addedExtractedData = true;
            }
        }

        return queryTerms.isEmpty() ? null : StringUtils.join(queryTerms, " ");
    }

    /**
     * Generates queries for the KeywordQueryLogic. Minimally they will include things like:
     *
     * <pre>
     *      DOCUMENT:row/dataType/uid
     * </pre>
     *
     * But they will also potentially be enriched with the identifier, which appears in the HIT_TERM field of the lookupUUID response, and the LANGUAGE of the
     * original document, so they will look like:
     *
     * <pre>
     *     DOCUMENT:row/datatype/uid!PAGEID:12345%LANGUAGE:ENGLISH
     * </pre>
     *
     * @param docId
     * @param documentData
     * @return
     */
    private String extractKeywordQueryTerm(String docId, Map<String,Attribute<? extends Comparable<?>>> documentData) {
        List<String> identifiers = null;
        List<String> languages = null;

        for (Entry<String,Attribute<? extends Comparable<?>>> data : documentData.entrySet()) {
            if (data.getKey().equals("LANGUAGE")) {
                languages = KeywordQueryUtil.getStringValuesFromAttribute(data.getValue());
            } else if (data.getKey().equals("HIT_TERM")) {
                identifiers = KeywordQueryUtil.getStringValuesFromAttribute(data.getValue());
            }
        }

        String queryTerm = "DOCUMENT:" + docId;
        String language, identifier;
        if (((identifier = KeywordQueryUtil.chooseBestIdentifier(identifiers)) != null)) {
            if (log.isTraceEnabled()) {
                log.trace("Chose best identifier '" + identifier + "' from '" + identifiers + "' for query " + queryTerm);
            }
            queryTerm += "!" + identifier;
        } else if (log.isTraceEnabled()) {
            log.trace("No identifier found for query " + queryTerm);
        }

        if (((language = KeywordQueryUtil.chooseBestLanguage(languages)) != null)) {
            if (log.isTraceEnabled()) {
                log.trace("Chose best language '" + languages + "' from '" + languages + "' for query " + queryTerm);
            }
            queryTerm += "%LANGUAGE:" + language;
        } else if (log.isTraceEnabled()) {
            log.trace("No language found for query " + queryTerm);
        }

        return queryTerm;
    }
}
