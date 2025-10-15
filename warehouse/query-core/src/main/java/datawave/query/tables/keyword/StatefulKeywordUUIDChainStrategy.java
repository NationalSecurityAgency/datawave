package datawave.query.tables.keyword;

import static datawave.query.jexl.JexlASTHelper.deconstructIdentifier;
import static datawave.query.tables.keyword.KeywordQueryLogic.TAG_CLOUD_FIELDS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import datawave.util.keyword.KeywordResults;
import datawave.util.keyword.TokenScore;

/**
 * Strategy for chaining UUID lookup and keyword extraction queries together. Grabs the results of the lookupUUID query and uses these to generate the query
 * terms that are fed into a keyword extraction query.
 */
public class StatefulKeywordUUIDChainStrategy extends FullChainStrategy<Entry<Key,Value>,Entry<Key,Value>> {

    private static final Logger log = Logger.getLogger(StatefulKeywordUUIDChainStrategy.class);

    private int batchSize = -1;
    protected DocumentDeserializer deserializer;
    private final QueryLogic<Entry<Key,Value>> nextLogic;

    private final List<String> tagCloudFields;

    public StatefulKeywordUUIDChainStrategy(Query settings, QueryLogic<Entry<Key,Value>> nextLogic) {
        this.deserializer = DocumentSerialization.getDocumentDeserializer(settings);
        this.nextLogic = nextLogic;

        tagCloudFields = new ArrayList<>();
        QueryImpl.Parameter tagCloudFieldsParam = settings.findParameter(TAG_CLOUD_FIELDS);
        if (tagCloudFieldsParam != null) {
            String[] fields = tagCloudFieldsParam.getParameterValue().split(",");
            tagCloudFields.addAll(Arrays.asList(fields));
        }
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    // @Override
    // public Iterator<Entry<Key,Value>> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths,
    // Iterator<Entry<Key,Value>> initialQueryResults, QueryLogic<Entry<Key,Value>> latterQueryLogic) throws Exception {
    // // TODO maybe interface this? not sure we really have many use cases for this concept
    // if (latterQueryLogic instanceof KeywordQueryLogic) {
    // KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) latterQueryLogic;
    // keywordQueryLogic.setFieldedKeywordResults(fieldedKeywordResults);
    // }
    // return super.runChainedQuery(client, initialQuery, auths, initialQueryResults, latterQueryLogic);
    // }

    @Override
    protected Query buildLatterQuery(Query initialQuery, Iterator<Entry<Key,Value>> initialQueryResults, String latterLogicName) {
        log.debug("buildLatterQuery() called...");

        String queryString = captureResultsAndBuildQuery(initialQueryResults, batchSize);

        if (log.isDebugEnabled()) {
            log.debug("latter query is " + queryString);
        }

        if (queryString == null || queryString.isBlank()) {
            return null;
        }

        Query q = new QueryImpl(); // todo need to use a factory? consider not hardcoding this.
        q.setQuery(queryString);
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE); // todo choose something reasonable.
        q.setQueryAuthorizations(initialQuery.getQueryAuthorizations());
        q.setUserDN(initialQuery.getUserDN());

        q.addParameter(KeywordQueryLogic.TAG_CLOUD_CREATE, initialQuery.findParameter(KeywordQueryLogic.TAG_CLOUD_CREATE).getParameterValue());
        q.addParameter(KeywordQueryLogic.PREFERRED_VIEW_NAMES, initialQuery.findParameter(KeywordQueryLogic.PREFERRED_VIEW_NAMES).getParameterValue());
        q.addParameter(KeywordQueryLogic.TAG_CLOUD_MAX, initialQuery.findParameter(KeywordQueryLogic.TAG_CLOUD_MAX).getParameterValue());
        q.addParameter(KeywordQueryLogic.TAG_CLOUD_LANGUAGE, initialQuery.findParameter(KeywordQueryLogic.TAG_CLOUD_LANGUAGE).getParameterValue());

        return q;
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
     * @param initialQueryResults
     *            the raw results from the lookup uuid query, pre-transformation.
     * @param batchSize
     *            the number of entries we'll put in a single query for the keyword query logic. We expect this to be called multiple times until the input
     *            iterator is exhausted.
     * @return the next query to run against the KeywordQueryLogic, null if there were no query results.
     */
    public String captureResultsAndBuildQuery(Iterator<Entry<Key,Value>> initialQueryResults, int batchSize) {
        int count = 0;

        Map<String,List<KeywordResults>> fieldedKeywordResults = new HashMap<>();

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

            List<String> identifiers = null;
            List<String> languages = null;
            String docId = row + "/" + dataType + "/" + uid;

            Map<String,List<TokenScore>> fieldLabels = new HashMap<>();
            for (Entry<String,Attribute<? extends Comparable<?>>> data : documentData.entrySet()) {
                if (data.getKey().equals("LANGUAGE")) {
                    languages = KeywordQueryUtil.getStringValuesFromAttribute(data.getValue());
                } else if (data.getKey().equals("HIT_TERM")) {
                    identifiers = KeywordQueryUtil.getStringValuesFromAttribute(data.getValue());
                } else {
                    String baseField = deconstructIdentifier(data.getKey());
                    if (tagCloudFields.contains(baseField)) {
                        List<String> labels = KeywordQueryUtil.getStringValuesFromAttribute(data.getValue());
                        List<TokenScore> existing = fieldLabels.computeIfAbsent(baseField, k -> new ArrayList<>());

                        // TODO pull a score (if configured) and remove anything below the threshold
                        labels.forEach(label -> existing.add(new TokenScore(label, 1.0f)));
                    }
                }
            }

            // build up the results for each field found
            for (String field : fieldLabels.keySet()) {
                List<TokenScore> labels = fieldLabels.get(field);
                LinkedHashMap<String,Double> mapped = new LinkedHashMap<>();
                labels.forEach(label -> mapped.put(label.getToken(), label.getScore()));
                KeywordResults kr = new KeywordResults(docId, "event:" + field, field, entry.getKey().getColumnVisibility().toString(), mapped);
                fieldedKeywordResults.computeIfAbsent(field, k -> new ArrayList<>()).add(kr);
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

            queryTerms.add(queryTerm);
            count++;
        }

        if (nextLogic instanceof KeywordQueryLogic) {
            KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) nextLogic;
            keywordQueryLogic.setFieldedKeywordResults(fieldedKeywordResults);
        }

        return queryTerms.isEmpty() ? null : StringUtils.join(queryTerms, " ");
    }
}
