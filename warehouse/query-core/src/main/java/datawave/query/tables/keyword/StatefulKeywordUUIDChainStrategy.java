package datawave.query.tables.keyword;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.DocumentSerialization;
import datawave.query.QueryParameters;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.tables.chained.strategy.FullChainStrategy;
import datawave.util.keyword.language.YakeLanguage;

/**
 * Strategy for chaining UUID lookup and keyword extraction queries together. Grabs the results of the lookupUUID query and uses these to generate the query
 * terms that are fed into a keyword extraction query.
 */
public class StatefulKeywordUUIDChainStrategy extends FullChainStrategy<Entry<Key,Value>,Entry<Key,Value>> {

    private static final Logger log = Logger.getLogger(StatefulKeywordUUIDChainStrategy.class);

    private int batchSize = -1;
    protected DocumentDeserializer deserializer;

    public StatefulKeywordUUIDChainStrategy(Query settings) {
        this.deserializer = DocumentSerialization.getDocumentDeserializer(settings);
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

        q.addParameter(QueryParameters.TAG_CLOUD_CREATE, initialQuery.findParameter(QueryParameters.TAG_CLOUD_CREATE).getParameterValue());
        q.addParameter(KeywordQueryLogic.PREFERRED_VIEW_NAMES, initialQuery.findParameter(KeywordQueryLogic.PREFERRED_VIEW_NAMES).getParameterValue());

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

            for (Entry<String,Attribute<? extends Comparable<?>>> data : documentData.entrySet()) {
                if (data.getKey().equals("LANGUAGE")) {
                    languages = getStringValuesFromAttribute(data.getValue());
                } else if (data.getKey().equals("HIT_TERM")) {
                    identifiers = getStringValuesFromAttribute(data.getValue());
                }
            }

            String queryTerm = "DOCUMENT:" + row + "/" + dataType + "/" + uid;
            String language, identifier;
            if (((identifier = chooseBestIdentifier(identifiers)) != null)) {
                if (log.isTraceEnabled()) {
                    log.trace("Chose best identifier '" + identifier + "' from '" + identifiers + "' for query " + queryTerm);
                }
                queryTerm += "!" + identifier;
            } else if (log.isTraceEnabled()) {
                log.trace("No identifier found for query " + queryTerm);
            }

            if (((language = chooseBestLanguage(languages)) != null)) {
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

        return queryTerms.isEmpty() ? null : StringUtils.join(queryTerms, " ");
    }

    /**
     * Choose the first identifier from a non-null, non-empty list of identifiers, otherwise return null.
     *
     * @param identifiers
     *            a list to choose from
     * @return the first identifier or null.
     */
    public static String chooseBestIdentifier(List<String> identifiers) {
        if (identifiers == null || identifiers.isEmpty()) {
            return null;
        }
        return identifiers.get(0);
    }

    /**
     * Choose the best language from a non-null, non-empty list of languages, otherwise return null.
     *
     * @param languages
     *            a list to choose from
     * @return the best identifier or null.
     */
    public static String chooseBestLanguage(List<String> languages) {
        if (languages == null || languages.isEmpty()) {
            return null;
        }

        for (String language : languages) {
            // if the language can't be found in the language registry, the language
            // registry will return English. So, if the language name returned by the
            // registry and the input language name match - it confirms we have
            // a good positive match, so just return that. If they don't match,
            // it's a good chance that we're just getting the default value from the registry.
            YakeLanguage yakeLanguage = YakeLanguage.Registry.find(language);
            if (yakeLanguage.getLanguageName().equalsIgnoreCase(language)) {
                return language;
            }
        }

        // if we get here, we couldn't find an ideal language, just return the first value, yake will default
        // to processing the data as if it were English.
        return languages.get(0);
    }

    /**
     * Read strings from a couple known Attribute types - TypeAttribute and Content. Relies on any TypeAttribute to have a delegate that produces a string in
     * order to render properly. Handles multivalued field by unpacking and flattening the Attributes class.
     * <p>
     * Generally LANGUAGE is expected to be a TypeAttribute, while HIT_TERM (which exposes the identifier) is expected to be Content.
     * </p>
     *
     * @param inputAttribute
     *            the attribute to extract strings from
     * @return the string version of the attribute value.
     */
    public static List<String> getStringValuesFromAttribute(Attribute<?> inputAttribute) {

        final List<String> values = new ArrayList<>();

        // a queue of attributes to process
        final List<Attribute<?>> attributeQueue = new ArrayList<>();
        attributeQueue.add(inputAttribute);

        // iterate over the attributes, possibly unpacking multi-valued attributes.
        final ListIterator<Attribute<?>> listIterator = attributeQueue.listIterator();
        while (listIterator.hasNext()) {
            String value = "";
            final Attribute<?> attribute = listIterator.next();
            if (attribute.getClass().isAssignableFrom(Attributes.class)) {
                Attributes attributes = (Attributes) inputAttribute;
                for (Attribute<?> childAttribute : attributes.getAttributes()) {
                    listIterator.add(childAttribute);
                }
            } else if (attribute.getClass().isAssignableFrom(TypeAttribute.class)) {
                value = attribute.toString();
            } else if (attribute.getClass().isAssignableFrom(Content.class)) {
                value = attribute.getData().toString();
            }

            // add the attribute string if we got one and it's non-blank
            if (!value.isBlank()) {
                values.add(value);
            }
        }

        return values;
    }
}
