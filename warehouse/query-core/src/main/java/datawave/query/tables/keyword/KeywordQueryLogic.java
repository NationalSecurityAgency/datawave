package datawave.query.tables.keyword;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.CheckpointableQueryLogic;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl.Parameter;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.config.KeywordQueryConfiguration;
import datawave.query.iterator.logic.KeywordExtractingIterator;
import datawave.query.tables.ScannerFactory;
import datawave.query.transformer.KeywordQueryTransformer;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;

/**
 * This query table implementation returns a QueryResults object that contains keywords extracted from documents from the Shard table. The query will contain
 * the shard id, datatype, and UID of each desired event so that we can seek directly to its respective document. Each document is stored as base64 compressed
 * binary in the Accumulo table. We will decompress the data and run the keyword extraction algorithm on it. Results are returned in JSON format for now.
 * <p>
 * The query that needs to be passed to the web service is:
 * </p>
 *
 * <pre>
 *     DOCUMENT:shardId/datatype/uid [DOCUMENT:shardId/datatype/uid]*
 * </pre>
 * <p>
 * Optionally, additional metadata can be provided to indicate useful information about the document in question (that will be included in the response).
 * </p>
 *
 * <pre>
 *     DOCUMENT:shardId/datatype/uid!ID_FIELD:ID_VALUE%LANGUAGE:ENGLISH
 * </pre>
 *
 * The optional parameter content.view.name can be used to retrieve an alternate view of the document, assuming one is stored with that name. The optional
 * parameter content.view.all can be used to retrieve all documents for the parent and children Both optional parameters can be used together
 */
public class KeywordQueryLogic extends BaseQueryLogic<Entry<Key,Value>> implements CheckpointableQueryLogic {

    private static final Logger log = Logger.getLogger(KeywordQueryLogic.class);

    /**
     * Used to pull back specific views of the data - expected to be a comma-delimited list of one or more views, if present.
     */
    public static final String CONTENT_VIEW_NAMES = "content.view.names";

    /**
     * Used to embed document language in query term
     */
    public static final String LANGUAGE_TOKEN = "%LANGUAGE:";

    private static final String PARENT_ONLY = "\1";
    private static final String ALL = "\u10FFFF";

    private int queryThreads = 100;

    ScannerFactory scannerFactory;

    private KeywordQueryConfiguration config;

    public KeywordQueryLogic() {
        super();
    }

    public KeywordQueryLogic(final KeywordQueryLogic keywordQueryLogic) {
        super(keywordQueryLogic);
        this.queryThreads = keywordQueryLogic.queryThreads;
        this.scannerFactory = keywordQueryLogic.scannerFactory;
        this.config = new KeywordQueryConfiguration(keywordQueryLogic.config);
    }

    /**
     * This method calls the base logic's close method, and then attempts to close all batch scanners tracked by the scanner factory, if it is not null.
     */
    @Override
    public void close() {
        super.close();
        final ScannerFactory factory = this.scannerFactory;
        if (null == factory) {
            log.debug("ScannerFactory is null; not closing it.");
        } else {
            int nClosed = 0;
            factory.lockdown();
            for (final ScannerBase bs : factory.currentScanners()) {
                factory.close(bs);
                ++nClosed;
            }
            if (log.isDebugEnabled())
                log.debug("Cleaned up " + nClosed + " batch scanners associated with this query logic.");
        }
    }

    @Override
    public GenericQueryConfiguration initialize(final AccumuloClient client, final Query settings, final Set<Authorizations> auths) throws Exception {
        // Initialize the config and scanner factory
        // NOTE: This needs to set the class-level config object. Do not use a local instance!
        config = new KeywordQueryConfiguration(this, settings);
        config.setClient(client);
        config.setAuthorizations(auths);
        config.setState(new KeywordQueryState());

        // Get the BYPASS_ACCUMULO parameter if given
        String bypassAccumuloString = settings.findParameter(BYPASS_ACCUMULO).getParameterValue().trim();
        if (StringUtils.isNotBlank(bypassAccumuloString)) {
            boolean bypassAccumuloBool = Boolean.parseBoolean(bypassAccumuloString);
            config.setBypassAccumulo(bypassAccumuloBool);
        }

        this.scannerFactory = new ScannerFactory(config);

        // Configure the view names.
        Parameter p = settings.findParameter(CONTENT_VIEW_NAMES);
        if (null != p && !StringUtils.isEmpty(p.getParameterValue())) {
            config.getState().getViewNames().add(p.getParameterValue());
        }

        // Determine whether we include the content of child events
        String end;
        p = settings.findParameter(QueryParameters.CONTENT_VIEW_ALL);
        if ((null != p) && (null != p.getParameterValue()) && StringUtils.isNotBlank(p.getParameterValue())) {
            end = ALL;
        } else {
            end = PARENT_ONLY;
        }

        // Extract the list of individual documents we're looking for.
        final Collection<String> queryTerms = extractQueryTerms(settings);

        // Configure the documentLanguageMap based on the data included for each document
        final Map<String,String> documentLanguageMap = createDocumentLanguageMap(queryTerms);
        config.getState().getDocumentLanguageMap().putAll(documentLanguageMap);

        // Configure ranges for finding content.
        final Collection<Range> ranges = createRanges(queryTerms, end);
        config.getState().setRanges(ranges);

        return config;
    }

    public void setQueryThreads(int queryThreads) {
        this.queryThreads = queryThreads;
    }

    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        if (!(genericConfig instanceof KeywordQueryConfiguration)) {
            throw new QueryException("Did not receive a KeywordQueryConfiguration instance!!");
        }

        this.config = (KeywordQueryConfiguration) genericConfig;

        try {
            final BatchScanner scanner = this.scannerFactory.newScanner(config.getTableName(), config.getAuthorizations(), this.queryThreads,
                            config.getQuery());
            scanner.setRanges(config.getState().getRanges());

            final IteratorSetting cfg = new IteratorSetting(60, "keyword-extractor", KeywordExtractingIterator.class);
            KeywordExtractingIterator.setOptions(cfg, config.getMinNgrams(), config.getMaxNgrams(), config.getMaxKeywords(), config.getMaxScore(),
                            config.getMaxContentChars(), config.getState().getViewNames(), config.getState().getDocumentLanguageMap(), false);
            scanner.addScanIterator(cfg);

            this.iterator = scanner.iterator();
            this.scanner = scanner;

        } catch (TableNotFoundException e) {
            throw new RuntimeException("Table not found: " + this.getTableName(), e);
        }
    }

    /**
     * Extract the individual query terms from the input query.
     *
     * @param settings
     *            the query to process
     * @return a collection of terms that
     */
    public static Collection<String> extractQueryTerms(final Query settings) {
        final String query = settings.getQuery().trim();
        //@formatter:off
        return Arrays.stream(query.split("\\s+"))
                .filter(s -> !s.isBlank())
                .map(String::trim)
                .collect(Collectors.toSet());
        //@formatter:on
    }

    private static Map<String,String> createDocumentLanguageMap(Collection<String> queryTerms) {
        Map<String,String> documentLanguageMap = new HashMap<>();

        for (String term : queryTerms) {
            int pos;
            if ((pos = term.indexOf(LANGUAGE_TOKEN)) > 0) {
                String language = term.substring(pos + LANGUAGE_TOKEN.length());
                String cleanTerm = term.substring(0, pos);
                String[] parts = extractUIDParts(cleanTerm);
                documentLanguageMap.put(parts[1] + "/" + parts[2], language);
            }
        }

        return documentLanguageMap;
    }

    /**
     * Create an ordered collection of Ranges for scanning
     *
     * @param queryTerms
     *            individual query terms.
     *
     * @param endKeyTerminator
     *            a string appended to each Range's end key indicating whether to include child content
     *
     * @return one or more Ranges
     */
    private static Collection<Range> createRanges(Collection<String> queryTerms, String endKeyTerminator) {
        // Initialize the returned collection of ordered ranges
        final Set<Range> ranges = new TreeSet<>();

        for (String term : queryTerms) {
            final String[] parts = extractUIDParts(term);
            final Range r = getRangeFromTermParts(parts, endKeyTerminator);
            ranges.add(r);
            log.debug("Adding range: " + r);
        }

        if (ranges.isEmpty()) {
            throw new IllegalArgumentException(
                            "At least one term required of the form " + "'DOCUMENT:shardId/datatype/eventUID', but none were: " + queryTerms);
        }

        return ranges;
    }

    /**
     * Parse a term into parts that will be used for setting up scan ranges. Expecting something like: DOCUMENT:shard/datatype/uid!optionalthings
     * <p>
     * The leading 'DOCUMENT:' and '!optionalthings' are not required to be present.
     * </p>
     *
     * @param term
     *            the term to parse
     * @return the term parsed into parts: shard, datatype, UID
     * @throws IllegalArgumentException
     *             if there are less than 3 parts to the UID identifier.
     */
    public static String[] extractUIDParts(String term) {
        // Remove the field if present.
        final int fieldSeparation = term.indexOf(':');
        final String valueIdentifier = fieldSeparation > 0 ? term.substring(fieldSeparation + 1) : term;

        // Remove the identifier if present - they are used later in the KeywordQueryTransformer
        final int idSeparation = valueIdentifier.indexOf("!");
        final String value = idSeparation > 0 ? valueIdentifier.substring(0, idSeparation) : valueIdentifier;

        // Validate number of expected parts
        String[] parts = value.split("/");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Query term does not specify all needed parts: " + term
                            + ". Each space-delimited term should be of the form 'DOCUMENT:shardId/datatype/eventUID'.");
        }
        return parts;
    }

    /**
     * Parse the parts extracted from a query term into ranges.
     *
     * @param parts
     *            the term we're parsing
     * @param endKeyTerminator
     *            the terminator for the end key of the range
     * @return the range determined from the input term or null if there was a problem parsing the term.
     */
    private static Range getRangeFromTermParts(String[] parts, String endKeyTerminator) {

        // Get the info necessary to build a content Range
        final String shardId = parts[0];
        final String datatype = parts[1];
        final String uid = parts[2];

        log.debug("Received pieces: " + shardId + ", " + datatype + ", " + uid);

        // Create and add a Range
        final String cf = ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY;
        final String cq = datatype + Constants.NULL_BYTE_STRING + uid;
        final Key startKey = new Key(shardId, cf, cq + Constants.NULL_BYTE_STRING);
        final Key endKey = new Key(shardId, cf, cq + endKeyTerminator);
        return new Range(startKey, true, endKey, false);
    }

    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public QueryLogicTransformer<Entry<Key,Value>,EventBase> getTransformer(Query settings) {
        return new KeywordQueryTransformer(settings, this.markingFunctions, this.responseObjectFactory);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new KeywordQueryLogic(this);
    }

    public int getQueryThreads() {
        return this.queryThreads;
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.add(QueryParameters.CONTENT_VIEW_NAME);
        params.add(QueryParameters.CONTENT_VIEW_ALL);
        return params;
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }

    @Override
    public KeywordQueryConfiguration getConfig() {
        if (this.config == null) {
            this.config = KeywordQueryConfiguration.create();
        }
        return this.config;
    }

    @Override
    public boolean isCheckpointable() {
        return getConfig().isCheckpointable();
    }

    @Override
    public void setCheckpointable(boolean checkpointable) {
        getConfig().setCheckpointable(checkpointable);
    }

    @Override
    public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
        if (!isCheckpointable()) {
            throw new UnsupportedOperationException("Cannot checkpoint a query that is not checkpointable.  Try calling setCheckpointable(true) first.");
        }

        // if we have started returning results, then capture the state of the query data objects
        if (this.iterator != null) {
            List<QueryCheckpoint> checkpoints = Lists.newLinkedList();
            for (Range range : getConfig().getState().getRanges()) {
                checkpoints.add(new KeywordQueryCheckpoint(queryKey, Collections.singletonList(range)));
            }
            return checkpoints;
        }
        // otherwise we still need to plan or there are no results
        else {
            return Lists.newArrayList(new QueryCheckpoint(queryKey));
        }
    }

    @Override
    public QueryCheckpoint updateCheckpoint(QueryCheckpoint checkpoint) {
        // for the content query logic, the query data objects automatically get updated with
        // the last result returned, so the checkpoint should already be updated!
        return checkpoint;
    }

    @Override
    public void setupQuery(AccumuloClient client, GenericQueryConfiguration config, QueryCheckpoint checkpoint) throws Exception {
        KeywordQueryConfiguration contentQueryConfig = (KeywordQueryConfiguration) config;
        contentQueryConfig.getState().setRanges(((KeywordQueryCheckpoint) checkpoint).getRanges());
        contentQueryConfig.setClient(client);

        scannerFactory = new ScannerFactory(client);

        setupQuery(contentQueryConfig);
    }
}
