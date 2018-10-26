package datawave.ingest.mapreduce.handler.tokenize;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.data.config.ingest.TermFrequencyIngestHelperInterface;
import datawave.ingest.data.tokenize.DefaultTokenSearch;
import datawave.ingest.data.tokenize.TokenSearch;
import datawave.ingest.data.tokenize.TokenizationHelper;
import datawave.ingest.data.tokenize.TokenizationHelper.HeartBeatThread;
import datawave.ingest.data.tokenize.TokenizationHelper.TokenizerTimeoutException;
import datawave.ingest.data.tokenize.TruncateAttribute;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.content.BoundedOffsetQueue;
import datawave.ingest.mapreduce.handler.shard.content.BoundedOffsetQueue.OffsetList;
import datawave.ingest.mapreduce.handler.shard.content.ContentIndexCounters;
import datawave.ingest.mapreduce.handler.shard.content.OffsetQueue;
import datawave.ingest.mapreduce.handler.shard.content.TermAndZone;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.util.BloomFilterUtil;
import datawave.ingest.util.BloomFilterWrapper;
import datawave.ingest.util.Identity;
import datawave.ingest.util.TimeoutStrategy;
import datawave.util.TextUtil;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Content indexing column based handler. will provide content tokenization, which will include storing offsets, and the TERM_COUNT for that event.
 * 
 * @param <KEYIN>
 */
public abstract class ContentIndexingColumnBasedHandler<KEYIN> extends AbstractColumnBasedHandler<KEYIN> implements TermFrequencyIngestHelperInterface {
    
    private static final Logger log = Logger.getLogger(ContentIndexingColumnBasedHandler.class);
    
    public abstract AbstractContentIngestHelper getContentIndexingDataTypeHelper();
    
    // helper
    protected AbstractContentIngestHelper contentHelper;
    
    protected TokenizationHelper tokenHelper;
    
    // token field designator - the suffix added to fields that contain tokens
    // that are generated from other fields.
    protected String tokenFieldNameSuffix = "";
    
    /**
     * portions of the generic event.
     */
    protected byte[] shardId;
    
    protected String eventDataTypeName;
    
    protected String eventUid;
    
    protected String tokenRegex;
    
    protected OffsetQueue<Integer> tokenOffsetCache = null;
    
    protected Identity hasher = new Identity();
    
    protected Configuration conf;
    
    private BloomFilterUtil bloomFilterUtil;
    
    private TokenSearch searchUtil;
    
    private TokenSearch searchUtilReverse;
    
    protected ContentIndexCounters counters = null;
    
    private Set<String> termTypeBlacklist = Collections.emptySet();
    
    private boolean tokenizerTimeWarned = false;
    
    private int termPosition = 0;
    
    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
        contentHelper = getContentIndexingDataTypeHelper();
        tokenFieldNameSuffix = contentHelper.getTokenFieldNameDesignator();
        Preconditions.checkNotNull(tokenFieldNameSuffix);
        
        counters = new ContentIndexCounters();
        
        conf = context.getConfiguration();
        contentHelper.setup(conf);
        helper = contentHelper;
        
        tokenHelper = new TokenizationHelper(helper, conf);
        
        // TODO: refactor explicit DefaultTokenSearch usage here and get class from config
        searchUtil = TokenSearch.Factory.newInstance(DefaultTokenSearch.class.getCanonicalName(), tokenHelper.getStopWords(), false);
        tokenHelper.configureSearchUtil(searchUtil);
        
        // TODO: refactor explicit DefaultTokenSearch usage here and get class from config
        searchUtilReverse = TokenSearch.Factory.newInstance(DefaultTokenSearch.class.getCanonicalName(), tokenHelper.getStopWords(), true);
        tokenHelper.configureSearchUtil(searchUtilReverse);
        
        tokenOffsetCache = new BoundedOffsetQueue<>(tokenHelper.getTokenOffsetCacheMaxSize());
        
        // Conditionally create an NGrams factory
        if (this.getBloomFiltersEnabled()) {
            this.bloomFilterUtil = newBloomFilterUtil(this.conf);
        }
    }
    
    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields,
                    StatusReporter reporter) {
        
        if (event.fatalError()) {
            return null;
        }
        
        this.shardId = getShardId(event);
        this.eventDataTypeName = event.getDataType().outputName();
        this.eventUid = event.getId().toString();
        
        Multimap<BulkIngestKey,Value> values = HashMultimap.create();
        // get the typical shard/index information
        values.putAll(super.processBulk(key, event, eventFields, reporter));
        
        // now flush out the offset queue
        if (tokenOffsetCache != null) {
            int termCount = 0;
            try {
                for (OffsetList<Integer> offsets : tokenOffsetCache.offsets()) {
                    // no need to normalize as that was already done
                    // upon insertion into the token offset cache
                    NormalizedFieldAndValue nfv = new NormalizedFieldAndValue(offsets.termAndZone.zone, offsets.termAndZone.term);
                    
                    byte[] fieldVisibility = getVisibility(event, nfv);
                    
                    createTermFrequencyIndex(event, values, this.shardId, nfv, offsets.offsets, fieldVisibility);
                    termCount++;
                }
                
                if (termCount > 0) {
                    Multimap<String,NormalizedContentInterface> tokenMap = HashMultimap.create();
                    NormalizedFieldAndValue nfav = new NormalizedFieldAndValue();
                    nfav.setEventFieldName("TERM_COUNT");
                    nfav.setEventFieldValue(Long.toString(termCount));
                    tokenMap.put(nfav.getEventFieldName(), nfav);
                    byte[] fieldVisibility = getVisibility(event, nfav);
                    createEventColumn(event, tokenMap, values, this.shardId, fieldVisibility, nfav);
                }
            } catch (IOException ex) {
                log.fatal("IOException", ex);
            } catch (InterruptedException ex) {
                log.warn("Interrupted!", ex);
                Thread.interrupted();
            }
            
            tokenOffsetCache.clear();
        }
        
        counters.flush(reporter);
        
        return values;
    }
    
    /**
     * Creates and writes the BulkIngestKey for the event's field/value to the ContextWriter (instead of the Multimap that the {@link ShardedDataTypeHandler}
     * uses).
     *
     * @param event
     * @param eventFields
     * @param values
     * @param fieldVisibility
     * @param shardId
     * @param fieldVisibility
     * @param nFV
     * @throws IOException
     * @throws InterruptedException
     */
    protected void createEventColumn(RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields, Multimap<BulkIngestKey,Value> values,
                    byte[] shardId, byte[] fieldVisibility, NormalizedContentInterface nFV) throws IOException, InterruptedException {
        
        String fieldName = nFV.getEventFieldName();
        String fieldValue = nFV.getEventFieldValue();
        
        if (StringUtils.isEmpty(fieldValue))
            return;
        
        Text colf = new Text(event.getDataType().outputName());
        TextUtil.textAppend(colf, event.getId().toString(), helper.getReplaceMalformedUTF8());
        
        Text colq = new Text(fieldName);
        TextUtil.textAppend(colq, fieldValue, helper.getReplaceMalformedUTF8());
        Key k = createKey(shardId, colf, colq, fieldVisibility, event.getDate(), helper.getDeleteMode());
        BulkIngestKey bKey = new BulkIngestKey(new Text(this.getShardTableName()), k);
        values.put(bKey, NULL_VALUE);
    }
    
    @Override
    protected Multimap<String,NormalizedContentInterface> getShardNamesAndValues(RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> eventFields, boolean createGlobalIndexTerms, boolean createGlobalReverseIndexTerms,
                    StatusReporter reporter) {
        
        // Reset state.
        fields = HashMultimap.create();
        index = HashMultimap.create();
        reverse = HashMultimap.create();
        
        Analyzer analyzer = tokenHelper.getAnalyzer();
        
        try {
            String lastFieldName = "";
            
            for (Entry<String,NormalizedContentInterface> e : eventFields.entries()) {
                NormalizedContentInterface nci = e.getValue();
                
                // Put the normalized field name and normalized value into the index
                if (createGlobalIndexTerms) {
                    if (helper.isIndexedField(nci.getIndexedFieldName())) {
                        index.put(nci.getIndexedFieldName(), nci);
                    }
                }
                
                // Put the normalized field name and normalized value into the reverse
                if (createGlobalReverseIndexTerms) {
                    if (helper.isReverseIndexedField(nci.getIndexedFieldName())) {
                        NormalizedContentInterface rField = (NormalizedContentInterface) (nci.clone());
                        rField.setEventFieldValue(new StringBuilder(rField.getEventFieldValue()).reverse().toString());
                        rField.setIndexedFieldValue(new StringBuilder(rField.getIndexedFieldValue()).reverse().toString());
                        reverse.put(nci.getIndexedFieldName(), rField);
                    }
                }
                
                // Skip any fields that should not be included in the shard table.
                if (helper.isShardExcluded(nci.getIndexedFieldName())) {
                    continue;
                }
                
                // Put the event field name and original value into the fields
                fields.put(nci.getIndexedFieldName(), nci);
                
                String indexedFieldName = nci.getIndexedFieldName();
                
                // reset term position to zero if the indexed field name has changed, otherwise
                // bump the offset based on the inter-field position increment.
                if (!lastFieldName.equals(indexedFieldName)) {
                    termPosition = 0;
                    lastFieldName = indexedFieldName;
                } else {
                    termPosition = tokenHelper.getInterFieldPositionIncrement();
                }
                
                boolean indexField = contentHelper.isContentIndexField(indexedFieldName);
                boolean reverseIndexField = contentHelper.isReverseContentIndexField(indexedFieldName);
                
                if ((createGlobalIndexTerms && indexField) || (createGlobalReverseIndexTerms && reverseIndexField)) {
                    try {
                        if (isTokenizationBySubtypeEnabled()) {
                            if (determineTokenizationBySubtype(nci.getIndexedFieldName())) {
                                tokenizeField(analyzer, nci, indexField, reverseIndexField, reporter);
                            }
                        } else {
                            tokenizeField(analyzer, nci, indexField, reverseIndexField, reporter);
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        } finally {
            analyzer.close();
        }
        
        validateIndexedFields(createGlobalIndexTerms, createGlobalReverseIndexTerms, reporter);
        
        return fields;
    }
    
    protected boolean isTokenizationBySubtypeEnabled() {
        return false;
    }
    
    protected boolean determineTokenizationBySubtype(String field) {
        return false;
    }
    
    public boolean isTokenizerTimeWarned() {
        return tokenizerTimeWarned;
    }
    
    /**
     * Tokenize the specified field using the analyzer provided.
     * 
     */
    protected void tokenizeField(final Analyzer a, final NormalizedContentInterface nci, boolean indexField, boolean reverseIndexField, StatusReporter reporter)
                    throws IOException, InterruptedException {
        
        if (!(indexField || reverseIndexField)) {
            return;
        }
        
        String indexedFieldName = nci.getIndexedFieldName();
        String modifiedFieldName = indexedFieldName + tokenFieldNameSuffix;
        String content = nci.getIndexedFieldValue();
        
        TokenStream tokenizer = a.tokenStream(indexedFieldName, new StringReader(content));
        tokenizer.reset();
        
        try {
            final CharTermAttribute termAtt = tokenizer.getAttribute(CharTermAttribute.class);
            final TypeAttribute typeAtt = tokenizer.getAttribute(TypeAttribute.class);
            final PositionIncrementAttribute posIncrAtt = tokenizer.getAttribute(PositionIncrementAttribute.class);
            final TruncateAttribute truncAtt = tokenizer.getAttribute(TruncateAttribute.class);
            
            // Track amount of time we've spent tokenizing this document,
            // at the least we will use this for metrics, we could also use
            // this to halt indexing if we exceed a certain threshold
            int heartBeatCount = HeartBeatThread.counter;
            int tokenizerBeats = 0;
            long start = System.currentTimeMillis();
            
            tokenizerTimeWarned = false;
            
            while (true) {
                if (heartBeatCount != HeartBeatThread.counter) {
                    tokenizerBeats += HeartBeatThread.counter - heartBeatCount;
                    heartBeatCount = HeartBeatThread.counter;
                    
                    // warn once on exceeding the warn threshold
                    long elapsedEstimateMsec = tokenizerBeats * HeartBeatThread.INTERVAL;
                    if (elapsedEstimateMsec > tokenHelper.getTokenizerTimeWarnThresholdMsec() && !tokenizerTimeWarned) {
                        long realDelta = System.currentTimeMillis() - start;
                        counters.incrementValue(ContentIndexCounters.TOKENIZER_TIME_WARNINGS, 1, reporter);
                        log.warn("Tokenization of field " + modifiedFieldName + " has exceeded warning threshold "
                                        + tokenHelper.getTokenizerTimeWarnThresholdMsec() + "ms (" + realDelta + "ms)");
                        tokenizerTimeWarned = true;
                    }
                    
                    // error when we exceed the error threshold
                    if (elapsedEstimateMsec > tokenHelper.getTokenizerTimeErrorThresholdMsec()) {
                        long realDelta = System.currentTimeMillis() - start;
                        counters.incrementValue(ContentIndexCounters.TOKENIZER_TIME_ERRORS, 1, reporter);
                        throw new TokenizerTimeoutException("Tokenization of field " + modifiedFieldName + " has exceeded error threshold "
                                        + tokenHelper.getTokenizerTimeErrorThresholdMsec() + "ms (" + realDelta + "ms), aborting");
                    }
                }
                
                // getting the next token can take a long time depending on the compexity of the data...
                // so lets report progress to hadoop on each round
                if (reporter != null)
                    reporter.progress();
                
                if (!tokenizer.incrementToken()) {
                    break; // eof
                }
                
                // Get the term and any synonyms for it
                String token = termAtt.toString();
                String type = typeAtt.type();
                
                // term positions aren't reset between fields of the same name, see getShardNamesAndValues.
                termPosition += posIncrAtt.getPositionIncrement();
                
                if (type.startsWith("<") && type.endsWith(">")) {
                    type = type.substring(1, type.length() - 1); // <FOO> => FOO without regex
                }
                
                // Make sure the term length is greater than the minimum allowed length
                int tlen = token.length();
                if (tlen < tokenHelper.getTermLengthMinimum()) {
                    log.debug("Ignoring token of length " + token.length() + " because it is too short");
                    counters.increment(ContentIndexCounters.TOO_SHORT_COUNTER, reporter);
                    continue;
                }
                
                // skip the term if it is over the length limit unless it is a FILE, URL or HTTP_REQUEST
                if (tlen > tokenHelper.getTermLengthLimit() && (!(type.equals("FILE") || type.equals("URL") || type.equals("HTTP_REQUEST")))) {
                    if (log.isDebugEnabled()) {
                        log.debug("Ignoring " + type + " token due to excessive length");
                    }
                    
                    counters.increment(ContentIndexCounters.EXCESSIVE_LENGTH_COUNTER, reporter);
                    continue;
                }
                
                if (tlen > tokenHelper.getTermLengthWarningLimit()) {
                    log.warn("Encountered long term: " + tlen + " characters, '" + token + "'");
                    counters.increment(ContentIndexCounters.LENGTH_WARNING_COUNTER, reporter);
                }
                
                if (truncAtt.isTruncated()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Encountered truncated term: " + tlen + " characters, '" + token + "'");
                    }
                    counters.increment(ContentIndexCounters.TRUNCATION_COUNTER, reporter);
                }
                
                if (tokenHelper.isVerboseTermSizeCounters()) {
                    if (tlen < 10) {
                        counters.increment(ContentIndexCounters.TERM_SIZE_GROUP_NAME, "SIZE_00" + tlen, reporter);
                    } else if (tlen < 100) {
                        counters.increment(ContentIndexCounters.TERM_SIZE_GROUP_NAME, "SIZE_0" + ((tlen / 10) * 10), reporter);
                    } else {
                        counters.increment(ContentIndexCounters.TERM_SIZE_GROUP_NAME, "SIZE_100", reporter);
                    }
                    
                    counters.increment(ContentIndexCounters.TERM_TYPE_GROUP_NAME, type + "_TERMS", reporter);
                }
                
                // Track the number of tokens processed
                counters.increment(ContentIndexCounters.ORIGINAL_PROCESSED_COUNTER, reporter);
                
                if (termTypeBlacklist.contains(type)) {
                    counters.increment(ContentIndexCounters.TERM_TYPE_GROUP_NAME, "BLACKLISTED_BY_TYPE", reporter);
                    continue;
                }
                
                if (indexField) {
                    NormalizedContentInterface newField;
                    Collection<String> synonyms;
                    
                    if (tokenHelper.isSynonymGenerationEnabled()) {
                        // Get the list of synonyms including the term itself
                        // Zone is empty in this case.
                        synonyms = searchUtil.getSynonyms(new String[] {token, ""}, typeAtt.type(), true);
                    } else {
                        synonyms = Collections.singletonList(token);
                    }
                    
                    for (String s : synonyms) {
                        newField = (NormalizedContentInterface) (nci.clone());
                        newField.setFieldName(modifiedFieldName);
                        // don't put tokens in the event.
                        newField.setEventFieldValue(null);
                        newField.setIndexedFieldValue(s);
                        index.put(modifiedFieldName, newField);
                        
                        // add this token to the event fields so a
                        // local fi\x00 key gets created
                        // NOTE: we already assigned it to the
                        // 'indexOnly' list so it won't show up in
                        // the event
                        fields.put(modifiedFieldName, newField);
                        
                        if (tokenOffsetCache != null) {
                            tokenOffsetCache.addOffset(new TermAndZone(s, modifiedFieldName), termPosition);
                        }
                    }
                    
                    counters.incrementValue(ContentIndexCounters.SYNONYMS_PROCESSED_COUNTER, synonyms.size() - 1, reporter);
                    if (tokenHelper.isVerboseTermIndexCounters()) {
                        counters.incrementValue(ContentIndexCounters.SYNONYM_TYPE_GROUP_NAME, type + ContentIndexCounters.SYNONYMS_PROCESSED_TYPE_SUFFIX,
                                        synonyms.size() - 1, reporter);
                    }
                }
                
                if (reverseIndexField) {
                    String rToken = StringUtils.reverse(token);
                    NormalizedContentInterface newField;
                    Collection<String> synonyms;
                    
                    if (tokenHelper.isSynonymGenerationEnabled()) {
                        synonyms = searchUtilReverse.getSynonyms(rToken, typeAtt.type(), true);
                    } else {
                        synonyms = Collections.singletonList(rToken);
                    }
                    
                    for (String s : synonyms) {
                        newField = (NormalizedContentInterface) (nci.clone());
                        newField.setFieldName(modifiedFieldName);
                        newField.setEventFieldValue(s);
                        newField.setIndexedFieldValue(s);
                        reverse.put(modifiedFieldName, newField);
                        
                        // NOTE: We don't want fi\x00 keys for reverse
                        // tokens
                    }
                    
                    counters.incrementValue(ContentIndexCounters.SYNONYMS_PROCESSED_COUNTER, synonyms.size() - 1, reporter);
                    if (tokenHelper.isVerboseTermIndexCounters()) {
                        counters.incrementValue(ContentIndexCounters.SYNONYM_TYPE_GROUP_NAME, type + ContentIndexCounters.SYNONYMS_PROCESSED_TYPE_SUFFIX,
                                        synonyms.size() - 1, reporter);
                    }
                }
            }
            
            final long tokenizerDeltaMsec = tokenizerBeats * HeartBeatThread.INTERVAL;
            final long[] tokenizerThresholds = tokenHelper.getTokenizerTimeThresholds();
            final String[] tokenizerThresholdNames = tokenHelper.getTokenizerTimeThresholdNames();
            boolean counted = false;
            for (int i = 0; i < tokenizerThresholds.length; i++) {
                if (tokenizerDeltaMsec < tokenizerThresholds[i]) {
                    counters.incrementValue(ContentIndexCounters.TOKENIZER_TIME_GROUP_NAME, ContentIndexCounters.TOKENIZER_TIME_PREFIX + "<"
                                    + tokenizerThresholdNames[i], 1, reporter);
                    counted = true;
                    break;
                }
            }
            
            // catch times outside of the max threshold if we're counting
            if (!counted && tokenizerThresholdNames.length > 0) {
                counters.incrementValue(ContentIndexCounters.TOKENIZER_TIME_GROUP_NAME, ContentIndexCounters.TOKENIZER_TIME_PREFIX + ">="
                                + tokenizerThresholdNames[tokenizerThresholdNames.length - 1], 1, reporter);
            }
        } finally {
            tokenizer.close();
        }
    }
    
    /**
     * Creates a Term Frequency index key in the "tf" column family.
     * 
     * @param event
     * @param values
     * @param shardId
     * @param nfv
     * @param offsets
     * @param visibility
     * @throws IOException
     * @throws InterruptedException
     */
    protected void createTermFrequencyIndex(RawRecordContainer event, Multimap<BulkIngestKey,Value> values, byte[] shardId, NormalizedFieldAndValue nfv,
                    List<Integer> offsets, byte[] visibility) throws IOException, InterruptedException {
        
        TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
        for (Integer offset : offsets) {
            builder.addTermOffset(offset);
        }
        Value value = new Value(builder.build().toByteArray());
        
        StringBuilder colq = new StringBuilder(this.eventDataTypeName.length() + this.eventUid.length() + nfv.getIndexedFieldName().length()
                        + nfv.getIndexedFieldValue().length() + 3);
        colq.append(this.eventDataTypeName).append('\u0000').append(this.eventUid).append('\u0000').append(nfv.getIndexedFieldValue()).append('\u0000')
                        .append(nfv.getIndexedFieldName());
        
        BulkIngestKey bKey = new BulkIngestKey(new Text(this.getShardTableName()), new Key(shardId,
                        ExtendedDataTypeHandler.TERM_FREQUENCY_COLUMN_FAMILY.getBytes(), colq.toString().getBytes(), visibility, event.getDate(),
                        helper.getDeleteMode()));
        
        values.put(bKey, value);
    }
    
    /**
     * overridable acceptance test for whether or not messages can/should be tokenized
     * 
     * @param message
     * @return
     */
    protected boolean isTokenizable(String message) {
        return true;
    }
    
    public boolean isTermFrequencyField(String field) {
        if (tokenFieldNameSuffix.length() > 0 && field.endsWith(tokenFieldNameSuffix)) {
            field = field.substring(0, (field.length() - tokenFieldNameSuffix.length()));
        }
        return contentHelper.isContentIndexField(field) || contentHelper.isReverseContentIndexField(field);
    }
    
    @Override
    protected BloomFilterWrapper createBloomFilter(final Multimap<String,NormalizedContentInterface> fields) {
        // Declare and create a bloom filter. If bloom filtering is enabled, an NGramsFactory
        // should have been created during setup. Otherwise, let the parent create it.
        final BloomFilterWrapper result;
        if (null != this.bloomFilterUtil) {
            result = this.bloomFilterUtil.newNGramBasedFilter(fields);
        } else {
            result = super.createBloomFilter(fields);
        }
        
        return result;
    }
    
    /**
     * Create a new factory instance based on a specialized content-indexing, column-based {@link ShardedDataTypeHandler}.
     *
     * @param configuration
     *            the Hadoop job configuration
     * @return a non-null factory instance
     *
     * @see ShardedDataTypeHandler
     * @see ContentIndexingColumnBasedHandler
     */
    protected BloomFilterUtil newBloomFilterUtil(final Configuration configuration) {
        // Conditionally create an NGrams factory
        final BloomFilterUtil util;
        final AbstractContentIngestHelper helper;
        if ((null != (helper = getContentIndexingDataTypeHelper()))) {
            float diskThreshold = getBloomFilteringDiskThreshold();
            final String diskThresholdPath = getBloomFilteringDiskThresholdPath();
            float memoryThreshold = getBloomFilteringMemoryThreshold();
            int maxFilterSize = getBloomFilteringOptimumMaxFilterSize();
            int timeoutMillis = -1;
            if (null != configuration) {
                float taskTimeout = configuration.getFloat(TimeoutStrategy.MAPRED_TASK_TIMEOUT, -1);
                if (taskTimeout > 0) {
                    float timeoutThreshold = getBloomFilteringTimeoutThreshold();
                    if ((timeoutThreshold > 0) && (timeoutThreshold <= 1)) {
                        timeoutMillis = Math.round(((1.0f - timeoutThreshold) * taskTimeout));
                    }
                }
            }
            
            util = BloomFilterUtil.newInstance(helper, memoryThreshold, diskThreshold, diskThresholdPath, timeoutMillis);
            util.setOptimumFilterSize(maxFilterSize);
        }
        // This should not happen, so log it
        else {
            util = null;
            
            final String message = "Unable to create factory for N-grams. ContentIngestHelperInterface is null.";
            ;
            Logger.getLogger(BloomFilterUtil.class).warn(message, new IllegalStateException());
        }
        
        return util;
    }
    
}
