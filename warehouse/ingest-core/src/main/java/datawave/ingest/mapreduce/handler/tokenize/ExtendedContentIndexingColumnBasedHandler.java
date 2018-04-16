package datawave.ingest.mapreduce.handler.tokenize;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.data.tokenize.DefaultTokenSearch;
import datawave.ingest.data.tokenize.TokenSearch;
import datawave.ingest.data.tokenize.TokenizationHelper;
import datawave.ingest.mapreduce.ContextWrappedStatusReporter;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.content.BoundedOffsetQueue;
import datawave.ingest.mapreduce.handler.shard.content.BoundedOffsetQueue.OffsetList;
import datawave.ingest.mapreduce.handler.shard.content.ContentIndexCounters;
import datawave.ingest.mapreduce.handler.shard.content.OffsetQueue;
import datawave.ingest.mapreduce.handler.shard.content.TermAndZone;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.Uid;
import datawave.util.TextUtil;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.util.CharArraySet;
import org.infinispan.commons.util.Base64;

import com.google.common.collect.Multimap;

/**
 * <p>
 * Calling the process method on this DataTypeHandler creates fields representing the tokenized content for text indexing. The process method also calls the
 * processBulk method on the {@link ShardedDataTypeHandler} to create the expected fields for the current {@link RawRecordContainer} object.
 * </p>
 * 
 * <p>
 * This class creates the following Mutations or Key/Values in addition to those created by the {@link ShardedDataTypeHandler}: <br>
 * <br>
 * <table border="1" summary="">
 * <tr>
 * <th>Schema Type</th>
 * <th>Use</th>
 * <th>Row</th>
 * <th>Column Family</th>
 * <th>Column Qualifier</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>Shard</td>
 * <td>Event Data</td>
 * <td>ShardId</td>
 * <td>DataType\0UID</td>
 * <td>TERM_ZONE\0TERM</td>
 * <td>NULL</td>
 * </tr>
 * <tr>
 * <td>Shard</td>
 * <td>Document content</td>
 * <td>ShardId</td>
 * <td>#ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY</td>
 * <td>DataType\0UID</td>
 * <td>Base64 encoded, GZIPed document</td>
 * </tr>
 * </table>
 *
 * <p>
 * The document is not placed into the RawRecordContainer object with the rest of the fields in an attempt to prevent any slow-downs when scanning over the
 * RawRecordContainer objects. Placing them into their own column family also allows a locality group to be set so that they will all be located within the same
 * RFiles and not add additional bloat to the RFiles containing the rest of the shard table.
 *
 * @param <KEYIN>
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public abstract class ExtendedContentIndexingColumnBasedHandler<KEYIN,KEYOUT,VALUEOUT> extends AbstractColumnBasedHandler<KEYIN> implements
                ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> {
    /*
     * "Offline" mode allows for documents to be written to the context rather than directly to Accumulo. This allows content indexing to run without needing to
     * connect to an active Accumulo instance.
     */
    public static final String OPT_OFFLINE = "content.ingest.documents.offline";
    /*
     * Disabling D column output will prevent document content from being written out in the context or to accumulo.
     */
    public static final String OPT_NO_D_COL = "content.ingest.documents.disable";
    /*
     * base64.dcolumn allows you to turn off base64 gzipped content and store gzipped bytes. If not set, base64 encoding is on by default.
     */
    public static final String OPT_BASE64 = "content.ingest.base64.dcolumn";
    
    private static final Logger log = Logger.getLogger(ExtendedContentIndexingColumnBasedHandler.class);
    
    protected static final String SPACE = " ";
    
    protected ExtendedContentIngestHelper ingestHelper = null;
    protected ExtendedContentDataTypeHelper dataTypeHelper = null;
    
    protected ContentIndexCounters counters = null;
    protected OffsetQueue<Integer> tokenOffsetCache = null;
    protected Set<String> zones = new HashSet<>();
    
    protected boolean eventReplaceMalformedUTF8 = false;
    protected String eventDataTypeName = null;
    protected String eventUid = null;
    protected byte[] shardId = null;
    
    protected boolean offlineDocProcessing = true;
    protected boolean disableDCol = false;
    protected ExecutorService docWriterService;
    protected BatchWriter docWriter;
    
    protected boolean tokenizerTimeWarned = false;
    
    protected boolean useBase64Encoding = true;
    
    protected Set<String> termTypeBlacklist = Collections.emptySet();
    
    protected TokenSearch searchUtil;
    protected CharArraySet stopWords;
    protected Configuration conf;
    
    protected TokenizationHelper tokenHelper = null;
    
    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
        
        conf = context.getConfiguration();
        tokenHelper = new TokenizationHelper(helper, conf);
        termTypeBlacklist = new HashSet<>(Arrays.asList(tokenHelper.getTermTypeBlacklist()));
        
        counters = new ContentIndexCounters();
        
        offlineDocProcessing = conf.getBoolean(OPT_OFFLINE, true);
        useBase64Encoding = conf.getBoolean(OPT_BASE64, true);
        disableDCol = conf.getBoolean(OPT_NO_D_COL, false);
        
        if (disableDCol) {
            // set this to true so we don't spin up a thread we don't need...
            offlineDocProcessing = true;
            log.info("D Column content storage disabled.");
        }
        
        if (!offlineDocProcessing) {
            docWriterService = Executors.newSingleThreadExecutor();
            try {
                AccumuloHelper accumuloHelper = new AccumuloHelper();
                accumuloHelper.setup(conf);
                
                log.debug("Attempting to create Accumulo connection.");
                docWriter = accumuloHelper.getConnector().createBatchWriter(conf.get("shard.table.name"),
                                new BatchWriterConfig().setMaxLatency(60, TimeUnit.SECONDS).setMaxMemory(100000000L).setMaxWriteThreads(10));
                log.debug("Created connection to Accumulo for asynchronous document storage.");
            } catch (Exception e) {
                log.warn("No document payloads will be written to Accumulo.", e);
                // giving a stub batchwriter means I don't have a bunch of "if(writer != null)"s lying around
                docWriter = new BatchWriter() {
                    @Override
                    public void addMutation(Mutation m) {}
                    
                    @Override
                    public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {}
                    
                    @Override
                    public void flush() throws MutationsRejectedException {}
                    
                    @Override
                    public void close() throws MutationsRejectedException {}
                };
            }
        }
        // The tokens offsets queue is a bounded priority queue that will allow us to cache the
        // highest cardinality offsets up to a predetermined max size
        tokenOffsetCache = new BoundedOffsetQueue<>(tokenHelper.getTokenOffsetCacheMaxSize());
        
        stopWords = tokenHelper.getStopWords();
        
        // TODO: refactor explicit DefaultTokenSearch usage here and get class from config
        searchUtil = TokenSearch.Factory.newInstance(DefaultTokenSearch.class.getCanonicalName(), stopWords, false);
        tokenHelper.configureSearchUtil(searchUtil);
        
        log.info("ExtendedContentIndexingColumnBasedHandler configured.");
    }
    
    /**
     * This method will block until all of the documents have been written to Accumulo, or a timeout has been reached.
     * 
     * TODO make the timeout configurable
     */
    @Override
    public void close(TaskAttemptContext context) {
        super.close(context);
        if (!offlineDocProcessing) {
            try {
                log.info("Attempting to flush document writer.");
                this.docWriterService.shutdown();
                this.docWriterService.awaitTermination(1, TimeUnit.MINUTES);
                this.docWriter.close();
            } catch (InterruptedException | MutationsRejectedException e) {
                log.error("Unable to terminate document writing service!", e);
            }
        }
    }
    
    @Override
    public long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {
        
        // Hold some event-specific variables to avoid re-processing
        this.shardId = getShardId(event);
        
        if (tokenHelper.isVerboseShardCounters()) {
            context.getCounter("EVENT_SHARD_ID", new String(this.shardId)).increment(1);
        }
        
        this.eventDataTypeName = event.getDataType().outputName();
        this.eventUid = event.getId().toString();
        
        // write the standard set of keys
        Multimap<BulkIngestKey,Value> keys = super.processBulk(key, event, eventFields, new ContextWrappedStatusReporter(context));
        long count = keys.size();
        contextWriter.write(keys, context);
        
        StatusReporter reporter = new ContextWrappedStatusReporter(context);
        
        // gc before we get into the tokenization piece
        keys = null;
        
        // stream the tokens to the context writer here
        count += tokenizeEvent(event, context, contextWriter, reporter);
        
        // return the number of records written
        return count;
    }
    
    public boolean isTokenizerTimeWarned() {
        return tokenizerTimeWarned;
    }
    
    /**
     * Tokenize the event, and write all of the shard, shardIndex, and shardReverseIndex keys out to the context
     * 
     * @param event
     * @param context
     * @param contextWriter
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    protected abstract long tokenizeEvent(RawRecordContainer event, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter, StatusReporter reporter) throws IOException, InterruptedException;
    
    /**
     * Process a term and zone by writting all applicable keys to the context.
     * 
     * @param event
     * @param position
     * @param termAndZone
     * @param alreadyIndexedTerms
     * @param context
     * @param contextWriter
     * @param reporter
     * @throws IOException
     * @throws InterruptedException
     */
    private void processTermAndZone(RawRecordContainer event, int position, TermAndZone termAndZone, BloomFilter alreadyIndexedTerms,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    StatusReporter reporter) throws IOException, InterruptedException {
        
        // Make sure the term length is greater than the minimum allowed length
        if (termAndZone.term.length() < tokenHelper.getTermLengthMinimum()) {
            log.debug("Ignoring token of length " + termAndZone.term.length() + " because it is too short");
            counters.increment(ContentIndexCounters.TOO_SHORT_COUNTER, reporter);
            return;
        }
        
        // Track all tokens (including synonyms) processed
        counters.increment(ContentIndexCounters.ALL_PROCESSED_COUNTER, reporter);
        
        // Normalize the term since it won't be auto-normalized through the eventFields map
        NormalizedFieldAndValue normFnV = new NormalizedFieldAndValue(termAndZone.zone, termAndZone.term);
        Set<NormalizedContentInterface> ncis = this.ingestHelper.normalize(normFnV);
        // nfv = (NormalizedFieldAndValue) this.ingestHelper.normalize(nfv);
        
        for (NormalizedContentInterface nci : ncis) {
            if (!(nci instanceof NormalizedFieldAndValue)) {
                log.warn("Can't handle a " + nci.getClass() + "; must be a NormalizedFieldAndValue.");
            }
            NormalizedFieldAndValue nfv = (NormalizedFieldAndValue) nci;
            byte[] fieldVisibility = getVisibility(event, nfv);
            
            // Build the event column key/value
            createShardEventColumn(event, contextWriter, context, nfv, this.shardId, fieldVisibility);
            
            // Create a index normalized variant of the term and zone for indexing purposes
            TermAndZone indexedTermAndZone = new TermAndZone(nfv.getIndexedFieldValue(), nfv.getIndexedFieldName());
            
            org.apache.hadoop.util.bloom.Key alreadySeen = null;
            if ((alreadyIndexedTerms != null)
                            && alreadyIndexedTerms.membershipTest(alreadySeen = new org.apache.hadoop.util.bloom.Key(indexedTermAndZone.getToken().getBytes()))) {
                if (log.isDebugEnabled()) {
                    log.debug("Not creating index mutations for " + termAndZone + " as we've already created mutations for it.");
                }
                counters.increment(ContentIndexCounters.BLOOM_FILTER_EXISTS, reporter);
            } else if ((tokenOffsetCache != null) && tokenOffsetCache.containsKey(indexedTermAndZone)) {
                if (log.isDebugEnabled()) {
                    log.debug("Not creating index mutations for " + termAndZone + " as we've already created mutations for it.");
                }
                counters.increment(ContentIndexCounters.TOKEN_OFFSET_CACHE_EXISTS, reporter);
            } else {
                // create the index
                createShardIndexColumns(event, contextWriter, context, nfv, this.shardId, fieldVisibility);
                
                if (alreadyIndexedTerms != null) {
                    alreadyIndexedTerms.add(alreadySeen);
                    counters.increment(ContentIndexCounters.BLOOM_FILTER_ADDED, reporter);
                }
            }
            
            // Now add the offset to the token offset queue, and if we overflow then output the overflow
            if (tokenOffsetCache != null) {
                OffsetList<Integer> overflow = tokenOffsetCache.addOffset(indexedTermAndZone, position);
                if (overflow != null) {
                    // no need to normalize as that was already done upon insertion into the token offset cache
                    NormalizedFieldAndValue overflowNfv = new NormalizedFieldAndValue(overflow.termAndZone.zone, overflow.termAndZone.term);
                    byte[] overflowFieldVisibility = getVisibility(event, overflowNfv);
                    
                    // Build the field index key/value
                    createTermFrequencyIndex(event, contextWriter, context, this.shardId, overflowNfv, overflow.offsets, overflowFieldVisibility,
                                    this.ingestHelper.getDeleteMode());
                    counters.increment(ContentIndexCounters.TOKENIZER_OFFSET_CACHE_OVERFLOWS, reporter);
                    counters.incrementValue(ContentIndexCounters.TOKENIZER_OFFSET_CACHE_POSITIONS_OVERFLOWED, overflow.offsets.size(), reporter);
                }
            } else {
                createTermFrequencyIndex(event, contextWriter, context, this.shardId, nfv, Arrays.asList(position), fieldVisibility,
                                this.ingestHelper.getDeleteMode());
            }
        }
    }
    
    protected void buildAllPhrases(ArrayList<Collection<String>> terms, String zone, RawRecordContainer event, int position, BloomFilter alreadyIndexedTerms,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    StatusReporter reporter) throws IOException, InterruptedException {
        if (terms.size() < 2) {
            // An empty list has no tokens/phrases to emit and phrases of length one
            // were already handled
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (String term : terms.get(0)) {
            if (term.length() <= tokenHelper.getTermLengthMinimum()) {
                continue;
            }
            sb.append(term);
            // Need to move the position pointer back by the amount of the phrase lengths
            // accounting for zero-indexing
            completePhrase(sb, terms.subList(1, terms.size()), zone, event, position - (terms.size() - 1), alreadyIndexedTerms, context, contextWriter,
                            reporter);
            
            sb.setLength(0);
        }
    }
    
    private void completePhrase(StringBuilder baseTerm, List<Collection<String>> terms, String zone, RawRecordContainer event, int position,
                    BloomFilter alreadyIndexedTerms, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter, StatusReporter reporter) throws IOException, InterruptedException {
        if (terms.isEmpty()) {
            return;
        }
        for (String term : terms.get(0)) {
            if (term == null) {
                continue;
            }
            boolean properLen = term.length() >= tokenHelper.getTermLengthMinimum();
            // Add the current term and emit the phrase if the current term isn't empty
            if (properLen) {
                baseTerm.append(SPACE).append(term);
                
                counters.increment(ContentIndexCounters.PHRASES_PROCESSED_COUNTER, reporter);
                
                processTermAndZone(event, position, new TermAndZone(baseTerm.toString(), zone), alreadyIndexedTerms, context, contextWriter, reporter);
            }
            
            // If we have more terms to add to this phrase, recurse
            if (terms.size() > 1) {
                completePhrase(baseTerm, terms.subList(1, terms.size()), zone, event, position, alreadyIndexedTerms, context, contextWriter, reporter);
            }
            
            // Only remove the space and term if we actually added one
            if (properLen) {
                // Remove the space and the token we appended last
                baseTerm.setLength(baseTerm.length() - 1 - term.length());
            }
        }
    }
    
    static final Pattern EMPTY_PATTERN = Pattern.compile("\\s*");
    
    /**
     * Return true if this term appears to be empty (all spaces)
     *
     * @param term
     * @return true if term is zero or more spaces
     */
    protected static boolean isEmptyTerm(String term) {
        return ((term.length() == 0) || EMPTY_PATTERN.matcher(term).matches());
    }
    
    /**
     * Creates and writes the BulkIngestKey for the event's field/value to the ContextWriter (instead of the Multimap that the {@link ShardedDataTypeHandler}
     * uses).
     * 
     * @param event
     * @param contextWriter
     * @param context
     * @param nFV
     * @param shardId
     * @param visibility
     * @throws IOException
     * @throws InterruptedException
     */
    protected void createShardEventColumn(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, NormalizedContentInterface nFV, byte[] shardId,
                    byte[] visibility) throws IOException, InterruptedException {
        
        String fieldName = nFV.getEventFieldName();
        String fieldValue = nFV.getEventFieldValue();
        
        if (this.ingestHelper.isIndexOnlyField(fieldName) || this.ingestHelper.isCompositeField(fieldName))
            return;
        
        if (StringUtils.isEmpty(fieldValue))
            return;
        
        Text colf = new Text(event.getDataType().outputName());
        TextUtil.textAppend(colf, event.getId().toString(), this.eventReplaceMalformedUTF8);
        
        Text colq = new Text(fieldName);
        TextUtil.textAppend(colq, fieldValue, this.ingestHelper.getReplaceMalformedUTF8());
        Key k = createKey(shardId, colf, colq, visibility, event.getDate(), this.ingestHelper.getDeleteMode());
        BulkIngestKey bKey = new BulkIngestKey(new Text(this.getShardTableName()), k);
        contextWriter.write(bKey, DataTypeHandler.NULL_VALUE, context);
    }
    
    /**
     * Creates and writes the BulkIngestKey for the event's field and global indexes to the ContextWriter
     * 
     * @param event
     * @param contextWriter
     * @param context
     * @param nFV
     * @param shardId
     * @param fieldVisibility
     * @throws IOException
     * @throws InterruptedException
     */
    protected void createShardIndexColumns(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, NormalizedContentInterface nFV, byte[] shardId,
                    byte[] fieldVisibility) throws IOException, InterruptedException {
        
        if (log.isDebugEnabled()) {
            log.debug("Creating a mutation for " + nFV.getIndexedFieldValue() + ':' + nFV.getIndexedFieldName());
        }
        
        // Still need the field index record for the token
        createShardFieldIndexColumn(event, contextWriter, context, nFV, shardId, null, fieldVisibility, this.ingestHelper.getReplaceMalformedUTF8(),
                        this.ingestHelper.getDeleteMode());
        
        // If we're creating index terms
        if ((null != this.getShardIndexTableName()) && this.ingestHelper != null) {
            if (this.ingestHelper.isIndexedField(nFV.getIndexedFieldName())) {
                // Throw it into the index
                createIndexColumn(event, contextWriter, context, nFV, shardId, this.getShardIndexTableName(), fieldVisibility,
                                this.ingestHelper.getReplaceMalformedUTF8(), this.ingestHelper.getDeleteMode());
            }
        }
        
        // If we're creating reverse index terms
        if ((null != this.getShardReverseIndexTableName()) && this.ingestHelper != null) {
            if (this.ingestHelper.isReverseIndexedField(nFV.getIndexedFieldName())) {
                // Throw the reversed term into the reverse index
                NormalizedContentInterface reverseNfv = new NormalizedFieldAndValue(nFV);
                reverseNfv.setIndexedFieldValue(new StringBuilder(nFV.getIndexedFieldValue()).reverse().toString());
                createIndexColumn(event, contextWriter, context, reverseNfv, shardId, this.getShardReverseIndexTableName(), fieldVisibility,
                                this.ingestHelper.getReplaceMalformedUTF8(), this.ingestHelper.getDeleteMode());
            }
        }
    }
    
    /**
     * Writes the document's content into the {@link #FULL_CONTENT_COLUMN_FAMILY} column family. The data is compressed (GZIP) and Base64 encoded before being
     * placed into the value.
     * 
     * @param event
     * @param contextWriter
     * @param context
     * @param reporter
     * @param uid
     * @param visibility
     * @param shardId
     * @param rawValue
     * @throws IOException
     * @throws InterruptedException
     * @throws MutationsRejectedException
     */
    protected void createContentRecord(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, StatusReporter reporter, Text uid, byte[] visibility,
                    byte[] shardId, byte[] rawValue) throws IOException, InterruptedException, MutationsRejectedException {
        
        Key k = createKey(shardId, new Text(ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY), uid, visibility, event.getDate(),
                        this.ingestHelper.getDeleteMode());
        
        ByteArrayOutputStream baos = null;
        Base64.OutputStream b64os = null;
        GZIPOutputStream gzos = null;
        Value value = null;
        try {
            baos = new ByteArrayOutputStream(Math.max(rawValue.length / 2, 1024));
            if (useBase64Encoding) {
                b64os = new Base64.OutputStream(baos, Base64.ENCODE);
            }
            gzos = new GZIPOutputStream(useBase64Encoding ? b64os : baos);
            
            gzos.write(rawValue);
        } finally {
            closeOutputStreams(gzos, b64os, baos);
            if (baos != null) {
                value = new Value(baos.toByteArray());
            }
            gzos = null;
            b64os = null;
            baos = null;
        }
        counters.increment(ContentIndexCounters.CONTENT_RECORDS_CREATED, reporter);
        if (!disableDCol) {
            if (offlineDocProcessing) {
                BulkIngestKey bKey = new BulkIngestKey(new Text(this.getShardTableName()), k);
                contextWriter.write(bKey, value, context);
            } else {
                DocWriter dw = new DocWriter();
                dw.k = k;
                dw.shardId = shardId;
                dw.visibility = visibility;
                dw.event = event;
                dw.value = value;
                this.docWriterService.execute(dw);
            }
        }
    }
    
    /**
     * Ensures all of the output streams are closed
     * 
     * @param streams
     *            order to attempt closing: outermost first, innermost last
     */
    public static void closeOutputStreams(OutputStream... streams) {
        for (OutputStream stream : streams) {
            if (null != stream) {
                try {
                    stream.close();
                    return; // if outermost one closed, then we can stop
                } catch (IOException e) {
                    log.trace("Failed to close stream: " + stream.getClass().getCanonicalName(), e);
                }
            }
        }
    }
    
    /**
     * Used to track tokenization execution time. It's too expensive to perform a call to System.currentTimeMillis() each time we produce a new token, so spawn
     * a thread that increments a counter every 500ms.
     * <p>
     * The main thread will check the counter value each time it produces a new token and thus track the number of ticks that have elapsed.
     */
    protected static class HeartBeatThread extends Thread {
        public static final long INTERVAL = 500; // half second resolution
        public static volatile int counter = 0;
        public static long lastRun;
        
        static {
            new HeartBeatThread().start();
        }
        
        private HeartBeatThread() {
            super("HeartBeatThread");
            setDaemon(true);
        }
        
        public void run() {
            while (true) {
                try {
                    Thread.sleep(INTERVAL);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                
                // verify that we're exeuting in a timely fashion
                // ..if not warn.
                long currentRun = System.currentTimeMillis();
                long delta = currentRun - lastRun;
                if (delta > (INTERVAL * 1.5)) {
                    log.warn("HeartBeatThread starved for cpu, " + "should execute every " + INTERVAL + " ms, " + "latest: " + delta + " ms.");
                }
                lastRun = currentRun;
                counter++;
            }
        }
    }
    
    private class DocWriter implements Runnable {
        Key k;
        byte[] shardId;
        byte[] visibility;
        RawRecordContainer event;
        Value value;
        
        @Override
        public void run() {
            log.debug("Writing out a document of size " + value.get().length + " bytes.");
            Mutation m = new Mutation(new Text(shardId));
            m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(visibility), event.getDate(), value);
            try {
                docWriter.addMutation(m);
            } catch (MutationsRejectedException e) {
                log.error("Could not write document payload to Accumulo!", e);
            }
        }
    }
    
    // Used to indicate that there was a case where the tokenizer took too
    // long.
    public static class TokenizerTimeoutException extends IOException {
        
        private static final long serialVersionUID = 2307696490675641276L;
        
        public TokenizerTimeoutException(String message) {
            super(message);
        }
    }
    
    /**
     * Creates and writes the BulkIngestKey for the field index to the ContextWriter (instead of the Multimap that the {@link ShardedDataTypeHandler} uses).
     * 
     * @param event
     * @param contextWriter
     * @param context
     * @param nFV
     * @param shardId
     * @param value
     * @param visibility
     * @param replaceMalformedUTF8
     * @param deleteMode
     * @throws IOException
     * @throws InterruptedException
     */
    protected void createShardFieldIndexColumn(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, NormalizedContentInterface nFV, byte[] shardId,
                    Value value, byte[] visibility, boolean replaceMalformedUTF8, boolean deleteMode) throws IOException, InterruptedException {
        Text colf = new Text("fi");
        TextUtil.textAppend(colf, nFV.getIndexedFieldName(), replaceMalformedUTF8);
        Text colq = new Text(nFV.getIndexedFieldValue());
        TextUtil.textAppend(colq, this.eventDataTypeName, replaceMalformedUTF8);
        TextUtil.textAppend(colq, this.eventUid, replaceMalformedUTF8);
        
        if (value == null) {
            value = DataTypeHandler.NULL_VALUE;
        }
        
        Key k = createKey(shardId, colf, colq, visibility, event.getDate(), deleteMode);
        BulkIngestKey bKey = new BulkIngestKey(new Text(this.getShardTableName()), k);
        contextWriter.write(bKey, value, context);
    }
    
    /**
     * Creates a Term Frequency index key in the "tf" column family.
     * 
     * @param event
     * @param contextWriter
     * @param context
     * @param shardId
     * @param nfv
     * @param offsets
     * @param visibility
     * @param deleteMode
     * @throws IOException
     * @throws InterruptedException
     */
    protected void createTermFrequencyIndex(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, byte[] shardId, NormalizedFieldAndValue nfv,
                    List<Integer> offsets, byte[] visibility, boolean deleteMode) throws IOException, InterruptedException {
        
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
                        ExtendedDataTypeHandler.TERM_FREQUENCY_COLUMN_FAMILY.getBytes(), colq.toString().getBytes(), visibility, event.getDate(), deleteMode));
        
        contextWriter.write(bKey, value, context);
    }
    
    /**
     * Creates and writes the BulkIngestKey for the global (reverse) index to the ContextWriter (instead of the Multimap that the {@link ShardedDataTypeHandler}
     * uses).
     * 
     * @param event
     * @param contextWriter
     * @param context
     * @param nFV
     * @param shardId
     * @param tableName
     * @param visibility
     * @param replacedMalformedUTF8
     * @param deleteMode
     * @throws IOException
     * @throws InterruptedException
     */
    protected void createIndexColumn(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, NormalizedContentInterface nFV, byte[] shardId,
                    Text tableName, byte[] visibility, boolean replacedMalformedUTF8, boolean deleteMode) throws IOException, InterruptedException {
        
        // Shard Global Index Table Structure
        // Row: Field Value
        // Colf: Field Name
        // Colq: Shard Id : DataType
        // Value: UID
        Text colf = new Text(nFV.getIndexedFieldName());
        Text colq = new Text(shardId);
        TextUtil.textAppend(colq, this.eventDataTypeName, replacedMalformedUTF8);
        
        Key k = this.createIndexKey(nFV.getIndexedFieldValue().getBytes(), colf, colq, visibility, event.getDate(), deleteMode);
        
        // Create a UID object for the Value
        Uid.List.Builder uidBuilder = Uid.List.newBuilder();
        uidBuilder.setIGNORE(false);
        if (!deleteMode) {
            uidBuilder.setCOUNT(1);
            uidBuilder.addUID(this.eventUid);
        } else {
            uidBuilder.setCOUNT(-1);
            uidBuilder.addUID(this.eventUid);
        }
        Uid.List uidList = uidBuilder.build();
        Value val = new Value(uidList.toByteArray());
        
        BulkIngestKey bKey = new BulkIngestKey(tableName, k);
        contextWriter.write(bKey, val, context);
    }
}
