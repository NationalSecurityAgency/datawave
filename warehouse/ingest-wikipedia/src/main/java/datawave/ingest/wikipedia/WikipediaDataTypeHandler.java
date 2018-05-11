package datawave.ingest.wikipedia;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.ContextWrappedStatusReporter;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.content.BoundedOffsetQueue.OffsetList;
import datawave.ingest.mapreduce.handler.shard.content.ContentIndexCounters;
import datawave.ingest.mapreduce.handler.shard.content.TermAndZone;
import datawave.ingest.mapreduce.handler.tokenize.ExtendedContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.util.TextUtil;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * 
 */
public class WikipediaDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> extends ExtendedContentIndexingColumnBasedHandler<KEYIN,KEYOUT,VALUEOUT> {
    private static final Logger log = Logger.getLogger(WikipediaDataTypeHandler.class);
    
    private static final String REVISION_TEXT_FIELD_NAME = "REVISION_TEXT";
    private static final String REVISION_TEXT_TOKEN = "REVISION_TEXT_TOKEN";
    private static final String REVISION_COMMENT_FIELD_NAME = "REVISION_COMMENT";
    private static final String REVISION_COMMENT_TOKEN = "REVISION_COMMENT_TOKEN";
    
    private static final String REVISION_COMMENT = "comment";
    private static final String REVISION_TEXT = "text";
    
    /*
     * Disabling D column output will prevent document content from being written out in the context or to accumulo.
     */
    public static final String OPT_NO_D_COL = "wikipedia.ingest.documents.disable";
    
    private static DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    
    private DocumentBuilder parser = null;
    private WikipediaIngestHelper ingestHelper = null;
    private WikipediaHelper helper = null;
    
    @Override
    public void setup(TaskAttemptContext context) {
        super.setup(context);
        
        // Dealing with the stupid in AbstractDataTypeHandler
        if (this.getHelper(null) instanceof WikipediaIngestHelper) {
            this.ingestHelper = (WikipediaIngestHelper) this.getHelper(null);
            this.helper = this.ingestHelper.getHelper();
        }
        
        try {
            this.parser = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException("Error instantiating DocumentBuilder", e);
        }
        
        Configuration conf = context.getConfiguration();
        
        this.counters = new ContentIndexCounters();
        disableDCol = conf.getBoolean(OPT_NO_D_COL, disableDCol);
        
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
    
    @Override
    public long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {
        
        // Hold some event-specific variables to avoid re-processing
        this.shardId = getShardId(event);
        
        this.eventDataTypeName = event.getDataType().outputName();
        this.eventUid = event.getId().toString();
        
        // write the standard set of keys
        Multimap<BulkIngestKey,Value> keys = super.processBulk(key, event, eventFields, new ContextWrappedStatusReporter(context));
        long count = keys.size();
        contextWriter.write(keys, context);
        
        // gc before we get into the tokenization piece
        keys = null;
        
        // stream the tokens to the context writer here
        StatusReporter reporter = new ContextWrappedStatusReporter(context);
        count += tokenizeEvent(event, context, contextWriter, reporter);
        
        // return the number of records written
        return count;
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
    @Override
    protected long tokenizeEvent(RawRecordContainer event, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    ContextWriter<KEYOUT,VALUEOUT> contextWriter, StatusReporter reporter) throws IOException, InterruptedException {
        
        long count = 0;
        
        final byte[] visibility = flatten(event.getVisibility());
        final byte[] rawData = event.getRawData();
        
        Document root;
        try {
            root = this.parser.parse(new ByteArrayInputStream(rawData));
        } catch (SAXException e) {
            throw new RuntimeException(e);
        }
        
        NodeList revisions = root.getElementsByTagName("revision");
        
        // For each revision, try to find the stuff we want to tokenize
        for (int i = 0; i < revisions.getLength(); i++) {
            Node revision = revisions.item(i);
            NodeList children = revision.getChildNodes();
            for (int j = 0; j < children.getLength(); j++) {
                Node revChild = children.item(j);
                
                if (REVISION_COMMENT.equals(revChild.getNodeName())) {
                    count += tokenizeTextNode(revChild.getTextContent(), event, visibility, context, contextWriter, REVISION_COMMENT_FIELD_NAME,
                                    REVISION_COMMENT_TOKEN, reporter);
                } else if (REVISION_TEXT.equals(revChild.getNodeName())) {
                    count += tokenizeTextNode(revChild.getTextContent(), event, visibility, context, contextWriter, REVISION_TEXT_FIELD_NAME,
                                    REVISION_TEXT_TOKEN, reporter);
                }
            }
        }
        
        return count;
    }
    
    protected long tokenizeTextNode(String content, RawRecordContainer event, byte[] visibility,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    String fieldName, String fieldNameToken, StatusReporter reporter) throws IOException, InterruptedException {
        
        StringReader contentReader = new StringReader(content);
        
        int position = 0;
        try {
            if (helper.includeContent()) {
                // Create the prefix for the full content and any alternate view content keys
                Text colfBase = new Text(event.getDataType().outputName());
                TextUtil.textAppend(colfBase, this.eventUid, this.ingestHelper.getReplaceMalformedUTF8());
                
                Text colf = new Text();
                
                colf.set(colfBase.getBytes());
                
                final String contentPresenceFieldName = "HAS_" + fieldName + "_CONTENT";
                
                // Use the real field name for the 'd' record
                NormalizedContentInterface norm = new NormalizedFieldAndValue(fieldName, "_");
                TextUtil.textAppend(colf, norm.getEventFieldName());
                
                // Create the full content record
                if (!content.isEmpty()) {
                    createContentRecord(event, contextWriter, context, reporter, colf, visibility, this.shardId, content.getBytes());
                    
                    norm = new NormalizedFieldAndValue(contentPresenceFieldName, "true");
                    byte[] fieldVisibility = getVisibility(event, norm);
                    createShardEventColumn(event, contextWriter, context, norm, this.shardId, fieldVisibility);
                    Multimap<String,NormalizedContentInterface> contentFields = HashMultimap.create();
                    contentFields.put(contentPresenceFieldName, norm);
                    getMetadata().addEvent(this.ingestHelper, event, contentFields, false);
                }
            }
            
            WikipediaTokenizer wikiTokenizer = new WikipediaTokenizer(contentReader);
            CharTermAttribute termAttr = wikiTokenizer.addAttribute(CharTermAttribute.class);
            wikiTokenizer.reset();
            
            while (wikiTokenizer.incrementToken()) {
                String term = termAttr.toString();
                
                // getting the next token can take a long time depending on the compexity of the data...
                // so lets report progress to hadoop on each round
                if (context != null)
                    context.progress();
                
                if (StringUtils.isBlank(term)) {
                    context.getCounter("Tokenization", "Blank tokens (null, empty, or whitespace)").increment(1l);
                    continue;
                }
                
                processTerm(event, position, term, null, context, contextWriter, fieldName, fieldNameToken, reporter);
                
                // Get the word position for this term
                position++;
            }
            
            // now flush out the offset queue
            if (tokenOffsetCache != null) {
                for (OffsetList<Integer> offsets : tokenOffsetCache.offsets()) {
                    // no need to normalize as that was already done upon insertion into the token offset cache
                    NormalizedFieldAndValue nfv = new NormalizedFieldAndValue(offsets.termAndZone.zone, offsets.termAndZone.term);
                    byte[] fieldVisibility = getVisibility(event, nfv);
                    
                    // Build the field index key/value
                    createTermFrequencyIndex(event, contextWriter, context, this.shardId, nfv, offsets.offsets, fieldVisibility, false);
                }
            }
            
            if (position > 0) {
                
                // create a term count event field
                final String termCountFieldName = fieldName + "_TERM_COUNT";
                NormalizedFieldAndValue nfav = new NormalizedFieldAndValue(termCountFieldName, Long.toString(position));
                
                Set<NormalizedContentInterface> norms = this.ingestHelper.normalize(nfav);
                byte[] fieldVisibility = getVisibility(event, nfav);
                
                HashMultimap<String,NormalizedContentInterface> normMap = HashMultimap.create();
                
                for (NormalizedContentInterface norm : norms) {
                    this.createShardEventColumn(event, contextWriter, context, norm, this.shardId, fieldVisibility);
                    this.createShardIndexColumns(event, contextWriter, context, norm, this.shardId, fieldVisibility);
                    
                    normMap.put(norm.getEventFieldName(), norm);
                }
                
                normMap.put(fieldNameToken, new NormalizedFieldAndValue(fieldNameToken, ""));
                
                getMetadata().addEvent(this.ingestHelper, event, normMap);
            }
            
        } catch (Exception e) {
            // If error, return empty results map.
            log.error("Error processing Wikipedia document", e);
            throw new RuntimeException("Error processing Wikipedia document", e);
        } finally {
            counters.flush(reporter);
            if (null != tokenOffsetCache) {
                tokenOffsetCache.clear();
            }
        }
        
        return position;
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
    @Override
    protected void createShardEventColumn(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, NormalizedContentInterface nFV, byte[] shardId,
                    byte[] visibility) throws IOException, InterruptedException {
        
        String fieldName = nFV.getEventFieldName();
        String fieldValue = nFV.getEventFieldValue();
        
        if (this.ingestHelper.isIndexOnlyField(fieldName))
            return;
        
        if (this.ingestHelper.isCompositeField(fieldName))
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
    @Override
    protected void createShardIndexColumns(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, NormalizedContentInterface nFV, byte[] shardId,
                    byte[] fieldVisibility) throws IOException, InterruptedException {
        
        if (log.isDebugEnabled()) {
            log.debug("Creating a mutation for " + nFV.getIndexedFieldValue() + ':' + nFV.getIndexedFieldName());
        }
        
        // Still need the field index record for the token
        createShardFieldIndexColumn(event, contextWriter, context, nFV, shardId, null, fieldVisibility, false, false);
        
        // If we're creating index terms
        if ((null != this.getShardIndexTableName()) && this.ingestHelper != null) {
            if (this.ingestHelper.isIndexedField(nFV.getIndexedFieldName())) {
                // Throw it into the index
                createIndexColumn(event, contextWriter, context, nFV, shardId, this.getShardIndexTableName(), fieldVisibility, false, false);
            }
        }
        
        // If we're creating reverse index terms
        if ((null != this.getShardReverseIndexTableName()) && this.ingestHelper != null) {
            if (this.ingestHelper.isReverseIndexedField(nFV.getIndexedFieldName())) {
                // Throw the reversed term into the reverse index
                NormalizedContentInterface reverseNfv = new NormalizedFieldAndValue(nFV);
                reverseNfv.setIndexedFieldValue(new StringBuilder(nFV.getIndexedFieldValue()).reverse().toString());
                createIndexColumn(event, contextWriter, context, reverseNfv, shardId, this.getShardReverseIndexTableName(), fieldVisibility, false, false);
            }
        }
    }
    
    /**
     * Writes the document's content into the {@link ExtendedDataTypeHandler#FULL_CONTENT_COLUMN_FAMILY} column family. The data is compressed (GZIP) and Base64
     * encoded before being placed into the value.
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
    @Override
    protected void createContentRecord(RawRecordContainer event, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, StatusReporter reporter, Text uid, byte[] visibility,
                    byte[] shardId, byte[] rawValue) throws IOException, InterruptedException, MutationsRejectedException {
        
        if (disableDCol) {
            return;
        }
        
        Key k = createKey(shardId, new Text(ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY), uid, visibility, event.getDate(),
                        this.ingestHelper.getDeleteMode());
        
        ByteArrayOutputStream baos = null;
        GZIPOutputStream gzos = null;
        Value value = null;
        try {
            baos = new ByteArrayOutputStream(Math.max(rawValue.length / 2, 1024));
            gzos = new GZIPOutputStream(baos);
            
            gzos.write(rawValue);
        } finally {
            closeOutputStreams(gzos, baos);
            if (baos != null) {
                value = new Value(baos.toByteArray());
            }
            gzos = null;
            baos = null;
        }
        
        this.counters.increment(ContentIndexCounters.CONTENT_RECORDS_CREATED, reporter);
        
        DocWriter dw = new DocWriter(this.docWriter);
        dw.k = k;
        dw.shardId = shardId;
        dw.visibility = visibility;
        dw.event = event;
        dw.value = value;
        
        this.docWriterService.execute(dw);
    }
    
    /**
     * Process a term and zone by writting all applicable keys to the context.
     * 
     * @param event
     * @param position
     * @param term
     * @param alreadyIndexedTerms
     * @param context
     * @param contextWriter
     * @param fieldName
     * @param fieldNameToken
     * @param reporter
     * @throws IOException
     * @throws InterruptedException
     */
    protected void processTerm(RawRecordContainer event, int position, String term, BloomFilter alreadyIndexedTerms,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter,
                    String fieldName, String fieldNameToken, StatusReporter reporter) throws IOException, InterruptedException {
        
        // Track all tokens (including synonyms) processed
        if (context != null) {
            counters.increment(ContentIndexCounters.ALL_PROCESSED_COUNTER, reporter);
        }
        
        // Normalize the term since it won't be auto-normalized through the eventFields map
        NormalizedFieldAndValue normFnV = new NormalizedFieldAndValue(fieldNameToken, term);
        Set<NormalizedContentInterface> ncis = this.ingestHelper.normalize(normFnV);
        
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
            
            if ((tokenOffsetCache != null) && tokenOffsetCache.containsKey(indexedTermAndZone)) {
                if (log.isDebugEnabled()) {
                    log.debug("Not creating index mutations for " + term + " as we've already created mutations for it.");
                }
                counters.increment(ContentIndexCounters.TOKEN_OFFSET_CACHE_EXISTS, reporter);
            } else {
                // create the index
                createShardIndexColumns(event, contextWriter, context, nfv, this.shardId, fieldVisibility);
            }
            
            // Now add the offset to the token offset queue, and if we overflow then output the overflow
            if (tokenOffsetCache != null) {
                OffsetList overflow = tokenOffsetCache.addOffset(indexedTermAndZone, position);
                if (overflow != null) {
                    // no need to normalize as that was already done upon insertion into the token offset cache
                    NormalizedFieldAndValue overflowNfv = new NormalizedFieldAndValue(overflow.termAndZone.zone, overflow.termAndZone.term);
                    byte[] overflowFieldVisibility = getVisibility(event, overflowNfv);
                    
                    // Build the field index key/value
                    createTermFrequencyIndex(event, contextWriter, context, this.shardId, overflowNfv, overflow.offsets, overflowFieldVisibility, false);
                    counters.increment(ContentIndexCounters.TOKENIZER_OFFSET_CACHE_OVERFLOWS, reporter);
                    counters.incrementValue(ContentIndexCounters.TOKENIZER_OFFSET_CACHE_POSITIONS_OVERFLOWED, overflow.offsets.size(), reporter);
                }
            } else {
                createTermFrequencyIndex(event, contextWriter, context, this.shardId, nfv, Arrays.asList(position), fieldVisibility, false);
            }
        }
    }
    
}
