package datawave.experimental.doc;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;
import datawave.experimental.util.ScanStats;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.postprocessing.tf.Function;
import datawave.query.postprocessing.tf.TermOffsetPopulator;
import datawave.experimental.QueryTermVisitor;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Provides methods for fetching document keys, field index keys, and term frequency keys
 */
public class DocumentFetcher {
    
    private static final Logger log = Logger.getLogger(DocumentFetcher.class);
    
    protected Connector conn;
    
    protected String scanId;
    protected String tableName;
    protected Authorizations auths;
    
    // some constants
    private final String TF_STRING = "tf";
    private final Text TF_CF = new Text(TF_STRING);
    private final String NULL_BYTE = "\0";
    private final char NULL_CHAR = '\u0000';
    
    private ScanStats scanStats = new ScanStats();
    
    public DocumentFetcher(String scanId, Connector conn, String tableName, Authorizations auths) {
        this.scanId = scanId;
        this.conn = conn;
        this.tableName = tableName;
        this.auths = auths;
    }
    
    /**
     * Fetch a document given the provided range, datatype and uid. Does not fetch index only or term frequency fields
     *
     * @param range
     *            a range that defines a document
     * @param datatypeUid
     *            the datatype, null byte, and uid
     * @return a Document
     */
    public Document fetchDocument(Range range, String datatypeUid) {
        long start = System.currentTimeMillis();
        Document d = new Document();
        try (Scanner scanner = conn.createScanner(tableName, auths)) {
            scanner.setRange(range);
            // this may break tld
            scanner.fetchColumnFamily(new Text(datatypeUid)); // uid is actually datatype\0uid
            
            String field;
            Attribute<?> attr;
            for (Map.Entry<Key,Value> entry : scanner) {
                field = fieldFromKey(entry.getKey());
                attr = keyToAttribute(entry.getKey());
                d.put(field, attr);
                scanStats.incrementNextDocumentAggregation();
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("exception while fetching document " + datatypeUid + ", error was: " + e.getMessage());
        }
        log.info("time to fetch document " + datatypeUid + " was " + (System.currentTimeMillis() - start) + " ms");
        return d;
    }
    
    /**
     * Fetch the attributes of a document given the provided range, datatype and uid. Does not fetch index only or term frequency fields
     *
     * @param range
     *            a range that defines a document
     * @param datatypeUid
     *            the datatype, null byte, and uid
     * @return a list of attributes that make up a Document
     */
    public List<Map.Entry<Key,Value>> fetchDocumentAttributes(Range range, String datatypeUid) {
        long start = System.currentTimeMillis();
        final List<Map.Entry<Key,Value>> attrs = new ArrayList<>();
        try (Scanner scanner = conn.createScanner(tableName, auths)) {
            scanner.setRange(range);
            // this may break tld
            scanner.fetchColumnFamily(new Text(datatypeUid)); // uid is actually datatype\0uid
            
            for (Map.Entry<Key,Value> entry : scanner) {
                attrs.add(entry);
                scanStats.incrementNextDocumentAggregation();
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("exception while fetching document " + datatypeUid + ", error was: " + e.getMessage());
        }
        log.info("time to fetch attrs " + datatypeUid + " was " + (System.currentTimeMillis() - start) + " ms");
        return attrs;
    }
    
    /**
     * Extract the field from a document key
     *
     * @param key
     *            a document key like shard:dt\x00uid:field\x00value
     * @return the field
     */
    private String fieldFromKey(Key key) {
        String cq = key.getColumnQualifier().toString();
        int nullIndex = cq.indexOf(NULL_CHAR);
        return cq.substring(0, nullIndex);
    }
    
    /**
     * Transform a document key into an attribute
     *
     * @param key
     *            a document key like shard:dt\x00uid:field\x00value
     * @return an attribute
     */
    private Attribute<?> keyToAttribute(Key key) {
        String cq = key.getColumnQualifier().toString();
        int nullIndex = cq.indexOf(NULL_CHAR);
        String value = cq.substring(nullIndex + 1);
        Key docKey = new Key(key.getRow(), key.getColumnFamily());
        return new Content(value, docKey, true);
    }
    
    /**
     * Extract the field from a field index key
     *
     * @param key
     *            a field index key like shard:fi\x00field:value\x00dt\x00uid
     * @return the field
     */
    private String fieldFromFiKey(Key key) {
        String cq = key.getColumnFamily().toString();
        int nullIndex = cq.indexOf(NULL_CHAR);
        return cq.substring(nullIndex + 1);
    }
    
    /**
     * Transform a field index key into an attribute
     *
     * @param key
     *            a field index key like shard:fi\x00field:value\x00dt\x00uid
     * @return an attribute
     */
    private Attribute<?> fiKeyToAttribute(Key key) {
        String cq = key.getColumnQualifier().toString();
        int nullIndex = cq.indexOf(NULL_CHAR);
        String value = cq.substring(0, nullIndex);
        String dtUid = cq.substring(nullIndex + 1);
        Key docKey = new Key(key.getRow(), new Text(dtUid));
        return new Content(value, docKey, true);
    }
    
    /**
     * Fetch index only fields and add them to the document. Builds a set of ranges and uses a batch scanner to fetch keys in parallel.
     *
     * @param d
     *            the document
     * @param uid
     *            the uid
     * @param indexOnlyFields
     *            a set of index only fields
     * @param terms
     *            a set of all query terms
     */
    public void fetchIndexOnlyFields(Document d, String shard, String uid, Set<String> indexOnlyFields, Set<JexlNode> terms) {
        long start = System.currentTimeMillis();
        // 1. filter terms to just index only
        Set<JexlNode> filtered = new HashSet<>();
        for (JexlNode term : terms) {
            if (!(term instanceof ASTAndNode)) { // use this one weird trick to avoid bounded ranges
                String field = JexlASTHelper.getIdentifier(term);
                if (indexOnlyFields.contains(field)) {
                    filtered.add(term);
                }
            }
        }
        
        if (filtered.isEmpty())
            return;
        
        // 2. Create a set of ranges to fetch
        Set<String> fields = new HashSet<>();
        SortedSet<Range> ranges = new TreeSet<>();
        for (JexlNode node : filtered) {
            if (node instanceof ASTERNode || node instanceof ASTNRNode) {
                throw new IllegalStateException("");
            }
            
            String field = JexlASTHelper.getIdentifier(node);
            String value = (String) JexlASTHelper.getLiteralValue(node);
            Range range = createIndexOnlyFetchRange(shard, field, value, uid);
            ranges.add(range);
            fields.add(field);
        }
        
        // 3. Fetch from the field index using a batch scanner
        int defaultThreads = 10;
        int threads = Math.min(ranges.size(), defaultThreads);
        try (BatchScanner scanner = conn.createBatchScanner(tableName, auths, threads)) {
            scanner.setRanges(ranges);
            for (String field : fields) {
                scanner.fetchColumnFamily(new Text("fi" + NULL_BYTE + field));
            }
            
            String field;
            Attribute<?> attr;
            for (Map.Entry<Key,Value> entry : scanner) {
                field = fieldFromFiKey(entry.getKey());
                attr = fiKeyToAttribute(entry.getKey());
                d.put(field, attr);
                scanStats.incrementNextFieldIndex();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error fetching field index entries for doc range");
        }
        log.info("time to fetch " + ranges.size() + " index-only fields for document " + uid + " was " + (System.currentTimeMillis() - start) + " ms");
    }
    
    /**
     * Build a field index range given all the requisite components
     *
     * @param shard
     *            the shard
     * @param field
     *            the field
     * @param value
     *            the normalized value
     * @param uid
     *            the uid
     * @return a field index range
     */
    private Range createIndexOnlyFetchRange(String shard, String field, String value, String uid) {
        Key startKey = new Key(shard, "fi" + NULL_BYTE + field, value + NULL_BYTE + uid);
        Key endKey;
        boolean isTld = false;
        if (isTld) {
            endKey = new Key(shard, "fi" + NULL_BYTE + field, value + NULL_BYTE + uid);
        } else {
            endKey = startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        }
        return new Range(startKey, true, endKey, true);
    }
    
    /**
     * Given a set of fields and the results of a field index lookup, generate index only document keys and put them into the document
     *
     * @param d
     *            the document
     * @param fields
     *            set of index only fields
     * @param uid
     *            the uid for the document
     * @param nodesToUids
     *            a mapping of nodes to uids
     */
    public void createIndexOnlyFields(Document d, String shard, Set<String> fields, String uid, Map<String,Set<String>> nodesToUids) {
        String value;
        Attribute<?> attr;
        Key docKey = new Key(shard, uid);
        for (String key : nodesToUids.keySet()) {
            JexlNode node = parseKeyToJexlNode(key).jjtGetChild(0);
            for (String field : fields) {
                if (key.contains(field) && nodesToUids.get(key).contains(uid)) {
                    value = (String) JexlASTHelper.getLiteralValue(node);
                    attr = new Content(value, docKey, true);
                    d.put(field, attr);
                }
            }
        }
    }
    
    /**
     * Parse a Jexl string into a JexlNode
     *
     * @param key
     *            a Jexl string
     * @return a JexlNode
     */
    private JexlNode parseKeyToJexlNode(String key) {
        try {
            return JexlASTHelper.parseJexlQuery(key);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    /**
     * Fetch TermFrequency offsets.
     *
     * @param script
     * @param d
     * @param uid
     * @param tfFields
     * @return
     */
    public Map<String,Object> fetchTermFrequencyOffsets(ASTJexlScript script, Document d, String shard, String uid, Set<String> tfFields) {
        long start = System.currentTimeMillis();
        // first check for a content function, i.e. that the query actually requires offsets
        Multimap<String,Function> functionMap = TermOffsetPopulator.getContentFunctions(script);
        
        // by default just fetch all
        Set<String> args = new HashSet<>();
        for (String key : functionMap.keySet()) {
            Collection<Function> functions = functionMap.get(key);
            for (Function function : functions) {
                for (JexlNode arg : function.args()) {
                    args.add(JexlStringBuildingVisitor.buildQueryWithoutParse(arg));
                }
            }
        }
        
        Set<JexlNode> queryTerms = QueryTermVisitor.parse(script);
        Set<JexlNode> tfTerms = new HashSet<>();
        
        for (JexlNode term : queryTerms) {
            String built = JexlStringBuildingVisitor.buildQueryWithoutParse(term);
            boolean containsTokenizedField = false;
            for (String tfField : tfFields) {
                if (built.contains(tfField)) {
                    containsTokenizedField = true;
                    break;
                }
            }
            
            boolean containsContentFunctionArg = false;
            for (String arg : args) {
                if (built.contains(arg)) {
                    containsContentFunctionArg = true;
                    break;
                }
            }
            
            if (containsTokenizedField || containsContentFunctionArg) {
                tfTerms.add(term);
            }
        }
        
        // fetch all tf args
        Map<String,TermFrequencyList> termOffsetMap = new HashMap<>();
        for (JexlNode term : tfTerms) {
            String field = JexlASTHelper.getIdentifier(term);
            String value = (String) JexlASTHelper.getLiteralValue(term);
            
            TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> offsets = fetchOffset(shard, field, value, uid, d);
            if (!offsets.isEmpty()) {
                TermFrequencyList tfl = termOffsetMap.get(value);
                if (null == tfl) {
                    termOffsetMap.put(value, new TermFrequencyList(offsets));
                } else {
                    // Merge in the offsets for the current field+term with all previous
                    // offsets from other fields in the same term
                    tfl.addOffsets(offsets);
                }
            }
        }
        
        // put term offset map
        Map<String,Object> map = new HashMap<>();
        map.put(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffsetMap);
        log.info("time to fetch " + tfTerms + " term frequency fields for document " + uid + " was " + (System.currentTimeMillis() - start) + " ms");
        return map;
    }
    
    /**
     * Fetch offsets.
     *
     * @param shard
     * @param field
     * @param value
     * @param uid
     * @param d
     * @return
     */
    private TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> fetchOffset(String shard, String field, String value, String uid, Document d) {
        TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> offsets = TreeMultimap.create();
        Range range = buildTermFrequencyRange(shard, field, value, uid);
        try (Scanner scanner = conn.createScanner(tableName, auths)) {
            scanner.setRange(range);
            scanner.fetchColumnFamily(TF_CF);
            for (Map.Entry<Key,Value> entry : scanner) {
                TermWeight.Info twInfo = TermWeight.Info.parseFrom(entry.getValue().get());
                TermFrequencyList.Zone twZone = new TermFrequencyList.Zone(field, true, uid);
                TermWeightPosition.Builder position = new TermWeightPosition.Builder();
                for (int i = 0; i < twInfo.getTermOffsetCount(); i++) {
                    position.setTermWeightOffsetInfo(twInfo, i);
                    offsets.put(twZone, position.build());
                    position.reset();
                }
                
                // need to add fragment to the document for the case of a non-indexed, tokenized field
                d.put(field, new Content(value, entry.getKey(), true));
                
                scanStats.incrementNextTermFrequency();
            }
        } catch (TableNotFoundException | InvalidProtocolBufferException e) {
            e.printStackTrace();
            log.error("exception while fetching tf offsets: " + e.getMessage());
        }
        return offsets;
    }
    
    /**
     * Build a TermFrequency range given its requisite parts
     *
     * @param shard
     *            the shard
     * @param field
     *            the field
     * @param value
     *            the value
     * @param uid
     *            the uid
     * @return a term frequency fetch range
     */
    private Range buildTermFrequencyRange(String shard, String field, String value, String uid) {
        boolean isTld = false;
        Key startKey = new Key(shard, TF_STRING, uid + NULL_BYTE + value + NULL_BYTE + field);
        Key endKey;
        if (isTld) {
            endKey = null;
        } else {
            endKey = startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        }
        return new Range(startKey, true, endKey, false);
    }
    
    public ScanStats getScanStats() {
        return this.scanStats;
    }
}
