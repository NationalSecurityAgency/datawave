package datawave.experimental.scanner;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.experimental.util.ScanStats;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.jexl.JexlASTHelper;

/**
 * Outstanding work: datatype filtering/optimization
 */
public class FieldIndexScanner {

    private static final Logger log = Logger.getLogger(FieldIndexScanner.class);

    private static final String NULL_BYTE = "\0";
    private static final char NULL_CHAR = '\u0000';

    private final AccumuloClient client;
    private final Authorizations auths;
    private final String tableName;
    private final String scanId;

    private final AttributeFactory preNormalizedAttributeFactory; // pre normalized

    private boolean logStats;

    private final ScanStats scanStats = new ScanStats();

    public FieldIndexScanner(AccumuloClient client, Authorizations auths, String tableName, String scanId, AttributeFactory attributeFactory) {
        this.client = client;
        this.auths = auths;
        this.tableName = tableName;
        this.scanId = scanId;
        this.preNormalizedAttributeFactory = attributeFactory;
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
        try (BatchScanner scanner = client.createBatchScanner(tableName, auths, threads)) {
            scanner.setRanges(ranges);
            for (String field : fields) {
                scanner.fetchColumnFamily(new Text("fi" + NULL_BYTE + field));
            }

            String field;
            String value;
            Attribute<?> attr;
            FieldIndexKey parser = new FieldIndexKey();
            for (Map.Entry<Key,Value> entry : scanner) {
                parser.parse(entry.getKey());
                field = parser.getField();
                value = parser.getValue();
                attr = preNormalizedAttributeFactory.create(field, value, entry.getKey(), parser.getDatatype(), true, false);
                d.put(field, attr);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error fetching field index entries for doc range");
        }

        if (logStats) {
            log.info("time to fetch " + ranges.size() + " index-only fields for document " + uid + " was " + (System.currentTimeMillis() - start) + " ms");
        }
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

        FieldIndexKey parser = new FieldIndexKey();
        parser.parse(key);
        return preNormalizedAttributeFactory.create(parser.getField(), parser.getValue(), key, parser.getDatatype(), true, false);
        // return new Content(value, docKey, true);
    }

    public void setLogStats(boolean logStats) {
        this.logStats = logStats;
    }
}
