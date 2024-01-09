package datawave.experimental.scanner.event;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.experimental.util.ScanStats;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.data.parsers.EventKey;

/**
 * Fully remote event scanner
 */
public class DefaultEventScanner extends AbstractEventScanner {
    private static final Logger log = Logger.getLogger(DefaultEventScanner.class);

    private boolean logStats;

    private Set<String> includeFields = null;
    private Set<String> excludeFields = null;
    private final ScanStats scanStats = new ScanStats();

    public DefaultEventScanner(String tableName, Authorizations auths, AccumuloClient client, AttributeFactory attributeFactory) {
        super(tableName, auths, client, attributeFactory);
    }

    /**
     * Default implementation for event aggregation, fully remote, all keys traverse the network
     *
     * @param range
     *            the scan range
     * @param datatypeUid
     *            the document's datatype and uid
     * @return a document
     */
    @Override
    public Document fetchDocument(Range range, String datatypeUid) {
        long start = System.currentTimeMillis();
        Key key = null;
        Document d = new Document();
        EventKey parser = new EventKey();
        try (Scanner scanner = client.createScanner(tableName, auths)) {
            Range documentRange = rebuildRange(range, datatypeUid);
            scanner.setRange(documentRange);
            // this may break tld
            // scanner.fetchColumnFamily(new Text(datatypeUid)); // uid is actually datatype\0uid

            String field;
            Attribute<?> attr;
            for (Map.Entry<Key,Value> entry : scanner) {
                parser.parse(entry.getKey());
                if (key == null) {
                    key = parser.getKey();
                }
                field = parser.getField();
                if ((includeFields == null || includeFields.contains(field)) && (excludeFields == null || !excludeFields.contains(field))) {
                    attr = attributeFactory.create(parser.getField(), parser.getValue(), entry.getKey(), true);
                    attr.setToKeep(includeFields == null || includeFields.contains(field));
                    attr.setFromIndex(false);
                    d.put(field, attr);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("exception while fetching document " + datatypeUid + ", error was: " + e.getMessage());
        }
        try {
            // TODO -- this needs to aggregate the classification
            Key docKey = new Key(key.getRow(), new Text(parser.getDatatype() + '\u0000' + parser.getUid()), new Text(), key.getColumnVisibility(),
                            key.getTimestamp());
            d.put(Document.DOCKEY_FIELD_NAME, new DocumentKey(docKey, true));
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (logStats) {
            log.info("time to fetch event " + datatypeUid + " was " + (System.currentTimeMillis() - start) + " ms");
        }
        return d;
    }

    @Override
    public void setLogStats(boolean logStats) {
        this.logStats = logStats;
    }

    /**
     * Rebuild a range into a document range
     *
     * @param range
     *            the range
     * @param datatypeUid
     *            a datatype and uid
     * @return a scan range for a document
     */
    private Range rebuildRange(Range range, String datatypeUid) {
        // TODO -- build exact range using first/last field if include fields is set
        Key start = new Key(range.getStartKey().getRow(), new Text(datatypeUid));
        Key end = start.followingKey(PartialKey.ROW_COLFAM); // TODO -- switch on tld
        return new Range(start, true, end, false);
    }

    public void setIncludeFields(Set<String> includeFields) {
        this.includeFields = includeFields;
    }

    public void setExcludeFields(Set<String> excludeFields) {
        this.excludeFields = excludeFields;
    }
}
