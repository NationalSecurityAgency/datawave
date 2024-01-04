package datawave.experimental.scanner.event;

import java.util.Map;

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
                field = parser.getField();
                attr = attributeFactory.create(parser.getField(), parser.getValue(), entry.getKey(), true);
                d.put(field, attr);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("exception while fetching document " + datatypeUid + ", error was: " + e.getMessage());
        }
        try {
            d.put(Document.DOCKEY_FIELD_NAME, new DocumentKey(new Key(range.getStartKey().getRow(), new Text(datatypeUid)), true));
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("time to fetch event " + datatypeUid + " was " + (System.currentTimeMillis() - start) + " ms");
        return d;
    }

    /**
     * Rebuild a range into a document range
     *
     * @param range
     *            the range
     * @param datatypeUid
     *            a datatype and uid
     * @return
     */
    private Range rebuildRange(Range range, String datatypeUid) {
        Key start = new Key(range.getStartKey().getRow(), new Text(datatypeUid));
        Key end = start.followingKey(PartialKey.ROW_COLFAM); // TODO -- switch on tld
        return new Range(start, true, end, false);
    }
}
