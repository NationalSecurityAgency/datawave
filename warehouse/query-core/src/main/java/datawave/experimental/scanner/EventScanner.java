package datawave.experimental.scanner;

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

public class EventScanner {
    private static final Logger log = Logger.getLogger(EventScanner.class);

    protected AccumuloClient client;
    protected Authorizations auths;
    protected String tableName;
    protected String scanId;

    private final AttributeFactory attributeFactory;
    private final ScanStats scanStats = new ScanStats();

    public EventScanner(String tableName, Authorizations auths, AccumuloClient client, AttributeFactory attributeFactory) {
        this.tableName = tableName;
        this.auths = auths;
        this.client = client;
        this.attributeFactory = attributeFactory;
    }

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
