package datawave.experimental.scanner.event;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

import datawave.experimental.iterators.DocumentScanIterator;
import datawave.experimental.util.ScanStats;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.util.TypeMetadata;

public class ConfiguredEventScanner extends AbstractEventScanner {
    private static final Logger log = Logger.getLogger(ConfiguredEventScanner.class);

    private Set<String> includeFields;
    private Set<String> excludeFields;
    private TypeMetadata typeMetadata;

    private boolean logStats;

    private final ScanStats scanStats = new ScanStats();
    private final KryoDocumentDeserializer deser = new KryoDocumentDeserializer();

    public ConfiguredEventScanner(String tableName, Authorizations auths, AccumuloClient client, AttributeFactory attributeFactory) {
        super(tableName, auths, client, attributeFactory);
    }

    public Document fetchDocument(Range range, String datatypeUid) {
        long start = System.currentTimeMillis();
        Document d = new Document();
        try (Scanner scanner = client.createScanner(tableName, auths)) {
            Range documentRange = rebuildRange(range, datatypeUid);
            scanner.setRange(documentRange);
            // this may break tld
            // scanner.fetchColumnFamily(new Text(datatypeUid)); // uid is actually datatype\0uid

            IteratorSetting setting = new IteratorSetting(100, DocumentScanIterator.class);
            setting.addOption(DocumentScanIterator.UID_OPT, datatypeUid);
            if (includeFields != null && !includeFields.isEmpty()) {
                setting.addOption(DocumentScanIterator.INCLUDE_FIELDS, Joiner.on(',').join(includeFields));
            }
            if (excludeFields != null && !excludeFields.isEmpty()) {
                setting.addOption(DocumentScanIterator.EXCLUDE_FIELDS, Joiner.on(',').join(excludeFields));
            }
            setting.addOption(DocumentScanIterator.TYPE_METADATA, typeMetadata.toString());

            scanner.addScanIterator(setting);

            Map.Entry<Key,Value> entry = scanner.iterator().next();

            synchronized (deser) {
                d = deser.deserialize(new ByteArrayInputStream(entry.getValue().get()));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("exception while fetching document " + datatypeUid + ", error was: " + e.getMessage());
        }

        // add the record id
        try {
            d.put(Document.DOCKEY_FIELD_NAME, new DocumentKey(new Key(range.getStartKey().getRow(), new Text(datatypeUid)), true));
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
     * @return a document range
     */
    private Range rebuildRange(Range range, String datatypeUid) {
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

    public void setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }
}
