package datawave.experimental.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;

import datawave.experimental.util.AccumuloUtil;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.util.TableName;

class DocumentScanIteratorTest {

    protected static AccumuloUtil util;

    private final KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();

    @BeforeAll
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(DocumentScanIteratorTest.class.getSimpleName());
        util.loadData();
    }

    @Test
    void testAliceUids() {
        Set<String> uids = util.getAliceUids();
        test(uids);
    }

    @Test
    void testEveUids() {
        Set<String> uids = util.getEveUids();
        test(uids);
    }

    @Test
    void testOberonUids() {
        Set<String> uids = util.getOberonUids();
        test(uids);
    }

    private void test(Set<String> uids) {
        try {
            TreeSet<String> sortedUids = getSortedUids(uids);
            String serializedTypeMetadata = util.getMetadataHelper().getTypeMetadata().toString();

            IteratorSetting setting = new IteratorSetting(100, DocumentScanIterator.class);
            setting.addOption(DocumentScanIterator.UID_OPT, Joiner.on(',').join(sortedUids));
            setting.addOption(DocumentScanIterator.TYPE_METADATA, serializedTypeMetadata);

            AccumuloClient client = util.getClient();
            try (Scanner scanner = client.createScanner(TableName.SHARD, util.getAuths())) {
                scanner.addScanIterator(setting);
                scanner.setRange(createRange());

                SortedSet<String> foundUids = new TreeSet<>();
                for (Map.Entry<Key,Value> entry : scanner) {
                    Document document = deserializer.deserialize(new ByteArrayInputStream(entry.getValue().get()));
                    String uid = document.get("RECORD_ID").getMetadata().getColumnFamily().toString();
                    foundUids.add(uid);
                }

                assertEquals(sortedUids, foundUids);
            }

        } catch (TableNotFoundException e) {
            fail("Failed to run test");
        }
    }

    // builds the datatype/uid
    private TreeSet<String> getSortedUids(Set<String> uids) {
        TreeSet<String> sortedSet = new TreeSet<>();
        for (String uid : uids) {
            sortedSet.add("dt\0" + uid);
        }
        return sortedSet;
    }

    private Range createRange() {
        Key start = new Key("20201212_0");
        Key end = start.followingKey(PartialKey.ROW);
        return new Range(start, true, end, false);
    }
}
