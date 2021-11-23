package datawave.experimental.scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import datawave.experimental.util.AccumuloUtil;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.util.MetadataHelper;
import datawave.query.util.TypeMetadata;
import datawave.util.TableName;

class EventScannerTest {

    protected static AccumuloUtil util;
    protected static MetadataHelper metadataHelper;
    protected static TypeMetadata typeMetadata;

    @BeforeAll
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(EventScannerTest.class.getSimpleName());
        util.loadData();

        metadataHelper = util.getMetadataHelper();
        typeMetadata = metadataHelper.getTypeMetadata();
    }

    @Test
    void testSimpleEq() {
        Set<String> uids = util.getAliceUids();
        test(uids);
    }

    private void test(Set<String> uids) {
        EventScanner scanner = getEventScanner();
        Range range = getRange();
        Set<String> expectedUids = transformUids(uids);
        Set<String> foundUids = new HashSet<>();
        for (String uid : expectedUids) {
            Document d = scanner.fetchDocument(range, uid);
            String dtUid = d.get("RECORD_ID").getMetadata().getColumnFamily().toString();
            foundUids.add(dtUid);
        }
        assertEquals(expectedUids, foundUids);
    }

    private EventScanner getEventScanner() {
        return new EventScanner(TableName.SHARD, util.getAuths(), util.getClient(), new AttributeFactory(typeMetadata));
    }

    private Range getRange() {
        Key start = new Key("20201212_0");
        Key end = new Key("20201212_0\uFFFF");
        return new Range(start, true, end, false);
    }

    private SortedSet<String> transformUids(Set<String> uids) {
        SortedSet<String> dtUids = new TreeSet<>();
        for (String uid : uids) {
            dtUids.add("dt\0" + uid);
        }
        return dtUids;
    }

}
