package datawave.experimental.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.List;
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
import org.checkerframework.checker.units.qual.K;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.experimental.util.AccumuloUtil;
import datawave.query.data.parsers.TermFrequencyKey;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.postprocessing.tf.TermOffsetPopulator;
import datawave.util.TableName;

class TermFrequencyScanIteratorTest {

    protected static AccumuloUtil util;

    private final TermFrequencyKey parser = new TermFrequencyKey();

    @BeforeAll
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(DocumentScanIteratorTest.class.getSimpleName());
        util.loadData();
    }

    @Test
    void testPhrase() {
        String query = "content:phrase(TOK, termOffsetMap, 'the', 'message') && TOK == 'the' && TOK == 'message'";
        SortedSet<String> uids = new TreeSet<>(Set.of(util.getUid1(), util.getUid2(), util.getUid3()));
        TreeMultimap<String,String> fieldValues = TreeMultimap.create();
        fieldValues.putAll("TOK", List.of("the", "message"));
        test(query, uids, fieldValues);
    }

    private void test(String query, SortedSet<String> uids, TreeMultimap<String,String> expectedFieldValues) {
        for (String uid : uids) {
            test(query, uid, expectedFieldValues);
            test(query, uid, expectedFieldValues, true);
        }
    }

    private void test(String query, String uid, TreeMultimap<String,String> expectedFieldValues) {
        test(query, uid, expectedFieldValues, false);
    }

    private void test(String query, String uid, TreeMultimap<String,String> expectedFieldValues, boolean seekingAggregation) {

        try {
            IteratorSetting setting = new IteratorSetting(100, TermFrequencyScanIterator.class);
            setting.addOption(TermFrequencyScanIterator.FIELD_VALUES, TermFrequencyScanIterator.serializeFieldValue(expectedFieldValues));
            if (seekingAggregation) {
                setting.addOption(TermFrequencyScanIterator.MODE, "seek");
            }

            AccumuloClient client = util.getClient();
            Multimap<String,String> scannedFieldValues = HashMultimap.create();

            long start = System.currentTimeMillis();
            try (Scanner scanner = client.createScanner(TableName.SHARD, util.getAuths())) {
                scanner.addScanIterator(setting);
                scanner.setRange(getRange(uid));

                for (Map.Entry<Key,Value> entry : scanner) {
                    parser.parse(entry.getKey());
                    scannedFieldValues.put(parser.getField(), parser.getValue());
                }

                long elapsed = System.currentTimeMillis() - start;
                System.out.println("elapsed: " + elapsed + " ms");
                assertEquals(expectedFieldValues, scannedFieldValues);
            }
        } catch (TableNotFoundException e) {
            fail("Failed to create scanner", e);
        }
    }

    private Range getRange(String uid) {
        Key start = new Key("20201212_0", "tf", "dt\0" + uid);
        Key stop = start.followingKey(PartialKey.ROW_COLFAM);
        return new Range(start, true, stop, false);
    }
}
