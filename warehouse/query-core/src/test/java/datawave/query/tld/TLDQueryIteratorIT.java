package datawave.query.tld;

import static datawave.query.iterator.QueryOptions.INDEXED_FIELDS;
import static datawave.query.iterator.QueryOptions.NON_INDEXED_DATATYPES;
import static datawave.query.iterator.QueryOptions.TERM_FREQUENCY_FIELDS;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

import datawave.query.Constants;
import datawave.query.iterator.QueryIteratorIT;
import datawave.query.iterator.QueryOptions;

/**
 * Anything QueryIterator does TLDQueryIterator should do too... plus stuff
 */
public class TLDQueryIteratorIT extends QueryIteratorIT {

    @Before
    public void setup() throws IOException {
        super.setup();
        iterator = new TLDQueryIterator();

        // update indexed
        options.put(INDEXED_FIELDS, options.get(INDEXED_FIELDS) + ",TF_FIELD3");
        options.put(TERM_FREQUENCY_FIELDS, options.get(TERM_FREQUENCY_FIELDS) + ",TF_FIELD3");

        // update unindexed fields
        String nonIndexedDataTypes = options.get(NON_INDEXED_DATATYPES);
        Map<String,Set<String>> nonIndexedQueryFieldsDatatypes = QueryOptions.buildFieldDataTypeMap(nonIndexedDataTypes);
        nonIndexedQueryFieldsDatatypes.put("EVENT_FIELD7", Collections.singleton("datawave.data.type.LcNoDiacriticsType"));
        nonIndexedDataTypes = QueryOptions.buildFieldNormalizerString(nonIndexedQueryFieldsDatatypes);
        options.put(NON_INDEXED_DATATYPES, nonIndexedDataTypes);

        // update type metadata
        typeMetadata.put("EVENT_FIELD7", "dataType1", "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("TF_FIELD3", "dataType1", "datawave.data.type.LcNoDiacriticsType");
    }

    @Override
    protected void configureIterator() {
        // configure iterator
        iterator.setEvaluationFilter(null);
        iterator.setTypeMetadata(typeMetadata);
    }

    @Test
    public void event_isNotNull_documentSpecific_tld_test() throws IOException {
        options.put(TERM_FREQUENCY_FIELDS, "TF_FIELD3");

        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");

        Map.Entry<Key,Map<String,List<String>>> expectedDocument = getBaseExpectedEvent("123.345.456");
        List<String> tfField1Hits = new ArrayList<>();
        tfField1Hits.add("z");
        expectedDocument.getValue().put("TF_FIELD3", tfField1Hits);

        event_test(seekRange, "EVENT_FIELD2 == 'b' && not(TF_FIELD3 == null)", false, expectedDocument, configureTLDTestData(11), Collections.emptyList());
    }

    @Test
    public void event_isNotNull_shardRange_tld_test() throws IOException {
        options.put(TERM_FREQUENCY_FIELDS, "TF_FIELD3");

        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange(null);

        Map.Entry<Key,Map<String,List<String>>> expectedDocument = getBaseExpectedEvent("123.345.456");
        List<String> tfField1Hits = new ArrayList<>();
        tfField1Hits.add("z");
        expectedDocument.getValue().put("TF_FIELD3", tfField1Hits);

        event_test(seekRange, "EVENT_FIELD2 == 'b' && not(TF_FIELD3 == null)", false, expectedDocument, configureTLDTestData(11), Collections.emptyList());
    }

    /**
     * Document specific range given expected from RangePartitioner and EVENT_FIELD1 index lookup. ExceededValueThreshold TF FIELD triggers TF index lookup for
     * evaluation.
     * <p>
     * document specific range, tf via TermFrequencyAggregator
     *
     * @throws IOException
     *             for issues with read/write
     */
    @Test
    public void tf_exceededValue_leadingWildcard_documentSpecific_tld_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*r'))";
        Map.Entry<Key,Map<String,List<String>>> expectedDocument = getBaseExpectedEvent("123.345.456");
        List<String> tfField1Hits = new ArrayList<>();
        tfField1Hits.add("a,, b,,, c,,");
        tfField1Hits.add("r");
        expectedDocument.getValue().put("TF_FIELD1", tfField1Hits);

        tf_test(seekRange, query, expectedDocument, configureTLDTestData(11), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_leadingWildcard_fullValueMatch_documentSpecific_tld_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*r s'))";
        Map.Entry<Key,Map<String,List<String>>> expectedDocument = getBaseExpectedEvent("123.345.456");
        List<String> tfField1Hits = new ArrayList<>();
        // from parent
        tfField1Hits.add("a,, b,,, c,,");
        // from child
        tfField1Hits.add(",,q ,r, ,s,");
        expectedDocument.getValue().put("TF_FIELD1", tfField1Hits);

        tf_test(seekRange, query, expectedDocument, configureTLDTestData(11), Collections.emptyList());
    }

    /**
     * Shard range given expected from RangePartitioner and EVENT_FIELD1 index lookup. ExceededValueThreshold TF FIELD triggers TF index lookup for evaluation.
     * <p>
     * fiFullySatisfies query, Create an ivarator for tf
     *
     * @throws IOException
     *             for issues with read/write
     */
    @Test
    public void tf_exceededValue_leadingWildcard_shardRange_tld_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*r'))";
        Map.Entry<Key,Map<String,List<String>>> expectedDocument = getBaseExpectedEvent("123.345.456");
        List<String> tfField1Hits = new ArrayList<>();
        tfField1Hits.add("a,, b,,, c,,");
        tfField1Hits.add("r");
        expectedDocument.getValue().put("TF_FIELD1", tfField1Hits);

        tf_test(seekRange, query, expectedDocument, configureTLDTestData(11), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_negated_leadingWildcard_documentSpecific_tld_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), configureTLDTestData(11), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_negated_leadingWildcard_shardRange_tld_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), configureTLDTestData(11), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ 'r.*'))";
        Map.Entry<Key,Map<String,List<String>>> expectedDocument = getBaseExpectedEvent("123.345.456");
        List<String> tfField1Hits = new ArrayList<>();
        tfField1Hits.add("a,, b,,, c,,");
        tfField1Hits.add("r");
        expectedDocument.getValue().put("TF_FIELD1", tfField1Hits);

        tf_test(seekRange, query, expectedDocument, configureTLDTestData(11), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ 'r.*'))";
        Map.Entry<Key,Map<String,List<String>>> expectedDocument = getBaseExpectedEvent("123.345.456");
        List<String> tfField1Hits = new ArrayList<>();
        tfField1Hits.add("a,, b,,, c,,");
        tfField1Hits.add("r");
        expectedDocument.getValue().put("TF_FIELD1", tfField1Hits);

        tf_test(seekRange, query, expectedDocument, configureTLDTestData(11), Collections.emptyList());
    }

    @Override
    protected Range getDocumentRange(String row, String dataType, String uid) {
        // not a document range
        if (uid == null) {
            // get the shard range from the super
            return super.getDocumentRange(row, dataType, null);
        }

        Key startKey = new Key(row, dataType + Constants.NULL + uid);
        Key endKey = new Key(row, dataType + Constants.NULL + uid + Constants.MAX_UNICODE_STRING);
        return new Range(startKey, true, endKey, true);
    }

    private List<Map.Entry<Key,Value>> configureTLDTestData(long eventTime) {
        List<Map.Entry<Key,Value>> listSource = super.configureTestData(eventTime);

        // add some indexed TF fields in a child
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("TF_FIELD1", ",,q ,r, ,s,", "123.345.456.1", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("TF_FIELD1", "q r s", "123.345.456.1", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("TF_FIELD1", "q", "123.345.456.1", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("TF_FIELD1", "r", "123.345.456.1", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("TF_FIELD1", "s", "123.345.456.1", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("TF_FIELD1", "q", "123.345.456.1", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("TF_FIELD1", "r", "123.345.456.1", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("TF_FIELD1", "s", "123.345.456.1", eventTime), EMPTY_VALUE));

        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("TF_FIELD2", ",d, ,e, ,f,", "123.345.456.2", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("TF_FIELD2", "d e f", "123.345.456.2", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("TF_FIELD2", "d", "123.345.456.2", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("TF_FIELD2", "e", "123.345.456.2", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("TF_FIELD2", "f", "123.345.456.2", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("TF_FIELD2", "d", "123.345.456.2", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("TF_FIELD2", "e", "123.345.456.2", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("TF_FIELD2", "f", "123.345.456.2", eventTime), EMPTY_VALUE));

        // add some event data for children
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("EVENT_FIELD7", "1", "123.345.456.1", eventTime), EMPTY_VALUE));

        // add some non-event data that is unique for children
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("TF_FIELD3", "z", "123.345.456.2", eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("TF_FIELD3", "z", "123.345.456.2", eventTime), EMPTY_VALUE));

        return listSource;
    }
}
