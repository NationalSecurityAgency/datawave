package datawave.query.iterator;

import static datawave.query.iterator.QueryOptions.DOC_AGGREGATION_THRESHOLD_MS;
import static datawave.query.iterator.QueryOptions.EVENT_FIELD_SEEK;
import static datawave.query.iterator.QueryOptions.EVENT_NEXT_SEEK;
import static datawave.query.iterator.QueryOptions.FI_FIELD_SEEK;
import static datawave.query.iterator.QueryOptions.FI_NEXT_SEEK;
import static datawave.query.iterator.QueryOptions.QUERY;
import static datawave.query.iterator.QueryOptions.SEEKING_EVENT_AGGREGATION;
import static datawave.query.iterator.QueryOptions.TERM_FREQUENCY_AGGREGATION_THRESHOLD_MS;
import static datawave.query.iterator.QueryOptions.TF_FIELD_SEEK;
import static datawave.query.iterator.QueryOptions.TF_NEXT_SEEK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.query.function.Equality;
import datawave.query.function.PrefixEquality;
import datawave.query.iterator.filter.EntryKeyIdentity;
import datawave.query.iterator.filter.FieldIndexKeyDataTypeFilter;

public class QueryOptionsTest {

    @BeforeClass
    public static void setupClass() {
        Logger.getLogger(QueryOptions.class).setLevel(Level.TRACE);
    }

    @Test
    public void testBuildFieldDataTypeMapFromSingleValueString() {
        String singleValueData = "k:v;";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        expectedDataTypeMap.put("k", Sets.newHashSet("v"));
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(singleValueData);
        assertEquals("Failed to parse single value option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testBuildFieldDataTypeMapFromMultiValueString() {
        String multiValueData = "k:v;key:value";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        expectedDataTypeMap.put("k", Sets.newHashSet("v"));
        expectedDataTypeMap.put("key", Sets.newHashSet("value"));
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(multiValueData);
        assertEquals("Failed to parse multi-value option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testBuildFieldDataTypeMapFromEmptyString() {
        String emptyData = "";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(emptyData);
        assertEquals("Failed to parse empty option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testBuildFieldDataTypeMapFromNullString() {
        String nulldata = null;
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(nulldata);
        assertEquals("Failed to parse null option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testBuildFieldDataTypeMapFromBadString() {
        String badData = "k:k2:k3:v;";
        Map<String,Set<String>> expectedDataTypeMap = new HashMap<>();
        Map<String,Set<String>> fieldDataTypeMap = QueryOptions.buildFieldDataTypeMap(badData);
        assertEquals("Failed to parse bad option string", expectedDataTypeMap, fieldDataTypeMap);
    }

    @Test
    public void testFetchDataTypeKeysFromSingleValueString() {
        String data = "k:v;";
        Set<String> expectedDataTypeKeys = Sets.newHashSet("k");
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse single value option string", expectedDataTypeKeys, dataTypeKeys);
    }

    @Test
    public void testFetchDataTypeKeysFromMultiValueString() {
        String data = "k:v;key:value";
        Set<String> expectedDataTypeKeys = Sets.newHashSet("k", "key");
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse multi-value option string", expectedDataTypeKeys, dataTypeKeys);
    }

    @Test
    public void testFetchDataTypeKeysFromEmptyString() {
        String data = "";
        Set<String> expectedDataTypeKeys = Sets.newHashSet();
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse empty option string", expectedDataTypeKeys, dataTypeKeys);
    }

    @Test
    public void testFetchDataTypeKeysFromNullString() {
        String data = null;
        Set<String> expectedDataTypeKeys = Sets.newHashSet();
        Set<String> dataTypeKeys = QueryOptions.fetchDataTypeKeys(data);
        assertEquals("Failed to parse null option string", expectedDataTypeKeys, dataTypeKeys);
    }

    @Test
    public void testSeekingConfiguration() {
        Map<String,String> optionsMap = new HashMap<>();
        optionsMap.put(QUERY, "set to avoid early return");
        optionsMap.put(FI_FIELD_SEEK, "10");
        optionsMap.put(FI_NEXT_SEEK, "11");
        optionsMap.put(EVENT_FIELD_SEEK, "12");
        optionsMap.put(EVENT_NEXT_SEEK, "13");
        optionsMap.put(TF_FIELD_SEEK, "14");
        optionsMap.put(TF_NEXT_SEEK, "15");
        optionsMap.put(SEEKING_EVENT_AGGREGATION, "true");

        QueryOptions options = new QueryOptions();

        // initial state
        assertEquals(-1, options.getFiFieldSeek());
        assertEquals(-1, options.getFiNextSeek());
        assertEquals(-1, options.getEventFieldSeek());
        assertEquals(-1, options.getEventNextSeek());
        assertEquals(-1, options.getTfFieldSeek());
        assertEquals(-1, options.getTfNextSeek());
        assertFalse(options.isSeekingEventAggregation());

        options.validateOptions(optionsMap);

        // expected state
        assertEquals(10, options.getFiFieldSeek());
        assertEquals(11, options.getFiNextSeek());
        assertEquals(12, options.getEventFieldSeek());
        assertEquals(13, options.getEventNextSeek());
        assertEquals(14, options.getTfFieldSeek());
        assertEquals(15, options.getTfNextSeek());
        assertTrue(options.isSeekingEventAggregation());
    }

    @Test
    public void testDocumentAndTermOffsetAggregationThresholds() {
        Map<String,String> optionsMap = new HashMap<>();
        optionsMap.put(QUERY, "query option required to validate");
        optionsMap.put(DOC_AGGREGATION_THRESHOLD_MS, "15000");
        optionsMap.put(TERM_FREQUENCY_AGGREGATION_THRESHOLD_MS, "10000");

        QueryOptions options = new QueryOptions();

        // initial state
        assertEquals(-1, options.getDocAggregationThresholdMs());
        assertEquals(-1, options.getTfAggregationThresholdMs());

        options.validateOptions(optionsMap);

        // expected state
        assertEquals(15000, options.getDocAggregationThresholdMs());
        assertEquals(10000, options.getTfAggregationThresholdMs());
    }

    @Test
    public void testGetEquality() {
        QueryOptions options = new QueryOptions();
        Equality equality = options.getEquality();
        assertEquals(PrefixEquality.class.getSimpleName(), equality.getClass().getSimpleName());
    }

    @Test
    public void testSimple() {
        Map<String,Set<String>> expected = new HashMap<>();

        expected.put("FIELD1", new HashSet<>(Arrays.asList("norm1", "norm2")));
        expected.put("FIELD2", new HashSet<>(Arrays.asList("norm1")));
        expected.put("FIELD3", new HashSet<>(Arrays.asList("norm1")));
        expected.put("FIELD4", new HashSet<>(Arrays.asList("norm1")));

        String data = QueryOptions.buildFieldNormalizerString(expected);
        Map<String,Set<String>> actual = QueryOptions.buildFieldDataTypeMap(data);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSingleField() {
        Map<String,Set<String>> expected = new HashMap<>();

        expected.put("FIELD1", new HashSet<>(Arrays.asList("norm1")));

        String data = QueryOptions.buildFieldNormalizerString(expected);
        Map<String,Set<String>> actual = QueryOptions.buildFieldDataTypeMap(data);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testEmptyMap() {
        Map<String,Set<String>> expected = new HashMap<>();

        String data = QueryOptions.buildFieldNormalizerString(expected);
        Map<String,Set<String>> actual = QueryOptions.buildFieldDataTypeMap(data);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNoDataTypeFilter() {
        Map<String,String> noOpts = Collections.emptyMap();
        QueryOptions opts = new QueryOptions();
        opts.validateOptions(noOpts);
        Assert.assertSame(EntryKeyIdentity.Function, opts.getFieldIndexKeyDataTypeFilter());
    }

    @Test
    public void testDataTypeFilter() {
        Map<String,String> opts = Maps.newHashMap();
        opts.put(QueryOptions.DATATYPE_FILTER, "foo,bar,meow");
        opts.put(QueryOptions.DISABLE_EVALUATION, "true");
        QueryOptions qopts = new QueryOptions();
        qopts.validateOptions(opts);
        Assert.assertTrue(qopts.getFieldIndexKeyDataTypeFilter() instanceof FieldIndexKeyDataTypeFilter);
    }

    @Test
    public void testLargeOptionsBuffer() throws IOException {
        Random rand = new Random(System.currentTimeMillis());
        int maxSize = 2522222;
        byte[] buffer = new byte[maxSize];
        for (int i = 0; i < maxSize; i++) {
            buffer[i] = (byte) (rand.nextInt(25) + 97);
        }
        String initialString = new String(buffer);
        String compressedOptions = QueryOptions.compressOption(initialString, QueryOptions.UTF8);
        String decompressOptions = WrappedQueryOptions.decompressOption(compressedOptions, QueryOptions.UTF8);

        Assert.assertEquals(initialString.length(), decompressOptions.length());
        Assert.assertEquals(initialString, decompressOptions);
    }

    private static class WrappedQueryOptions extends QueryOptions {
        protected static String decompressOption(final String buffer, Charset characterSet) throws IOException {
            return QueryOptions.decompressOption(buffer, characterSet);
        }
    }

}
