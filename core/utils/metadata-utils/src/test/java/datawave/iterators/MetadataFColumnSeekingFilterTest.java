package datawave.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.ColumnFamilyConstants;
import datawave.util.time.DateHelper;

/**
 * Run the seeking filter through a series of tests for the following combinations
 * <ul>
 * <li>single field, multi field</li>
 * <li>empty datatypes, single datatype, multiple datatypes, non-existent datatypes</li>
 * <li>single day, date range, date range that falls outside the bounds of the data</li>
 * </ul>
 */
class MetadataFColumnSeekingFilterTest {
    
    private static final Logger log = LoggerFactory.getLogger(MetadataFColumnSeekingFilterTest.class);
    
    private static final String METADATA_TABLE_NAME = "DatawaveMetadata";
    private static final LongCombiner.VarLenEncoder encoder = new LongCombiner.VarLenEncoder();
    private static AccumuloClient client;
    
    private int expectedKeys = 0;
    private long expectedCount = 0;
    
    private Set<String> fields;
    private Set<String> datatypes;
    private String startDate;
    private String endDate;
    
    @BeforeAll
    public static void setup() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(MetadataFColumnSeekingFilterTest.class.getName());
        client = new InMemoryAccumuloClient("", instance);
        client.tableOperations().create(METADATA_TABLE_NAME);
        writeData();
    }
    
    private static void writeData() throws Exception {
        
        BatchWriterConfig config = new BatchWriterConfig();
        try (BatchWriter bw = client.createBatchWriter(METADATA_TABLE_NAME, config)) {
            
            Set<String> fields = Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D");
            Set<String> datatypes = Set.of("datatype-a", "datatype-b");
            
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTime(DateHelper.parse("20240501"));
            for (int i = 0; i < 15; i++) {
                for (String field : fields) {
                    for (String datatype : datatypes) {
                        String date = DateHelper.format(cal.getTime());
                        String cq = datatype + '\u0000' + date;
                        write(bw, field, "f", cq, createValue(i + 1));
                    }
                }
                cal.add(Calendar.DAY_OF_MONTH, 1);
            }
        }
    }
    
    private static void write(BatchWriter bw, String row, String cf, String cq, Value value) throws Exception {
        Mutation m = new Mutation(row);
        m.put(cf, cq, value);
        bw.addMutation(m);
    }
    
    private static Value createValue(long count) {
        return new Value(encoder.encode(count));
    }
    
    @BeforeEach
    public void beforeEach() {
        expectedCount = -1L;
        expectedKeys = -1;
        fields = null;
        datatypes = null;
        startDate = null;
        endDate = null;
    }
    
    @Test
    public void testSingleField_SingleDay_ZeroDatatype() throws Exception {
        withExpectedKeysAndCount(2, 10L);
        withFieldsAndDatatypes(Set.of("FIELD_A"), Set.of());
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testSingleField_SingleDay_SingleDatatype() throws Exception {
        withExpectedKeysAndCount(1, 5L);
        withFieldsAndDatatypes(Set.of("FIELD_A"), Set.of("datatype-a"));
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testSingleField_SingleDay_MultiDatatype() throws Exception {
        withExpectedKeysAndCount(2, 10L);
        withFieldsAndDatatypes(Set.of("FIELD_A"), Set.of("datatype-a", "datatype-b"));
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testSingleField_SingleDay_ExtraDatatype() throws Exception {
        withExpectedKeysAndCount(2, 10L);
        withFieldsAndDatatypes(Set.of("FIELD_A"), Set.of("datatype-a", "datatype-b", "datatype-c"));
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testSingleField_MultiDay_ZeroDatatype() throws Exception {
        withExpectedKeysAndCount(12, 90L);
        withFieldsAndDatatypes(Set.of("FIELD_A"), Set.of());
        withDates("20240505", "20240510");
        test();
    }
    
    @Test
    public void testSingleField_MultiDay_SingleDatatype() throws Exception {
        withExpectedKeysAndCount(6, 45L);
        withFieldsAndDatatypes(Set.of("FIELD_A"), Set.of("datatype-a"));
        withDates("20240505", "20240510");
        test();
    }
    
    @Test
    public void testSingleField_MultiDay_MultiDatatype() throws Exception {
        withExpectedKeysAndCount(12, 90L);
        withFieldsAndDatatypes(Set.of("FIELD_A"), Set.of("datatype-a", "datatype-b"));
        withDates("20240505", "20240510");
        test();
    }
    
    @Test
    public void testSingleField_MultiDay_ExtraDatatype() throws Exception {
        withExpectedKeysAndCount(12, 90L);
        withFieldsAndDatatypes(Set.of("FIELD_A"), Set.of("datatype-a", "datatype-b", "datatype-c"));
        withDates("20240505", "20240510");
        test();
    }
    
    @Test
    public void testMultiField_SingleDay_ZeroDatatype() throws Exception {
        withExpectedKeysAndCount(4, 20L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_B"), Set.of());
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testMultiField_SingleDay_SingleDatatype() throws Exception {
        withExpectedKeysAndCount(2, 10L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_B"), Set.of("datatype-a"));
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testMultiField_SingleDay_MultiDatatype() throws Exception {
        withExpectedKeysAndCount(4, 20L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_B"), Set.of("datatype-a", "datatype-b"));
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testMultiField_SingleDay_ExtraDatatype() throws Exception {
        withExpectedKeysAndCount(4, 20L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_B"), Set.of("datatype-a", "datatype-b", "datatype-c"));
        withDates("20240505", "20240505");
        test();
    }
    
    // multi field with a gap
    
    @Test
    public void testMultiFieldWithGap_SingleDay_ZeroDatatype() throws Exception {
        withExpectedKeysAndCount(4, 20L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_C"), Set.of());
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testMultiFieldWithGap_SingleDay_SingleDatatype() throws Exception {
        withExpectedKeysAndCount(2, 10L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_C"), Set.of("datatype-a"));
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testMultiFieldWithGap_SingleDay_MultiDatatype() throws Exception {
        withExpectedKeysAndCount(4, 20L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_C"), Set.of("datatype-a", "datatype-b"));
        withDates("20240505", "20240505");
        test();
    }
    
    @Test
    public void testMultiFieldGap_SingleDay_ExtraDatatype() throws Exception {
        withExpectedKeysAndCount(4, 20L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_C"), Set.of("datatype-a", "datatype-b", "datatype-c"));
        withDates("20240505", "20240505");
        test();
    }
    
    // tests for field that doesn't exist
    
    @Test
    public void testSingleFieldNoField_MultiDay_ZeroDatatype() throws Exception {
        withExpectedKeysAndCount(12, 90L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_Z"), Set.of());
        withDates("20240505", "20240510");
        test();
    }
    
    @Test
    public void testSingleFieldNoField_MultiDay_SingleDatatype() throws Exception {
        withExpectedKeysAndCount(6, 45L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_Z"), Set.of("datatype-a"));
        withDates("20240505", "20240510");
        test();
    }
    
    @Test
    public void testSingleFieldNoField_MultiDay_MultiDatatype() throws Exception {
        withExpectedKeysAndCount(12, 90L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_Z"), Set.of("datatype-a", "datatype-b"));
        withDates("20240505", "20240510");
        test();
    }
    
    @Test
    public void testSingleFieldNoField_MultiDay_ExtraDatatype() throws Exception {
        withExpectedKeysAndCount(12, 90L);
        withFieldsAndDatatypes(Set.of("FIELD_A", "FIELD_Z"), Set.of("datatype-a", "datatype-b", "datatype-c"));
        withDates("20240505", "20240510");
        test();
    }
    
    private void test() throws Exception {
        assertNotEquals(-1, expectedCount, "expected count must be non-negative");
        assertNotEquals(-1, expectedKeys, "expected keys must be non-negative");
        assertNotNull(fields, "fields must not be null");
        assertNotNull(datatypes, "datatypes must not be null");
        assertNotNull(startDate, "start date must be non-null");
        assertNotNull(endDate, "end date must be non-null");
        
        Set<Range> ranges = createExactFieldRanges();
        scanRanges(ranges);
        
        if (!datatypes.isEmpty()) {
            ranges = createDateBoundedRanges();
            scanRanges(ranges);
        }
    }
    
    private void scanRanges(Collection<Range> ranges) throws Exception {
        try (BatchScanner scanner = client.createBatchScanner(METADATA_TABLE_NAME)) {
            scanner.setRanges(ranges);
            scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            
            IteratorSetting setting = new IteratorSetting(50, "MetadataFColumnSeekingFilter", MetadataFColumnSeekingFilter.class);
            setting.addOption(MetadataFColumnSeekingFilter.DATATYPES_OPT, Joiner.on(',').join(datatypes));
            setting.addOption(MetadataFColumnSeekingFilter.START_DATE, startDate);
            setting.addOption(MetadataFColumnSeekingFilter.END_DATE, endDate);
            scanner.addScanIterator(setting);
            
            int keys = 0;
            int count = 0;
            for (Map.Entry<Key,Value> entry : scanner) {
                log.debug("tk: {}", entry.getKey());
                
                keys++;
                count += encoder.decode(entry.getValue().get());
            }
            
            assertEquals(expectedKeys, keys);
            assertEquals(expectedCount, count);
        }
    }
    
    private Set<Range> createExactFieldRanges() {
        Set<Range> ranges = new HashSet<>();
        for (String field : fields) {
            ranges.add(Range.exact(field));
        }
        return ranges;
    }
    
    private Set<Range> createDateBoundedRanges() {
        assertNotNull(datatypes, "cannot build date bounded ranges with null datatypes");
        assertFalse(datatypes.isEmpty(), "cannot build date bounded ranges with empty datatypes");
        
        Set<Range> ranges = new HashSet<>();
        TreeSet<String> sortedTypes = new TreeSet<>(datatypes);
        for (String field : fields) {
            // build a range bounded by both datatype and date
            // the MetadataFColumnSeekingFilter will handle any other datatypes that fall inside the bounds
            Key start = new Key(field, "f", sortedTypes.first() + '\u0000' + startDate);
            Key end = new Key(field, "f", sortedTypes.last() + '\u0000' + endDate + '\u0000');
            ranges.add(new Range(start, true, end, false));
        }
        return ranges;
    }
    
    private void withFieldsAndDatatypes(Set<String> fields, Set<String> datatypes) {
        this.fields = fields;
        this.datatypes = datatypes;
    }
    
    private void withDates(String startDate, String endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }
    
    private void withExpectedKeysAndCount(int expectedKeys, long expectedCount) {
        this.expectedKeys = expectedKeys;
        this.expectedCount = expectedCount;
    }
}
