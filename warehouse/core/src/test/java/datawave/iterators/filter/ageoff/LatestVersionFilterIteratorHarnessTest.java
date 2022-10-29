package datawave.iterators.filter.ageoff;

import datawave.iterators.test.StubbedIteratorEnvironment;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.iteratortest.IteratorTestCaseFinder;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.junit4.BaseJUnit4IteratorTest;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.junit.runners.Parameterized.Parameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertTrue;

public class LatestVersionFilterIteratorHarnessTest extends BaseJUnit4IteratorTest {
    
    // the same scope is used for all the tests
    private static final IteratorUtil.IteratorScope TEST_SCOPE = IteratorUtil.IteratorScope.majc;
    private static final Map<String,String> ITERATOR_OPTIONS = createVersionFilterIteratorOptions();
    
    // Test datatype names. Each are configured differently in ITERATOR_OPTIONS. Expected filter behavior described inline
    private static final String DATATYPE_GTE_100 = "abcdefghijklmn"; // should accept keys greater than or equal to 100
    private static final String DATATYPE_EQ_NEG100 = "jkl"; // should accept keys equal to negative 100
    private static final String DATATYPE_UNDEFINED_MODE = "x"; // undefined mode: should accept all keys for majc scope
    private static final String DATATYPE_MISSING_ALL_CONFIGS = "efg"; // undefined mode and timestamp: should accept all keys for majc scope
    private static final String UNKNOWN_DATATYPE = "???"; // datatype not in configured list, should accept all keys
    
    /*
     * Based on Accumulo's iterator test harness guidance in BaseJUnit4IteratorTest
     */
    @Parameters
    public static Object[][] parameter() {
        IteratorTestInput input = createIteratorInput();
        IteratorTestOutput output = createIteratorOutput();
        List<IteratorTestCase> tests = IteratorTestCaseFinder.findAllTestCases();
        
        assertTrue(INPUT_DATA.size() > OUTPUT_DATA.size());
        assertTrue(OUTPUT_DATA.size() > 0);
        
        return BaseJUnit4IteratorTest.createParameters(input, output, tests);
    }
    
    private static final SortedMap<Key,Value> INPUT_DATA = createInputData();
    private static final SortedMap<Key,Value> OUTPUT_DATA = createOutputData();
    
    private static SortedMap<Key,Value> createInputData() {
        SortedMap<Key,Value> data = new TreeMap<>();
        data.putAll(generateAcceptableData());
        data.putAll(generateRejectableData());
        return data;
    }
    
    /*
     * Returns a SortedMap containing data that is expected to all be kept
     */
    private static SortedMap<Key,Value> generateAcceptableData() {
        TreeMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("rowA", DATATYPE_GTE_100 + "\000123456789", "cq", 100L), new Value("ignore1".getBytes()));
        data.put(new Key("rowB", DATATYPE_GTE_100 + "\000123456789", "cq", 123L), new Value("ignore2".getBytes()));
        data.put(new Key("rowB", DATATYPE_EQ_NEG100 + "\000123456789", "cq", -100L), new Value("ignore3".getBytes()));
        data.put(new Key("rowB", DATATYPE_UNDEFINED_MODE + "\000123456789", "cq", Long.MIN_VALUE), new Value("ignore4".getBytes()));
        data.put(new Key("rowC", DATATYPE_UNDEFINED_MODE + "\000123456789", "cq", 0L), new Value("ignore5".getBytes()));
        data.put(new Key("rowC", DATATYPE_UNDEFINED_MODE + "\000123456789", "cq", Long.MAX_VALUE), new Value("ignore6".getBytes()));
        data.put(new Key("rowC", DATATYPE_MISSING_ALL_CONFIGS + "\000123456789", "cq", Long.MIN_VALUE), new Value("ignore7".getBytes()));
        data.put(new Key("rowD", DATATYPE_MISSING_ALL_CONFIGS + "\000123456789", "cq", 0L), new Value("ignore8".getBytes()));
        data.put(new Key("rowD", DATATYPE_MISSING_ALL_CONFIGS + "\000123456789", "cq", Long.MAX_VALUE), new Value("ignore9".getBytes()));
        data.put(new Key("rowE", UNKNOWN_DATATYPE + "\000123456789", "cq", Long.MAX_VALUE), new Value("ignorf0".getBytes()));
        return data;
    }
    
    /*
     * Returns a SortedMap containing data that is all expected to be rejected
     */
    private static SortedMap<Key,Value> generateRejectableData() {
        TreeMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("rowA", DATATYPE_GTE_100 + "\000123456789", "cq", 99L), new Value("ignorf1".getBytes()));
        data.put(new Key("rowB", DATATYPE_EQ_NEG100 + "\000123456789", "cq", -99L), new Value("ignorf2".getBytes()));
        data.put(new Key("rowB", DATATYPE_EQ_NEG100 + "\000123456789", "cq", -101L), new Value("ignorf3".getBytes()));
        data.put(new Key("rowC", DATATYPE_EQ_NEG100 + "\000123456789", "cq", 100L), new Value("ignorf4".getBytes()));
        return data;
    }
    
    private static SortedMap<Key,Value> createOutputData() {
        return generateAcceptableData(); // don't include Rejectable data (should have been filtered out)
    }
    
    /*
     * Iterator options for testing
     */
    public static Map<String,String> createVersionFilterIteratorOptions() {
        Map<String,String> iteratorOptions = new HashMap<>();
        
        // define four datatypes
        iteratorOptions.put("dataTypes", DATATYPE_GTE_100 + "," + DATATYPE_UNDEFINED_MODE + "," + DATATYPE_MISSING_ALL_CONFIGS + "," + DATATYPE_EQ_NEG100);
        
        // define timestamps for some datatypes
        iteratorOptions.put("table.custom.timestamp.current." + DATATYPE_GTE_100, "100");
        iteratorOptions.put("table.custom.timestamp.current." + DATATYPE_EQ_NEG100, "-100");
        iteratorOptions.put("table.custom.timestamp.current." + DATATYPE_UNDEFINED_MODE, "50");
        // DATATYPE_MISSING_ALL_CONFIGS deliberately missing timestamp
        
        // define comparison mode for some datatypes
        iteratorOptions.put(DATATYPE_GTE_100 + ".mode", "gte");
        iteratorOptions.put(DATATYPE_EQ_NEG100 + ".mode", "eq");
        // DATATYPE_UNDEFINED_MODE and DATATYPE_MISSING_ALL_CONFIGS deliberately missing mode
        
        return iteratorOptions;
    }
    
    private static IteratorTestInput createIteratorInput() {
        return new IteratorTestInput(LatestVersionFilter.class, ITERATOR_OPTIONS, new Range(), INPUT_DATA, new StubbedIteratorEnvironment() {
            @Override
            public IteratorUtil.IteratorScope getIteratorScope() {
                return TEST_SCOPE;
            }
        });
    }
    
    private static IteratorTestOutput createIteratorOutput() {
        return new IteratorTestOutput(OUTPUT_DATA);
    }
    
    public LatestVersionFilterIteratorHarnessTest(IteratorTestInput input, IteratorTestOutput expectedOutput, IteratorTestCase testCase) {
        super(input, expectedOutput, testCase);
    }
}
