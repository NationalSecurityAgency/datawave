package datawave.query;

import datawave.data.normalizer.Normalizer;
import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFields;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.FlattenData;
import datawave.query.testframework.FlattenDataType;
import datawave.query.testframework.FlattenDataType.FlattenBaseFields;
import datawave.query.testframework.RawDataManager;
import datawave.query.testframework.RawMetaData;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GT_OP;
import static datawave.query.testframework.RawDataManager.LT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

/**
 * Test cases for flatten mode {@link FlattenMode@NORMAL}.
 */
public class NormalFlattenQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(NormalFlattenQueryTest.class);
    
    protected static final FlattenMode flatMode = FlattenMode.NORMAL;
    protected static final FlattenDataType flatten;
    protected static final RawDataManager manager;
    
    static {
        FieldConfig indexes = new NormalIndexing();
        FlattenData data = new FlattenData(NormalField.STARTDATE.name(), NormalField.EVENTID.name(), flatMode, NormalField.headers, NormalField.metadataMapping);
        manager = FlattenDataType.getManager(data);
        try {
            flatten = new FlattenDataType(FlattenDataType.FlattenEntry.cityFlatten, indexes, data);
        } catch (IOException | URISyntaxException e) {
            throw new AssertionError(e);
        }
    }
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        accumuloSetup.setData(FileType.JSON, flatten);
        connector = accumuloSetup.loadTables(log);
    }
    
    public NormalFlattenQueryTest() {
        super(manager);
    }
    
    @Test
    public void testState() throws Exception {
        String state = "'texas'";
        String query = NormalField.STATE + EQ_OP + state;
        runTest(query, query);
    }
    
    @Test
    public void testCity() throws Exception {
        log.info("------  testCity  ------");
        String cap = "'saLEM'";
        String city = "'auStin'";
        String query = NormalField.CAPITAL_CITY.name() + EQ_OP + cap + AND_OP + NormalField.SMALL_CITY.name() + EQ_OP + city;
        runTest(query, query);
    }
    
    @Test
    public void testCityOrState() throws Exception {
        log.info("------  testCity  ------");
        String city = "'auStin'";
        String state = "'kAnsAs'";
        String query = NormalField.STATE + EQ_OP + state + OR_OP + NormalField.CAPITAL_CITY.name() + EQ_OP + city;
        runTest(query, query);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorCityOrState() throws Exception {
        log.info("------  testErrorCityOrState  ------");
        String city = "'auStin'";
        String state = "'KansAs'";
        String query = NormalField.STATE + EQ_OP + state + OR_OP + NormalField.SMALL_CITY.name() + EQ_OP + city;
        runTest(query, query);
    }
    
    @Test
    public void testCityAndState() throws Exception {
        log.info("------  testCityAndState  ------");
        String city = "'portLAnd'";
        String state = "'KansAs'";
        String query = NormalField.SMALL_CITY.name() + EQ_OP + city + AND_OP + NormalField.STATE + EQ_OP + state;
        runTest(query, query);
    }
    
    @Test
    public void testCounty() throws Exception {
        log.info("------  testCounty  ------");
        String county = "'marion'";
        String query = NormalField.CAPITAL_COUNTIES.name() + EQ_OP + county;
        runTest(query, query);
    }
    
    @Test
    public void testFoundedRange() throws Exception {
        log.info("------  testFoundedRange  ------");
        String start = "1840";
        String end = "1860";
        String query = "((_Bounded_ = true) && (" + NormalField.CAPITAL_FOUNDED.name() + GT_OP + start + AND_OP + NormalField.CAPITAL_FOUNDED.name() + LT_OP
                        + end + "))";
        runTest(query, query);
    }
    
    @Test
    public void testFounded() throws Exception {
        log.info("------  testFounded  ------");
        String date = "1854";
        String query = NormalField.CAPITAL_FOUNDED.name() + EQ_OP + date;
        runTest(query, query);
    }
    
    @Test
    public void testAnyCity() throws Exception {
        log.info("------  testAnyCity  ------");
        String any = EQ_OP + "'daLLas'";
        String query = Constants.ANY_FIELD + any;
        String expect = this.dataManager.convertAnyField(any);
        runTest(query, expect);
    }
    
    @Test
    public void testAnyState() throws Exception {
        log.info("------  testAnyState  ------");
        String any = EQ_OP + "'TexaS'";
        String query = Constants.ANY_FIELD + any;
        String expect = this.dataManager.convertAnyField(any);
        runTest(query, expect);
    }
    
    @Test
    public void testAnyCounty() throws Exception {
        log.info("------  testAnyCounty  ------");
        String any = EQ_OP + "'marion'";
        String query = Constants.ANY_FIELD + any;
        String expect = this.dataManager.convertAnyField(any);
        runTest(query, expect);
    }
    
    // end of unit tests
    // ============================================
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = FlattenDataType.getTestAuths();
        this.documentKey = NormalField.EVENTID.name();
    }
    
    private enum NormalField {
        // include base fields - name must match
        // since enum cannot be extends - replicate these entries
        STARTDATE(FlattenBaseFields.STARTDATE.getNormalizer()),
        EVENTID(FlattenBaseFields.EVENTID.getNormalizer()),
        STATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        // mode specific fields
        CAPITAL_CITY(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        CAPITAL_FOUNDED(Normalizer.NUMBER_NORMALIZER),
        CAPITAL_COUNTIES(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        LARGE_CITY(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        LARGE_FOUNDED(Normalizer.NUMBER_NORMALIZER),
        LARGE_COUNTIES(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        SMALL_CITY(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        SMALL_FOUNDED(Normalizer.NUMBER_NORMALIZER),
        SMALL_COUNTIES(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
        
        static final List<String> headers;
        
        static {
            headers = Stream.of(NormalField.values()).map(e -> e.name()).collect(Collectors.toList());
        }
        
        static final Map<String,RawMetaData> metadataMapping = new HashMap<>();
        
        static {
            for (NormalField field : NormalField.values()) {
                RawMetaData data = new RawMetaData(field.name(), field.normalizer, false);
                metadataMapping.put(field.name().toLowerCase(), data);
            }
        }
        
        private final Normalizer<?> normalizer;
        
        NormalField(Normalizer<?> norm) {
            this.normalizer = norm;
        }
    }
    
    private static class NormalIndexing extends AbstractFields {
        
        private static final Collection<String> index = new HashSet<>();
        private static final Collection<String> indexOnly = new HashSet<>();
        private static final Collection<String> reverse = new HashSet<>();
        private static final Collection<String> multivalue = new HashSet<>();
        
        private static final Collection<Set<String>> composite = new HashSet<>();
        private static final Collection<Set<String>> virtual = new HashSet<>();
        
        static {
            // set index configuration values
            index.add(NormalField.STATE.name());
            index.add(NormalField.CAPITAL_CITY.name());
            index.add(NormalField.CAPITAL_COUNTIES.name());
            index.add(NormalField.CAPITAL_FOUNDED.name());
            index.add(NormalField.LARGE_CITY.name());
            index.add(NormalField.LARGE_COUNTIES.name());
            index.add(NormalField.LARGE_FOUNDED.name());
            reverse.addAll(index);
        }
        
        NormalIndexing() {
            super(index, indexOnly, reverse, multivalue, composite, virtual);
        }
        
        @Override
        public String toString() {
            return "NormalIndexing{" + super.toString() + "}";
        }
    }
}
