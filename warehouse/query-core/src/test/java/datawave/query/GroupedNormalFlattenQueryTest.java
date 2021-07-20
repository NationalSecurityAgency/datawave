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
 * Test cases for flatten mode {@link FlattenMode@GROUPED_AND_NORMAL}.
 */
public class GroupedNormalFlattenQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(GroupedNormalFlattenQueryTest.class);
    
    private static final FlattenMode flatMode = FlattenMode.GROUPED_AND_NORMAL;
    private static final FlattenDataType flatten;
    private static final RawDataManager manager;
    
    static {
        FieldConfig indexes = new GroupedNormalIndexing();
        FlattenData data = new FlattenData(GroupedNormalField.STARTDATE.name(), GroupedNormalField.EVENTID.name(), flatMode, GroupedNormalField.headers,
                        GroupedNormalField.metadataMapping);
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
    
    public GroupedNormalFlattenQueryTest() {
        super(manager);
    }
    
    @Test
    public void testState() throws Exception {
        log.info("------  testState  ------");
        String state = "'texas'";
        String query = GroupedNormalField.STATE + EQ_OP + state;
        runTest(query, query);
    }
    
    @Test
    public void testCity() throws Exception {
        log.info("------  testCity  ------");
        String city = "'PorTLand'";
        String query = GroupedNormalField.LARGE_CITY.name() + EQ_OP + city;
        runTest(query, query);
    }
    
    @Test
    public void testCityOrState() throws Exception {
        log.info("------  testCityOrState  ------");
        String city = "'portLAND'";
        String state = "'OreGon'";
        String query = GroupedNormalField.STATE + EQ_OP + state + OR_OP + GroupedNormalField.LARGE_CITY.name() + EQ_OP + city;
        runTest(query, query);
    }
    
    @Test
    public void testCityAndState() throws Exception {
        log.info("------  testCityAndState  ------");
        String city = "'portLAnd'";
        String state = "'KansAs'";
        String query = GroupedNormalField.SMALL_CITY.name() + EQ_OP + city + AND_OP + GroupedNormalField.STATE + EQ_OP + state;
        runTest(query, query);
    }
    
    @Test
    public void testCounty() throws Exception {
        log.info("------  testCounty  ------");
        String county = "'clackamas'";
        String query = GroupedNormalField.LARGE_COUNTIES.name() + EQ_OP + county;
        runTest(query, query);
    }
    
    @Test
    public void testFoundedRange() throws Exception {
        log.info("------  testFoundedRange  ------");
        String start = "1840";
        String end = "1860";
        String query = "((_Bounded_ = true) && (" + GroupedNormalField.LARGE_FOUNDED.name() + GT_OP + start + AND_OP + GroupedNormalField.LARGE_FOUNDED.name()
                        + LT_OP + end + "))";
        runTest(query, query);
    }
    
    @Test
    public void testFounded() throws Exception {
        log.info("------  testFounded  ------");
        String date = "1856";
        String query = GroupedNormalField.LARGE_FOUNDED.name() + EQ_OP + date;
        runTest(query, query);
    }
    
    @Test
    public void testAnyCity() throws Exception {
        log.info("------  testAnyCity  ------");
        String any = EQ_OP + "'PortLand'";
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
        String any = EQ_OP + "'dentON'";
        String query = Constants.ANY_FIELD + any;
        String expect = this.dataManager.convertAnyField(any);
        runTest(query, expect);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorCity() throws Exception {
        log.info("------  testErrorCity  ------");
        String city = "'auSTin'";
        String query = GroupedNormalField.SMALL_CITY.name() + EQ_OP + city;
        runTest(query, query);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorFounded() throws Exception {
        log.info("------  testErrorFounded  ------");
        String date = "1888";
        String query = GroupedNormalField.SMALL_FOUNDED.name() + EQ_OP + date;
        runTest(query, query);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorCounty() throws Exception {
        log.info("------  testErrorCounty  ------");
        String county = "'gRant'";
        String query = GroupedNormalField.CAPITAL_COUNTIES.name() + EQ_OP + county;
        runTest(query, query);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorCityOrState() throws Exception {
        log.info("------  testErrorCityOrState  ------");
        String city = "'auStin'";
        String state = "'KansAs'";
        String query = GroupedNormalField.STATE + EQ_OP + state + OR_OP + GroupedNormalField.SMALL_CITY.name() + EQ_OP + city;
        runTest(query, query);
    }
    
    // end of unit tests
    // ============================================
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = FlattenDataType.getTestAuths();
        this.documentKey = GroupedNormalField.EVENTID.name();
    }
    
    private enum GroupedNormalField {
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
            headers = Stream.of(GroupedNormalField.values()).map(e -> e.name()).collect(Collectors.toList());
        }
        
        static final Map<String,RawMetaData> metadataMapping = new HashMap<>();
        
        static {
            for (GroupedNormalField field : GroupedNormalField.values()) {
                RawMetaData data = new RawMetaData(field.name(), field.normalizer, false);
                metadataMapping.put(field.name().toLowerCase(), data);
            }
        }
        
        private final Normalizer<?> normalizer;
        
        GroupedNormalField(Normalizer<?> norm) {
            this.normalizer = norm;
        }
    }
    
    private static class GroupedNormalIndexing extends AbstractFields {
        
        private static final Collection<String> index = new HashSet<>();
        private static final Collection<String> indexOnly = new HashSet<>();
        private static final Collection<String> reverse = new HashSet<>();
        private static final Collection<String> multivalue = new HashSet<>();
        
        private static final Collection<Set<String>> composite = new HashSet<>();
        private static final Collection<Set<String>> virtual = new HashSet<>();
        
        static {
            // set index configuration values
            index.add(GroupedNormalField.STATE.name());
            // only enable ssearch on LARGE
            index.add(GroupedNormalField.LARGE_CITY.name());
            index.add(GroupedNormalField.LARGE_COUNTIES.name());
            index.add(GroupedNormalField.LARGE_FOUNDED.name());
            reverse.addAll(index);
        }
        
        GroupedNormalIndexing() {
            super(index, indexOnly, reverse, multivalue, composite, virtual);
        }
        
        @Override
        public String toString() {
            return "GroupedNormalIndexing{" + super.toString() + "}";
        }
    }
}
