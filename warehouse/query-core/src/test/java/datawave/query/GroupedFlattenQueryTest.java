package datawave.query;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GT_OP;
import static datawave.query.testframework.RawDataManager.LT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

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

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import datawave.data.normalizer.Normalizer;
import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
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

/**
 * Test cases for flatten mode {@link FlattenMode@GROUPED}.
 */
public class GroupedFlattenQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(GroupedFlattenQueryTest.class);

    private static final FlattenMode flatMode = FlattenMode.GROUPED;
    private static final FlattenDataType flatten;
    private static final RawDataManager manager;

    static {
        FieldConfig indexes = new GroupedIndexing();
        FlattenData data = new FlattenData(GroupedField.STARTDATE.name(), GroupedField.EVENTID.name(), flatMode, GroupedField.headers,
                        GroupedField.metadataMapping);
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
        client = accumuloSetup.loadTables(log);
    }

    public GroupedFlattenQueryTest() {
        super(manager);
    }

    @Test
    public void testState() throws Exception {
        log.info("------  testState  ------");
        String state = "'texas'";
        String query = GroupedField.STATE + EQ_OP + state;
        runTest(query, query);
    }

    @Test
    public void testCity() throws Exception {
        log.info("------  testCity  ------");
        String city = "'auStin'";
        String query = GroupedField.CITY.name() + EQ_OP + city;
        runTest(query, query);
    }

    @Test
    public void testCityOrState() throws Exception {
        log.info("------  testCityOrState  ------");
        String city = "'auStin'";
        String state = "'KansAs'";
        String query = GroupedField.CITY.name() + EQ_OP + city + OR_OP + GroupedField.STATE + EQ_OP + state;
        runTest(query, query);
    }

    @Test
    public void testCityAndState() throws Exception {
        log.info("------  testCityAndState  ------");
        String city = "'portLAnd'";
        String state = "'KansAs'";
        String query = GroupedField.CITY.name() + EQ_OP + city + AND_OP + GroupedField.STATE + EQ_OP + state;
        runTest(query, query);
    }

    @Test
    public void testCounty() throws Exception {
        log.info("------  testCounty  ------");
        String county = "'marion'";
        String query = GroupedField.COUNTIES.name() + EQ_OP + county;
        runTest(query, query);
    }

    @Test
    public void testFoundedRangeUnbounded() throws Exception {
        log.info("------  testFoundedRange  ------");
        String start = "1850";
        String end = "1860";
        String city = "'AuStiN'";
        String query = GroupedField.CITY.name() + EQ_OP + city + AND_OP + GroupedField.FOUNDED.name() + GT_OP + start + AND_OP + GroupedField.FOUNDED.name()
                        + LT_OP + end;
        // all entries have at least one founded less than end and one founded greater than start, just not the same value.
        String expectquery = GroupedField.CITY.name() + EQ_OP + city;
        runTest(query, expectquery);
    }

    @Test
    public void testFoundedRangeBounded() throws Exception {
        log.info("------  testFoundedRange  ------");
        String start = "1850";
        String end = "1860";
        String city = "'AuStiN'";
        String query = GroupedField.CITY.name() + EQ_OP + city + AND_OP + "((_Bounded_ = true) && (" + GroupedField.FOUNDED.name() + GT_OP + start + AND_OP
                        + GroupedField.FOUNDED.name() + LT_OP + end + "))";
        runTest(query, query);
    }

    @Test
    public void testFounded() throws Exception {
        log.info("------  testFounded  ------");
        String date = "1854";
        String query = GroupedField.FOUNDED.name() + EQ_OP + date;
        runTest(query, query);
    }

    // end of unit tests
    // ============================================

    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = FlattenDataType.getTestAuths();
        this.documentKey = GroupedField.EVENTID.name();
    }

    private enum GroupedField {
        // include base fields - name must match
        // since enum cannot be extends - replicate these entries
        STARTDATE(FlattenBaseFields.STARTDATE.getNormalizer()),
        EVENTID(FlattenBaseFields.EVENTID.getNormalizer()),
        STATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        // mode specific fields
        CITY(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        FOUNDED(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        COUNTIES(Normalizer.LC_NO_DIACRITICS_NORMALIZER);

        static final List<String> headers;

        static {
            headers = Stream.of(GroupedField.values()).map(e -> e.name()).collect(Collectors.toList());
        }

        static final Map<String,RawMetaData> metadataMapping = new HashMap<>();

        static {
            for (GroupedField field : GroupedField.values()) {
                RawMetaData data = new RawMetaData(field.name(), field.normalizer, false);
                metadataMapping.put(field.name().toLowerCase(), data);
            }
        }

        private final Normalizer<?> normalizer;

        GroupedField(Normalizer<?> norm) {
            this.normalizer = norm;
        }
    }

    private static class GroupedIndexing extends AbstractFields {

        private static final Collection<String> index = new HashSet<>();
        private static final Collection<String> indexOnly = new HashSet<>();
        private static final Collection<String> reverse = new HashSet<>();
        private static final Collection<String> multivalue = new HashSet<>();

        private static final Collection<Set<String>> composite = new HashSet<>();
        private static final Collection<Set<String>> virtual = new HashSet<>();

        static {
            // set index configuration values
            index.add(GroupedField.STATE.name());
            index.add(GroupedField.CITY.name());
            index.add(GroupedField.COUNTIES.name());
            index.add(GroupedField.FOUNDED.name());
            reverse.addAll(index);
        }

        GroupedIndexing() {
            super(index, indexOnly, reverse, multivalue, composite, virtual);
        }

        @Override
        public String toString() {
            return "GroupedIndexing{" + super.toString() + "}";
        }
    }
}
