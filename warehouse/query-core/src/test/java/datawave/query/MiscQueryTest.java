package datawave.query;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GTE_OP;
import static datawave.query.testframework.RawDataManager.LTE_OP;
import static datawave.query.testframework.RawDataManager.NE_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.JexlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.BaseShardIdRange;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.QueryLogicTestHarness;
import datawave.query.testframework.ShardIdValues;

public class MiscQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = LogManager.getLogger(MiscQueryTest.class);

    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        Set<String> virt = new HashSet<>(Arrays.asList(CityField.CITY.name(), CityField.CONTINENT.name()));
        generic.removeVirtualField(virt);
        generic.addIndexField(CityField.CODE.name());
        for (String idx : generic.getIndexFields()) {
            generic.addReverseIndexField(idx);
        }
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));

        accumuloSetup.setData(FileType.CSV, dataTypes);
        client = accumuloSetup.loadTables(log);
    }

    public MiscQueryTest() {
        super(CitiesDataType.getManager());
    }

    @Test
    public void testFieldOpField() throws Exception {
        log.info("------  testFieldOpField  ------");
        // not sure if this is a valid test as it requires full table scan
        String query = CityField.CITY.name() + NE_OP + CityField.CODE.name();
        String expect = CityField.CITY.name() + NE_OP + "all";
        try {
            runTest(query, expect);
            Assert.fail("full table scan exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }

        try {
            runTestQuery(Collections.emptyList(), query, this.dataManager.getShardStartEndDate()[0], this.dataManager.getShardStartEndDate()[1],
                            Collections.emptyMap());
        } catch (FullTableScansDisallowedException e) {
            // expected
        }

        // @formatter:off
        List<String> expectedResults = Arrays.asList(
                "par-usa-ma-10",
                "ldn-fra-lle-11",
                "rom-usa-ms-10",
                "par-usa-oh-8",
                "par-usa-oh-9",
                "ldn-uk-7",
                "par-usa-mo-8",
                "rom-ita-7",
                "edge-case-id-3",
                "edge-case-id-4",
                "ldn-usa-mo-8",
                "par-ita-11",
                "edge-case-id-1",
                "edge-case-id-2",
                "ldn-usa-oh-8",
                "rom-usa-mo-8",
                "edge-case-id-9",
                "ldn-usa-mi-10",
                "par-fra-lle-7",
                "rom-bel-11",
                "edge-case-id-7",
                "edge-case-id-8",
                "edge-case-id-5",
                "edge-case-id-6",
                "rom-usa-oh-8");
        // @formatter:on

        this.logic.setFullTableScanEnabled(true);
        runTestQuery(expectedResults, query, this.dataManager.getShardStartEndDate()[0], this.dataManager.getShardStartEndDate()[1], Collections.emptyMap());
    }

    @Test
    public void testEventThreshold() throws Exception {
        log.info("------  testEventThreshold  ------");
        String phrase = RE_OP + "'.*a'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }

    @Test(expected = InvalidQueryException.class)
    public void testFieldIgnoreParam1() throws Exception {
        log.info("------  testFieldIgnoreParam1  ------");
        String phrase = RE_OP + "'.*a'" + "&& FOO == bar2";
        String query = Constants.ANY_FIELD + phrase + "&& FOO == bar2";
        String expect = this.dataManager.convertAnyField(phrase);

        Map<String,String> options = new HashMap<>();

        // this will throw an exception due to the nonexistent fields not being ignored.
        options.put(QueryParameters.IGNORE_NONEXISTENT_FIELDS, "false");

        runTest(query, expect, options);
    }

    @Test
    public void testFieldIgnoreParam2() throws Exception {
        log.info("------  testFieldIgnoreParam2  ------");
        String phrase = RE_OP + "'.*a'" + "&& FOO == bar2";
        String query = Constants.ANY_FIELD + phrase + "&& FOO == bar2";
        String expect = this.dataManager.convertAnyField(phrase);

        Map<String,String> options = new HashMap<>();

        // this should allow the query to run successfully.
        options.put(QueryParameters.IGNORE_NONEXISTENT_FIELDS, "true");

        runTest(query, expect, options);
    }

    @Test
    public void testFieldIgnoreParam3() throws Exception {
        log.info("------  testFieldIgnoreParam3  ------");
        String phrase = RE_OP + "'.*a' && STATE == 'sta'";
        String query = Constants.ANY_FIELD + phrase + "&& STATE == 'sta'";
        String expect = this.dataManager.convertAnyField(phrase);

        Map<String,String> options = new HashMap<>();

        options.put(QueryParameters.IGNORE_NONEXISTENT_FIELDS, "false");

        runTest(query, expect, options);
    }

    @Test
    public void testShardThreshold() throws Exception {
        log.info("------  testShardThreshold  ------");
        String phrase = RE_OP + "'.*a'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }

    @Test
    public void testDateRangeNoMatch() throws Exception {
        log.info("------  testShardDateRange  ------");
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0808.getDateStr());
        Calendar cal = Calendar.getInstance();
        cal.setTime(start);
        cal.add(Calendar.HOUR, 4);
        start = cal.getTime();
        cal.add(Calendar.HOUR, 1);
        Date end = cal.getTime();

        String query = CityField.CITY.name() + EQ_OP + "'pAris'";
        runTest(query, query, start, end);
    }

    @Test
    public void testDateRangeHours() throws Exception {
        log.info("------  testShardDateRange  ------");
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0808.getDateStr());
        Calendar cal = Calendar.getInstance();
        cal.setTime(start);
        cal.add(Calendar.HOUR, 4);
        Date end = cal.getTime();

        String query = CityField.CITY.name() + EQ_OP + "'pAris'";
        runTest(query, query, start, end);
    }

    @Test
    public void testDateRangeSameDay() throws Exception {
        log.info("------  testDateRangeSameDay  ------");
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0808.getDateStr());
        String query = CityField.CITY.name() + EQ_OP + "'pAris'";
        runTest(query, query, start, start);
    }

    @Test
    public void testDateRangeOneDay() throws Exception {
        log.info("------  testDateRangeOneDay  ------");
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0808.getDateStr());
        Calendar cal = Calendar.getInstance();
        cal.setTime(start);
        cal.add(Calendar.DATE, 1);
        Date end = cal.getTime();

        String query = CityField.CITY.name() + EQ_OP + "'pAris'";
        runTest(query, query, start, end);
    }

    @Test
    public void testDateRangeMonth() throws Exception {
        log.info("------  testDateRangeMonth  ------");
        // Date start = CitiesDataType.CityShardId.DATE_2015_0808.getDate();
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0808.getDateStr());
        Calendar cal = Calendar.getInstance();
        cal.setTime(start);
        cal.add(Calendar.MONTH, 3);
        Date end = cal.getTime();

        String query = CityField.CITY.name() + EQ_OP + "'pAris'";
        runTest(query, query, start, end);
    }

    @Test
    public void testDateRangeOneYear() throws Exception {
        log.info("------  testDateRangeOneYear  ------");
        // Date start = CitiesDataType.CityShardId.DATE_2015_0707.getDate();
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0707.getDateStr());
        Calendar cal = Calendar.getInstance();
        cal.setTime(start);
        cal.add(Calendar.YEAR, 1);
        Date end = cal.getTime();

        String query = CityField.CITY.name() + EQ_OP + "'pAris'";
        runTest(query, query, start, end);
    }

    @Test
    public void testLiteralNoMatch() throws Exception {
        log.info("------  testLiteralNoMatch  ------");
        String query = CityField.CITY.name() + EQ_OP + "'no-match'";
        runTest(query, query);
    }

    @Test
    public void testRawDataOnly() throws Exception {
        log.info("------  testRawDataOnly  ------");
        String city = "'paris'";
        String state = "'ohio'";
        String query = CityField.CITY.name() + EQ_OP + city + AND_OP + CityField.STATE.name() + EQ_OP + state;

        final List<QueryLogicTestHarness.DocumentChecker> queryChecker = new ArrayList<>();
        final RawDataChecker checker = new RawDataChecker();
        RawDataChecker.addHeaders(this.dataManager.getHeaders());
        queryChecker.add(checker);

        Map<String,String> options = new HashMap<>();
        options.put(QueryParameters.RAW_DATA_ONLY, "true");

        runTest(query, query, options, queryChecker);
    }

    @Test
    public void testTermThreshold() throws Exception {
        log.info("------  testTermThreshold  ------");
        String state = "'ohio'";
        for (TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "((_Bounded_ = true) && (" + CityField.STATE.name() + LTE_OP
                            + state + AND_OP + CityField.STATE.name() + GTE_OP + state + "))";

            this.logic.setInitialMaxTermThreshold(3);
            this.logic.setFinalMaxTermThreshold(3);
            runTest(query, query);

            this.logic.setInitialMaxTermThreshold(1);
            this.logic.setFinalMaxTermThreshold(1);
            try {
                runTest(query, query);
                Assert.fail("threshold exception expected");
            } catch (DatawaveFatalQueryException e) {
                // expected
            }
        }
    }

    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorQuery() throws Exception {
        log.info("------  testErrorQuery  ------");
        String query = "error-query";
        runTestQuery(Collections.emptyList(), query, this.dataManager.getShardStartEndDate()[0], this.dataManager.getShardStartEndDate()[1],
                        Collections.emptyMap());
    }

    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }

    /**
     * Checks the keys in the document when the {@link QueryParameters#RAW_DATA_ONLY} parameter is set to true. The keys in the document should contain all of
     * the header keys in addition to ingest information. Currently, there are two keys that are not returned with the raw data, "EVENT_DATATYPE" and
     * "RECORD_ID". This class will verify that these keys are not included in the response document.
     */
    private static class RawDataChecker implements QueryLogicTestHarness.DocumentChecker {

        // additional keys added during ingest
        static final Set<String> validKeys = new HashSet<>(Arrays.asList("ORIG_FILE", "RAW_FILE", "LOAD_DATE", "TIMING_METADATA"));
        // these keys are returned when QueryParameters.RAW_DATA_ONLY is not set
        static final Set<String> exclude = new HashSet<>(Arrays.asList("EVENT_DATATYPE", "RECORD_ID"));

        static void addHeaders(List<String> headers) {
            validKeys.addAll(headers);
        }

        @Override
        public void assertValid(Document doc) {
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : doc.entrySet()) {
                String key = entry.getKey();
                Assert.assertFalse("excluded key found(" + key + ")", exclude.contains(key));
                Assert.assertTrue("invalid key(" + key + ")", validKeys.contains(key));
            }
        }
    }
}
