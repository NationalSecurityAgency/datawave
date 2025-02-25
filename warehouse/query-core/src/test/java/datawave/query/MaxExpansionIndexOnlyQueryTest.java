package datawave.query;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.NOT_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.planner.DatePartitionedQueryPlanner;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.MaxExpandCityFields;

public class MaxExpansionIndexOnlyQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(MaxExpansionRegexQueryTest.class);

    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig max = new MaxExpandCityFields();
        max.addIndexOnlyField(CitiesDataType.CityField.CITY.name());
        max.addIndexOnlyField(CitiesDataType.CityField.STATE.name());

        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.maxExp, max));

        accumuloSetup.setData(FileType.CSV, dataTypes);
        client = accumuloSetup.loadTables(log);
    }

    public MaxExpansionIndexOnlyQueryTest() {
        super(CitiesDataType.getManager());
    }

    // ===================================
    // test cases

    @Test
    public void testMaxValueRegexIndexOnly_defaultQueryPlanner() throws Exception {
        log.info("------  testMaxValueRegexIndexOnly : " + DefaultQueryPlanner.class.getSimpleName() + " ------");

        this.logic.setQueryPlanner(new DefaultQueryPlanner());

        // set regex to match multiple fields
        String city = EQ_OP + "'a-1'";
        String code = RE_OP + "'b.*'";

        String query = CitiesDataType.CityField.CITY.name() + city + AND_OP + CitiesDataType.CityField.STATE.name() + code;

        this.logic.setMaxValueExpansionThreshold(20);
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);

        this.logic.setMaxValueExpansionThreshold(2);
        try {
            runTest(query, query);
            Assert.fail("exception expected");
        } catch (DatawaveFatalQueryException e) {
            // expected
        }

        ivaratorConfig();
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }

    @Test
    public void testMaxValueRegexIndexOnly_federatedQueryPlanner() throws Exception {
        log.info("------  testMaxValueRegexIndexOnly : " + DatePartitionedQueryPlanner.class.getSimpleName() + " ------");

        this.logic.setQueryPlanner(new DatePartitionedQueryPlanner());

        // set regex to match multiple fields
        String city = EQ_OP + "'a-1'";
        String code = RE_OP + "'b.*'";

        String query = CitiesDataType.CityField.CITY.name() + city + AND_OP + CitiesDataType.CityField.STATE.name() + code;

        this.logic.setMaxValueExpansionThreshold(20);
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);

        this.logic.setMaxValueExpansionThreshold(2);
        try {
            runTest(query, query);
            Assert.fail("exception expected");
        } catch (DatawaveFatalQueryException e) {
            // expected
        }

        ivaratorConfig();
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }

    @Test
    public void testMaxValueAnyField_defaultQueryPlanner() throws Exception {
        log.info("------  testMaxValueAnyField : " + DefaultQueryPlanner.class.getSimpleName() + " ------");

        this.logic.setQueryPlanner(new DefaultQueryPlanner());

        String regexT = RE_OP + "'b-.*'";
        String regexA = RE_OP + "'a-.*'";
        String query = Constants.ANY_FIELD + regexT + AND_OP + Constants.ANY_FIELD + regexA;
        String anyT = this.dataManager.convertAnyField(regexT);
        String anyA = this.dataManager.convertAnyField(regexA);
        String expect = anyT + AND_OP + anyA;

        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);

        this.logic.setMaxValueExpansionThreshold(2);
        try {
            runTest(query, expect);
            Assert.fail("exception expected");
        } catch (RuntimeException re) {
            // expected
        }

        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);

        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 2);
    }

    @Test
    public void testMaxValueAnyField_federatedQueryPlanner() throws Exception {
        log.info("------  testMaxValueAnyField : " + DatePartitionedQueryPlanner.class.getSimpleName() + " ------");

        this.logic.setQueryPlanner(new DatePartitionedQueryPlanner());

        String regexT = RE_OP + "'b-.*'";
        String regexA = RE_OP + "'a-.*'";
        String query = Constants.ANY_FIELD + regexT + AND_OP + Constants.ANY_FIELD + regexA;
        String anyT = this.dataManager.convertAnyField(regexT);
        String anyA = this.dataManager.convertAnyField(regexA);
        String expect = anyT + AND_OP + anyA;

        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);

        this.logic.setMaxValueExpansionThreshold(2);
        try {
            runTest(query, expect);
            Assert.fail("exception expected");
        } catch (RuntimeException re) {
            // expected
        }

        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);

        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 2);
    }

    @Test
    public void testMaxValueNegAnyField_defaultQueryPlanner() throws Exception {
        log.info("------  testMaxValueNegAnyField : " + DefaultQueryPlanner.class.getSimpleName() + "  ------");

        this.logic.setQueryPlanner(new DefaultQueryPlanner());

        String regexPhrase = RE_OP + "'a.*'";
        String country = "'b-StaTe'";
        String query = Constants.ANY_FIELD + EQ_OP + country + AND_OP + NOT_OP + "(" + Constants.ANY_FIELD + regexPhrase + ")";
        String expect = CitiesDataType.CityField.STATE.name() + EQ_OP + "'bi-s'";

        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);

        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, expect);
            Assert.fail("exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }

        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }

    @Test
    public void testMaxValueNegAnyField_federatedQueryPlanner() throws Exception {
        log.info("------  testMaxValueNegAnyField : " + DatePartitionedQueryPlanner.class.getSimpleName() + "  ------");

        this.logic.setQueryPlanner(new DatePartitionedQueryPlanner());

        String regexPhrase = RE_OP + "'a.*'";
        String country = "'b-StaTe'";
        String query = Constants.ANY_FIELD + EQ_OP + country + AND_OP + NOT_OP + "(" + Constants.ANY_FIELD + regexPhrase + ")";
        String expect = CitiesDataType.CityField.STATE.name() + EQ_OP + "'bi-s'";

        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);

        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, expect);
            Assert.fail("exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }

        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }

    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getExpansionAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
}
