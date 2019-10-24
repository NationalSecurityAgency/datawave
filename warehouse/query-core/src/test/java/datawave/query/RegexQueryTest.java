package datawave.query;

import datawave.query.exceptions.DoNotPerformOptimizedQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.visitors.ParallelIndexExpansion;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.util.AllFieldMetadataHelper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class RegexQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(RegexQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.CODE.name());
        generic.removeIndexField(CityField.CONTINENT.name());
        generic.addReverseIndexField(CityField.STATE.name());
        generic.addReverseIndexField(CityField.CONTINENT.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public RegexQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testBasic() throws Exception {
        log.info("------  testBasic  ------");
        String regex = "'fR.*'";
        String query = CityField.CODE.name() + RE_OP + regex;
        runTest(query, query);
    }
    
    @Test
    public void testIndexNoMatch() throws Exception {
        log.info("------  testIndexNoMatch  ------");
        String regex = "'x.*'";
        String query = CityField.CODE.name() + RE_OP + regex;
        runTest(query, query);
    }
    
    @Test
    public void testReverse() throws Exception {
        log.info("------  testReverse  ------");
        String regex = "'.*i'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + " == " + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + RE_OP + regex;
            runTest(query, query);
        }
    }
    
    @Test
    public void testMissingIndex() throws Exception {
        log.info("------  testMissingIndex  ------");
        // should at least match France
        String regex = "'.*?e'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + " == " + "'" + city.name() + "'" + AND_OP + CityField.COUNTRY.name() + RE_OP + regex;
            runTest(query, query);
        }
    }
    
    @Test
    public void testMissingReverseIndex() throws Exception {
        log.info("------  testMissingReverseIndex  ------");
        Logger.getLogger(DefaultQueryPlanner.class).setLevel(Level.DEBUG);
        // should at least match usa, fra, and ita
        String regex = "'.*?a'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + " == " + "'" + city.name() + "'" + AND_OP + CityField.CODE.name() + RE_OP + regex;
            runTest(query, query);
        }
    }
    
    @Test
    public void testMissingReverseIndexPlus() throws Exception {
        log.info("------  testMissingReverseIndex  ------");
        Logger.getLogger(DefaultQueryPlanner.class).setLevel(Level.DEBUG);
        Logger.getLogger(ParallelIndexExpansion.class).setLevel(Level.DEBUG);
        Logger.getLogger(ShardIndexQueryTableStaticMethods.class).setLevel(Level.DEBUG);
        Logger.getLogger(AllFieldMetadataHelper.class).setLevel(Level.DEBUG);
        // should at least match usa, fra, and ita
        String regex = "'.*?a'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + " == " + "'" + city.name() + "'" + AND_OP + CityField.CODE.name() + RE_OP + regex + AND_OP + "!("
                            + CityField.STATE + EQ_OP + "null)";
            runTest(query, query);
        }
    }
    
    @Test
    public void testEqualAndRegex() throws Exception {
        log.info("------  testEqualAndRegex  ------");
        String regex = "'miss.*'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + RE_OP + regex;
            runTest(query, query);
        }
    }
    
    @Test
    public void testQueryThreads() throws Exception {
        log.info("------  testQueryThreads  ------");
        String regex = "'miss.*U.*'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + RE_OP + regex;
            this.logic.setQueryThreads(100);
            runTest(query, query);
        }
    }
    
    @Test
    public void testBoolean() throws Exception {
        log.info("------  testBoolean  ------");
        String regex = "'.*isS.*'";
        String code = "'uSa'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP + CityField.CODE.name() + EQ_OP + code + ")" + AND_OP
                            + CityField.STATE.name() + RE_OP + regex;
            runTest(query, query);
        }
    }
    
    @Test
    public void testAndNot() throws Exception {
        log.info("------  testAndNot  ------");
        String regex = "'.*iSs.*'";
        String code = "'uSa'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + " not (" + CityField.STATE.name() + RE_OP + regex + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testReluctantZeroOrMoreNoMatch() throws Exception {
        log.info("------  testReluctantZeroOrMoreNoMatch  ------");
        String regex = "'x.*?'";
        String query = CityField.STATE.name() + RE_OP + regex;
        runTest(query, query);
    }
    
    @Test
    public void testReluctantZeroOrMore() throws Exception {
        log.info("------  testReluctantZeroOrMore  ------");
        String regex = "'lA.*?'";
        String query = CityField.STATE.name() + RE_OP + regex;
        runTest(query, query);
    }
    
    @Test
    public void testInfinite() throws Exception {
        log.info("------  testInfinite  ------");
        String regex = "'.*'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + RE_OP + regex;
            runTest(query, query);
        }
    }
    
    @Test
    public void testFullTableScan() throws Exception {
        String regex = "'.*uro.*'";
        String query = CityField.CONTINENT.name() + RE_OP + regex;
        logic.setFullTableScanEnabled(true);
        runTest(query, query);
    }
    
    // ============================================
    // error conditions
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorOptimized() throws Exception {
        String regex = "'.*iss.*'";
        String query = CityField.STATE.name() + RE_OP + regex;
        runTest(query, query);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorFullTableScan() throws Exception {
        String regex = "'.*uro.*'";
        String query = CityField.CONTINENT.name() + RE_OP + regex;
        runTest(query, query);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorInfinite() throws Exception {
        String regex = "'.*'";
        String query = CityField.STATE.name() + RE_OP + regex;
        runTest(query, query);
    }
    
    @Test(expected = DoNotPerformOptimizedQueryException.class)
    public void testErrorFullTableInfinite() throws Exception {
        String regex = "'.*'";
        String query = CityField.STATE.name() + RE_OP + regex;
        logic.setFullTableScanEnabled(true);
        runTest(query, query);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorMissingReverseIndex() throws Exception {
        log.info("------  testMissingReverseIndex  ------");
        Logger.getLogger(DefaultQueryPlanner.class).setLevel(Level.DEBUG);
        // should at least match usa, fra, and ita
        String regex = "'.*?a'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CODE.name() + RE_OP + regex;
            runTest(query, query);
        }
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
