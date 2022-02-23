package datawave.query;

import datawave.query.exceptions.DoNotPerformOptimizedQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.visitors.RegexIndexExpansionVisitor;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.query.util.AllFieldMetadataHelper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class RegexQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
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
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
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
        Logger.getLogger(RegexIndexExpansionVisitor.class).setLevel(Level.DEBUG);
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
    
    // THE EVENT VALUE: '\Edge-City-1'
    // EQUALS QUERY: CITY == '\Edge-City-1'
    // REGEX QUERY: CITY =~ '\\Edge-City-1'
    @Test
    public void test1LeadingBackslashEquals() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'\\\\Edge-City-1'";
        String query = CityField.CITY.name() + EQ_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: '\Edge-City-1'
    // EQUALS QUERY: CITY == '\\Edge-City-1'
    // REGEX QUERY: CITY =~ '\\Edge-City-1'
    @Test
    public void test1LeadingBackslashRegex() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'\\\\Edge-City-1'";
        String query = CityField.CITY.name() + RE_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: '\\Edge-City-2'
    // EQUALS QUERY: CITY == '\\\\Edge-City-2'
    // REGEX QUERY: CITY =~ '\\\\Edge-City-2'
    @Test
    public void test2LeadingBackslashesEquals() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'\\\\\\\\Edge-City-2'";
        String query = CityField.CITY.name() + EQ_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: '\\Edge-City-2'
    // EQUALS QUERY: CITY == '\\\\Edge-City-2'
    // REGEX QUERY: CITY =~ '\\\\Edge-City-2'
    @Test
    public void test2LeadingBackslashesRegex() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'\\\\\\\\Edge-City-2'";
        String query = CityField.CITY.name() + RE_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: '\\\Edge-City-3'
    // EQUALS QUERY: CITY == '\\\\\\Edge-City-2'
    // REGEX QUERY: CITY =~ '\\\\\\Edge-City-2'
    @Test
    public void test3LeadingBackslashesEquals() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'\\\\\\\\\\\\Edge-City-3'";
        String query = CityField.CITY.name() + EQ_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: '\\\Edge-City-3'
    // EQUALS QUERY: CITY == '\\\\\\Edge-City-2'
    // REGEX QUERY: CITY =~ '\\\\\\Edge-City-2'
    @Test
    public void test3LeadingBackslashesRegex() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'\\\\\\\\\\\\Edge-City-3'";
        String query = CityField.CITY.name() + RE_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-City-4\'
    // EQUALS QUERY: CITY == 'Edge-City-2\\'
    // REGEX QUERY: CITY =~ 'Edge-City-2\\'
    @Test
    public void test1TrailingBackslashEquals() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-City-4\\\\'";
        String query = CityField.CITY.name() + EQ_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-City-4\'
    // EQUALS QUERY: CITY == 'Edge-City-2\\'
    // REGEX QUERY: CITY =~ 'Edge-City-2\\'
    @Test
    public void test1TrailingBackslashRegex() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-City-4\\\\'";
        String query = CityField.CITY.name() + RE_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-City-5\\'
    // EQUALS QUERY: CITY == 'Edge-City-5\\\\'
    // REGEX QUERY: CITY =~ 'Edge-City-5\\\\'
    @Test
    public void test2TrailingBackslashesEquals() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-City-5\\\\\\\\'";
        String query = CityField.CITY.name() + EQ_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-City-5\\'
    // EQUALS QUERY: CITY == 'Edge-City-5\\\\'
    // REGEX QUERY: CITY =~ 'Edge-City-5\\\\'
    @Test
    public void test2TrailingBackslashesRegex() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-City-5\\\\\\\\'";
        String query = CityField.CITY.name() + RE_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-City-6\\\'
    // EQUALS QUERY: CITY == 'Edge-City-6\\\\\\'
    // REGEX QUERY: CITY =~ 'Edge-City-6\\\\\\'
    @Test
    public void test3TrailingBackslashesEquals() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-City-6\\\\\\\\\\\\'";
        String query = CityField.CITY.name() + EQ_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-City-6\\\'
    // EQUALS QUERY: CITY == 'Edge-City-6\\\\\\'
    // REGEX QUERY: CITY =~ 'Edge-City-6\\\\\\'
    @Test
    public void test4TrailingBackslashesRegex() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-City-6\\\\\\\\\\\\'";
        String query = CityField.CITY.name() + RE_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-C\ity-7'
    // EQUALS QUERY: CITY == 'Edge-C\\ity-7'
    // REGEX QUERY: CITY =~ 'Edge-C\\ity-7'
    @Test
    public void test1InternalBackslashEquals() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-C\\\\ity-7'";
        String query = CityField.CITY.name() + EQ_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-C\ity-7'
    // EQUALS QUERY: CITY == 'Edge-C\\ity-7'
    // REGEX QUERY: CITY =~ 'Edge-C\\ity-7'
    @Test
    public void test1InternalBackslashRegex() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-C\\\\ity-7'";
        String query = CityField.CITY.name() + RE_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-C\\ity-8'
    // EQUALS QUERY: CITY == 'Edge-C\\\\ity-8'
    // REGEX QUERY: CITY =~ 'Edge-C\\\\ity-8'
    @Test
    public void test2InternalBackslashesEquals() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-C\\\\\\\\ity-8'";
        String query = CityField.CITY.name() + EQ_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-C\\ity-8'
    // EQUALS QUERY: CITY == 'Edge-C\\\\ity-8'
    // REGEX QUERY: CITY =~ 'Edge-C\\\\ity-8'
    @Test
    public void test2InternalBackslashesRegex() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-C\\\\\\\\ity-8'";
        String query = CityField.CITY.name() + RE_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-C\\\ity-9'
    // EQUALS QUERY: CITY == 'Edge-C\\\\\\ity-9'
    // REGEX QUERY: CITY =~ 'Edge-C\\\\\\ity-9'
    @Test
    public void test3InternalBackslashesEquals() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-C\\\\\\\\\\\\ity-9'";
        String query = CityField.CITY.name() + EQ_OP + term;
        runTest(query, query);
    }
    
    // THE EVENT VALUE: 'Edge-C\\\ity-9'
    // EQUALS QUERY: CITY == 'Edge-C\\\\\\ity-9'
    // REGEX QUERY: CITY =~ 'Edge-C\\\\\\ity-9'
    @Test
    public void test3InternalBackslashesRegex() throws Exception {
        // NOTE: JAVA REQUIRES THAT WE ESCAPE OUR BACKSLASHES
        String term = "'Edge-C\\\\\\\\\\\\ity-9'";
        String query = CityField.CITY.name() + RE_OP + term;
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
