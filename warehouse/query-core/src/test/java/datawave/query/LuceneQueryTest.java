package datawave.query;

import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class LuceneQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(LuceneQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public LuceneQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testSimpleEq() throws Exception {
        log.info("------  testSimpleEq  ------");
        String city = "rome";
        String query = CityField.CITY.name() + ":\"" + city + "\"";

        String expect = CityField.CITY.name() + EQ_OP + "'" + city + "'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);

        runTest(query, expect);
    }

    @Test
    public void testSimpleAndEq() throws Exception {
        log.info("------  testSimpleAndEq  ------");
        String city = "rome";
        String state = "italy";
        String query = CityField.CITY.name() + ":\"" + city + "\"" + AND_OP + CityField.STATE.name() + ":\"" + state + "\"";

        String expect = CityField.CITY.name() + '_' + CityField.STATE.name() + EQ_OP + "'" + city + CompositeIngest.DEFAULT_SEPARATOR + state + "'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);

        runTest(query, expect);
    }

    @Test
    public void testAnyFieldInclude() throws Exception {
        log.info("------  testAnyFieldInclude  ------");
        String code = "europe";
        String state = "lazio";
        String phrase = RE_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#INCLUDE(" + state + ")";

        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + JEXL_AND_OP + "filter:includeRegex(" + Constants.ANY_FIELD + ", '" + state
                + "')";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);

        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testExplicitAnyFieldInclude() throws Exception {
        log.info("------  testExplicitAnyFieldInclude  ------");
        String code = "europe";
        String state = "lazio";
        String phrase = RE_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#INCLUDE(" + Constants.ANY_FIELD + "," + state + ")";

        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + JEXL_AND_OP + "(filter:includeRegex(" + Constants.ANY_FIELD + ", '" + state
                + "'))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);

        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testAnyFieldWithRegex() throws Exception {
        log.info("------  testAnyField  ------");
        String code = "europe";
        String state = "l.*";
        String phrase = RE_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#INCLUDE(" + Constants.ANY_FIELD + "," + state + ")";

        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + JEXL_AND_OP + "(filter:includeRegex(" + Constants.ANY_FIELD + ", '" + state
                + "'))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);

        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testAnyFieldNotNullLiteral() throws Exception {
        log.info("------  testAnyField  ------");
        String cont = "europe";
        String state = "l.*";
        String phrase = RE_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + cont + "\"" + AND_OP + CityField.CITY.name() + ":*" + AND_OP + "#INCLUDE(" + Constants.ANY_FIELD
                        + "," + state + ")";

        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + cont + "'" + JEXL_AND_OP + "!(" + CityField.CITY.name() + EQ_OP + "null)" + JEXL_AND_OP
                + "(filter:includeRegex(" + Constants.ANY_FIELD + ", '" + state + "'))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);

        expect = CityField.CONTINENT.name() + EQ_OP + "'" + cont + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
        
        this.logic.setParser(new LuceneToJexlQueryParser());
    }
}
