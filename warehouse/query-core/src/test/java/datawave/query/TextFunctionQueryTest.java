package datawave.query;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.planner.DatePartitionedQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;

public class TextFunctionQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(TextFunctionQueryTest.class);

    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));

        accumuloSetup.setData(FileType.CSV, dataTypes);
        client = accumuloSetup.loadTables(log);
    }

    public TextFunctionQueryTest() {
        super(CitiesDataType.getManager());
    }

    @Test
    public void testAnyFieldText() throws Exception {
        log.info("------  testAnyFieldText  ------");
        // must be same case as original value in event
        String query = "CONTINENT:europe and #TEXT(Lazio)";
        String expect = "CONTINENT == 'europe' and (CITY  == 'Lazio'  or  CONTINENT  == 'Lazio'  or  STATE  == 'Lazio')";
        runTest(query, expect);

        // testing that incorrect case misses results, query should return an empty set
        query = "CONTINENT:\"europe\" and #TEXT(lazio)";
        runTestQuery(Collections.emptySet(), query);
    }

    @Test
    public void testAnyFieldTextNoHits() throws Exception {
        log.info("------  testAnyFieldTextNoHits  ------");

        ((DatePartitionedQueryPlanner) this.logic.getQueryPlanner()).getQueryPlanner().setReduceQuery(true);

        String code = "europe";
        // must be same case as original value in event
        String state = "blah";
        String phrase = EQ_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + OR_OP + "#TEXT(" + state + ")";
        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + OR_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }

    @Test
    public void testExplicitAnyFieldText() throws Exception {
        log.info("------  testExplicitAnyFieldText  ------");
        String code = "europe";
        String state = "Lazio";
        String phrase = EQ_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#TEXT(" + Constants.ANY_FIELD + "," + state + ")";
        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }

    @Test
    public void testMultiFieldText() throws Exception {
        log.info("------  testMultiFieldText  ------");

        String query = "CONTINENT:europe and #TEXT(OR, STATE, Lazio, STATE, London)";
        String expect = "CONTINENT == 'europe' and ( STATE == 'Lazio' or STATE == 'London' )";
        runTest(query, expect);

        // lowercase 'london' will fail to return all of those events, leaving only the 'Lazio' events
        query = "CONTINENT:europe and #TEXT(OR, STATE, Lazio, STATE, london)";
        expect = "CONTINENT == 'europe' and STATE == 'Lazio'";
        runTest(query, expect);

        // incorrect case for 'lazio' and 'london' will find zero hits
        query = "CONTINENT:\"europe\" and #TEXT(OR, STATE,lazio, STATE, london)";
        runTestQuery(Collections.emptySet(), query);
    }

    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }

    @Override
    public ShardQueryLogic createShardQueryLogic() {
        ShardQueryLogic logic = super.createShardQueryLogic();
        logic.setParser(new LuceneToJexlQueryParser());
        return logic;
    }
}
