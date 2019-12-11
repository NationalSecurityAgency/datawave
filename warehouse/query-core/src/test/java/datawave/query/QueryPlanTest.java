package datawave.query;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.*;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.RE_OP;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class QueryPlanTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(AnyFieldQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addReverseIndexField(CitiesDataType.CityField.STATE.name());
        generic.addReverseIndexField(CitiesDataType.CityField.CONTINENT.name());
        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.generic, generic));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public QueryPlanTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void getPlanAfterFullTableScanDisallowedException() throws Exception {
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = " != " + "'" + city.name() + "'";
            String query = Constants.ANY_FIELD + cityPhrase;
            // Test list of cities for each plan
            try {
                GenericQueryConfiguration config = setupConfig(query);
                fail("Expected FullTableScanDisallowedException.");
            } catch (FullTableScansDisallowedException e) {
                // assure that Query Plan is not default value
                assertNotEquals("There is no plan.", "No Query Plan was set.", this.logic.getQueryPlan());
            }
        }
    }
    
    @Test
    public void getPlanAfterDatawaveFatalQueryException() throws Exception {
        String phrase = RE_OP + "'.*iss.*'";
        String query = Constants.ANY_FIELD + phrase;
        
        // Test the plan
        try {
            GenericQueryConfiguration config = setupConfig(query);
            fail("Expected DatawaveFatalQueryException but got plan: " + this.logic.getQueryPlan());
        } catch (DatawaveFatalQueryException e) {
            // assure that Query Plan is not default value
            assertNotEquals("There is no plan.", "No Query Plan was set.", this.logic.getQueryPlan());
        }
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
}
