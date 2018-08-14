package datawave.query;

import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.GenericCityFields;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.NE_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static datawave.query.testframework.RawDataManager.RN_OP;

public class DataTypeQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(DataTypeQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.CODE.name());
        for (String idx : generic.getIndexFields()) {
            generic.addReverseIndexField(idx);
        }
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        dataTypes.add(new CitiesDataType(CityEntry.usa, generic));
        dataTypes.add(new CitiesDataType(CityEntry.dup_usa, generic));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public DataTypeQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testDataTypeEqualInQuery() throws Exception {
        log.info("------  testDataTypeEqualInQuery  ------");
        String city = "'rOme'";
        String event = "'" + CityEntry.getDataType(CityEntry.usa) + "'";
        
        String query = CityField.CITY.name() + EQ_OP + city + AND_OP + "EvEnt_DataType" + EQ_OP + event;
        String expect = CityField.CITY.name() + EQ_OP + city + AND_OP + CityField.EVENT_ID.name() + RE_OP + "'" + CityEntry.getDataType(CityEntry.usa) + ".*'";
        runTest(query, expect);
    }
    
    @Test
    public void testDataTypeNotEqualInQuery() throws Exception {
        log.info("------  testDataTypeEqualInQuery  ------");
        String city = "'rOme'";
        String event = "'" + CityEntry.getDataType(CityEntry.usa) + "'";
        
        String query = CityField.CITY.name() + EQ_OP + city + AND_OP + "EvEnt_DataType" + NE_OP + event;
        String expect = CityField.CITY.name() + EQ_OP + city + AND_OP + CityField.EVENT_ID.name() + RN_OP + "'" + CityEntry.getDataType(CityEntry.usa) + ".*'";
        runTest(query, expect);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
