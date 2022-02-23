package datawave.query;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.BaseRawData;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GTE_OP;
import static datawave.query.testframework.RawDataManager.LTE_OP;
import static datawave.query.testframework.RawDataManager.NE_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

public class DataTypeQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(DataTypeQueryTest.class);
    
    private static final List<String> TEST_STATES = Arrays.asList("'ohio'", "'Missouri'", "'Maine'");
    private static final List<String> TEST_NUMS = Arrays.asList("100", "110", "120");
    private static final List<CityEntry> TEST_DATATYPES = Arrays.asList(CityEntry.generic, CityEntry.usa, CityEntry.dup_usa);
    private static final List<String> INVALID_DATATYPES = Arrays.asList("invalid-one", "invalid-two");
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.NUM.name());
        for (String idx : generic.getIndexFields()) {
            generic.addReverseIndexField(idx);
        }
        
        for (CityEntry entry : TEST_DATATYPES) {
            dataTypes.add(new CitiesDataType(entry, generic));
        }
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public DataTypeQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testDataTypeEqualInQuery() throws Exception {
        log.info("------  testDataTypeEqualInQuery  ------");
        
        for (CityEntry dtEntry : TEST_DATATYPES) {
            String event = "'" + dtEntry.getDataType() + "'";
            for (String state : TEST_STATES) {
                String query = CityField.STATE.name() + EQ_OP + state + AND_OP + BaseRawData.EVENT_DATATYPE + EQ_OP + event;
                runTest(query, query);
            }
        }
    }
    
    @Test
    public void testDataTypeNotEqualInQuery() throws Exception {
        log.info("------  testDataTypeEqualInQuery  ------");
        
        for (CityEntry dtEntry : TEST_DATATYPES) {
            String event = "'" + dtEntry.getDataType() + "'";
            for (String state : TEST_STATES) {
                String query = CityField.STATE.name() + EQ_OP + state + AND_OP + BaseRawData.EVENT_DATATYPE + NE_OP + event;
                runTest(query, query);
            }
        }
    }
    
    @Test
    public void testSimpleParam() throws Exception {
        log.info("------  testSimpleParam  ------");
        
        final Map<String,String> qOptions = new HashMap<>();
        for (CityEntry dtEntry : TEST_DATATYPES) {
            String dataType = dtEntry.getDataType();
            qOptions.put(QueryParameters.DATATYPE_FILTER_SET, dataType);
            
            for (String state : TEST_STATES) {
                String query = CityField.STATE.name() + EQ_OP + state;
                String expect = query + AND_OP + BaseRawData.EVENT_DATATYPE + EQ_OP + "'" + dataType + "'";
                runTest(query, expect, qOptions);
            }
        }
    }
    
    @Test
    public void testMultiDatatype() throws Exception {
        log.info("------  testMultiDatatype  ------");
        
        final Map<String,String> qOptions = new HashMap<>();
        String dtFilter = CityEntry.generic.getDataType() + ShardQueryConfiguration.PARAM_VALUE_SEP_STR + CityEntry.usa.getDataType();
        qOptions.put(QueryParameters.DATATYPE_FILTER_SET, dtFilter);
        
        for (String state : TEST_STATES) {
            String query = CityField.STATE.name() + EQ_OP + state;
            String expect = query + AND_OP + "(" + BaseRawData.EVENT_DATATYPE + EQ_OP + "'" + CityEntry.generic.getDataType() + "'" + OR_OP
                            + BaseRawData.EVENT_DATATYPE + EQ_OP + "'" + CityEntry.usa.getDataType() + "')";
            runTest(query, expect, qOptions);
        }
    }
    
    @Test
    public void testEventPerDay() throws Exception {
        log.info("------  testEventPerDay  ------");
        
        final Map<String,String> qOptions = new HashMap<>();
        String dtFilter = CityEntry.generic.getDataType();
        qOptions.put(QueryParameters.DATATYPE_FILTER_SET, dtFilter);
        
        this.logic.setEventPerDayThreshold(1);
        
        String query = CityField.STATE.name() + EQ_OP + "'missouri'";
        String expect = "(" + query + ")" + AND_OP + BaseRawData.EVENT_DATATYPE + EQ_OP + "'" + CityEntry.generic.getDataType() + "'";
        runTest(query, expect, qOptions);
    }
    
    @Test
    public void testEventAndShardsPerDay() throws Exception {
        log.info("------  testEventAndShardsPerDay  ------");
        
        final Map<String,String> qOptions = new HashMap<>();
        String dtFilter = CityEntry.generic.getDataType();
        qOptions.put(QueryParameters.DATATYPE_FILTER_SET, dtFilter);
        
        this.logic.setEventPerDayThreshold(1);
        this.logic.setShardsPerDayThreshold(1);
        
        String query = CityField.STATE.name() + EQ_OP + "'missouri'";
        String expect = "(" + query + ")" + AND_OP + BaseRawData.EVENT_DATATYPE + EQ_OP + "'" + CityEntry.generic.getDataType() + "'";
        runTest(query, expect, qOptions);
    }
    
    @Test
    public void testRange() throws Exception {
        log.info("------  testRange  ------");
        
        final Map<String,String> qOptions = new HashMap<>();
        String dtFilter = CityEntry.generic.getDataType();
        qOptions.put(QueryParameters.DATATYPE_FILTER_SET, dtFilter);
        
        for (String num : TEST_NUMS) {
            String query = CityField.NUM.name() + GTE_OP + num + AND_OP + CityField.NUM.name() + LTE_OP + num;
            String expect = "(" + query + ")" + AND_OP + BaseRawData.EVENT_DATATYPE + EQ_OP + "'" + CityEntry.generic.getDataType() + "'";
            query = "((_Bounded_ = true) && (" + query + "))";
            runTest(query, expect, qOptions);
        }
    }
    
    @Test
    public void testRangeInvalidDatatypes() throws Exception {
        log.info("------  testRangeInvalidDatatypes  ------");
        
        final Map<String,String> qOptions = new HashMap<>();
        String dtFilter = CityEntry.generic.getDataType() + ShardQueryConfiguration.PARAM_VALUE_SEP_STR
                        + String.join(ShardQueryConfiguration.PARAM_VALUE_SEP_STR, INVALID_DATATYPES);
        qOptions.put(QueryParameters.DATATYPE_FILTER_SET, dtFilter);
        
        for (String num : TEST_NUMS) {
            String query = CityField.NUM.name() + GTE_OP + num + AND_OP + CityField.NUM.name() + LTE_OP + num;
            String expect = "(" + query + ")" + AND_OP + BaseRawData.EVENT_DATATYPE + EQ_OP + "'" + CityEntry.generic.getDataType() + "'";
            query = "((_Bounded_ = true) && (" + query + "))";
            runTest(query, expect, qOptions);
        }
    }
    
    @Test
    public void testAnyFieldMultiDatatypes() throws Exception {
        log.info("------  testAnyFieldMultiDatatypes  ------");
        
        final Map<String,String> qOptions = new HashMap<>();
        final List<String> dtEntries = new ArrayList<>();
        for (CityEntry dt : TEST_DATATYPES) {
            dtEntries.add(dt.getDataType());
        }
        String dtFilter = String.join(ShardQueryConfiguration.PARAM_VALUE_SEP_STR, dtEntries);
        qOptions.put(QueryParameters.DATATYPE_FILTER_SET, dtFilter);
        
        final String phrase = EQ_OP + "'ohio'";
        final String any = this.dataManager.convertAnyField(phrase);
        for (String num : TEST_NUMS) {
            String query = Constants.ANY_FIELD + phrase;
            StringBuilder expect = new StringBuilder();
            expect.append(any).append(AND_OP).append("(");
            String op = "";
            for (String dt : dtEntries) {
                expect.append(op).append(BaseRawData.EVENT_DATATYPE).append(EQ_OP).append("'").append(dt).append("'");
                op = OR_OP;
            }
            expect.append(")");
            runTest(query, expect.toString(), qOptions);
        }
    }
    
    @Test
    public void testAnyFieldInvalidDatatypes() throws Exception {
        log.info("------  testAnyFieldInvalidDatatypes  ------");
        
        final Map<String,String> qOptions = new HashMap<>();
        String dtFilter = String.join(ShardQueryConfiguration.PARAM_VALUE_SEP_STR, INVALID_DATATYPES);
        qOptions.put(QueryParameters.DATATYPE_FILTER_SET, dtFilter);
        
        final String phrase = EQ_OP + "'ohio'";
        final String any = this.dataManager.convertAnyField(phrase);
        for (String num : TEST_NUMS) {
            String query = Constants.ANY_FIELD + phrase;
            StringBuilder expect = new StringBuilder();
            expect.append(any).append(AND_OP).append("(");
            String op = "";
            for (String dt : INVALID_DATATYPES) {
                expect.append(op).append(BaseRawData.EVENT_DATATYPE).append(EQ_OP).append("'").append(dt).append("'");
                op = OR_OP;
            }
            expect.append(")");
            runTest(query, expect.toString(), qOptions);
        }
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
