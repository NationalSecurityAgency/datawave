package datawave.query;

import datawave.data.type.NumberType;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.webservice.query.configuration.QueryData;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GT_OP;
import static datawave.query.testframework.RawDataManager.LT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

public class UnindexedNumericQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(UnindexedNumericQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        Set<String> virt = new HashSet<>(Arrays.asList(CityField.CITY.name(), CityField.CONTINENT.name()));
        generic.removeVirtualField(virt);
        for (String idx : generic.getIndexFields()) {
            generic.addReverseIndexField(idx);
        }
        dataTypes.add(new CitiesDataType(CityEntry.usa, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public UnindexedNumericQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testNumericTerm() throws Exception {
        log.info("------  testNumericTerm  ------");
        
        String min = "115";
        String iowa = "'indiana'";
        String query = CityField.STATE.name() + EQ_OP + iowa + AND_OP + CityField.NUM.name() + GT_OP + min;
        
        ShardQueryConfiguration config = (ShardQueryConfiguration) setupConfig(query);
        // verify NUM is NumberType
        String indexStr = config.getIndexedFieldDataTypesAsString();
        Assert.assertTrue(indexStr.contains(CityField.NUM.name() + ":" + NumberType.class.getName()));
        
        // NUM field should not be indexed
        Set<String> indexes = config.getIndexedFields();
        Assert.assertFalse(indexes.contains(CityField.NUM.name()));
        
        NumberType nt = new NumberType();
        String norm90 = nt.normalize(min);
        
        Iterator<QueryData> queries = config.getQueries();
        Assert.assertTrue(queries.hasNext());
        QueryData data = queries.next();
        for (IteratorSetting it : data.getSettings()) {
            if (it.getIteratorClass().equals(QueryIterator.class.getName())) {
                Map<String,String> options = it.getOptions();
                String qo = options.get(QueryOptions.QUERY);
                Assert.assertTrue(qo.contains(norm90));
            }
        }
    }
    
    @Test
    public void testRange() throws Exception {
        log.info("------  testRange  ------");
        
        String min = "90";
        String max = "122";
        String ohio = "'ohio'";
        String iowa = "'iowa'";
        String query = "(" + CityField.STATE.name() + EQ_OP + ohio + OR_OP + CityField.STATE.name() + EQ_OP + iowa + ")" + AND_OP + "((_Bounded_ = true) && ("
                        + CityField.NUM.name() + GT_OP + min + AND_OP + CityField.NUM.name() + LT_OP + max + "))";
        
        ShardQueryConfiguration config = (ShardQueryConfiguration) setupConfig(query);
        // verify NUM is NumberType
        String indexStr = config.getIndexedFieldDataTypesAsString();
        Assert.assertTrue(indexStr.contains(CityField.NUM.name() + ":" + NumberType.class.getName()));
        
        // NUM field should not be indexed
        Set<String> indexes = config.getIndexedFields();
        Assert.assertFalse(indexes.contains(CityField.NUM.name()));
        
        NumberType nt = new NumberType();
        String norm90 = nt.normalize(min);
        String norm122 = nt.normalize(max);
        
        Iterator<QueryData> queries = config.getQueries();
        Assert.assertTrue(queries.hasNext());
        QueryData data = queries.next();
        for (IteratorSetting it : data.getSettings()) {
            if (it.getIteratorClass().equals(QueryIterator.class.getName())) {
                Map<String,String> options = it.getOptions();
                String qo = options.get(QueryOptions.QUERY);
                Assert.assertTrue(qo.contains(norm90));
                Assert.assertTrue(qo.contains(norm122));
            }
        }
    }
    
    // end of unit tests
    // ============================================
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
