package datawave.query.jexl.visitors;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.helpers.PrintUtility;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.DateIndexTestIngest;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;

/**
 * Test the function index query expansion
 *
 *
 */
public class DateIndexQueryExpansionVisitorTest {
    
    private static final Logger log = Logger.getLogger(DateIndexQueryExpansionVisitorTest.class);
    
    Authorizations auths = new Authorizations("HUSH");
    
    private static AccumuloClient client = null;
    
    protected ShardQueryLogic logic = null;
    
    private MetadataHelper helper;
    
    @BeforeClass
    public static void before() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        InMemoryInstance i = new InMemoryInstance(DateIndexQueryExpansionVisitorTest.class.getName());
        client = new InMemoryAccumuloClient("root", i);
    }
    
    @Before
    public void setupTests() throws Exception {
        
        this.helper = new MetadataHelperFactory().createMetadataHelper(client, TableName.DATE_INDEX, Collections.singleton(auths));
        
        this.createTables();
        
        DateIndexTestIngest.writeItAll(client);
        PrintUtility.printTable(client, auths, TableName.DATE_INDEX);
    }
    
    @Test
    public void testDateIndexExpansion() throws Exception {
        String originalQuery = "filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000')";
        String expectedQuery = "(filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000') && (SHARDS_AND_DAYS = '20100703_0,20100704_0,20100704_2,20100705_1'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Date startDate = DateHelper.parse("20100701");
        Date endDate = DateHelper.parse("20100710");
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setBeginDate(startDate);
        config.setEndDate(endDate);
        DateIndexHelper helper2 = new DateIndexHelperFactory().createDateIndexHelper().initialize(client, TableName.DATE_INDEX, Collections.singleton(auths),
                        2, 0.9f);
        ASTJexlScript newScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2, script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        
        Assert.assertEquals(expectedQuery, newQuery);
    }
    
    @Test
    public void testDateIndexExpansionWithTimeTravel() throws Exception {
        String originalQuery = "filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000')";
        String expectedQuery = "(filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000') && (SHARDS_AND_DAYS = '20100702_0,20100703_0,20100704_0,20100704_2,20100705_1'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Date startDate = DateHelper.parse("20100701");
        Date endDate = DateHelper.parse("20100710");
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setBeginDate(startDate);
        config.setEndDate(endDate);
        
        DateIndexHelper helper2 = new DateIndexHelperFactory().createDateIndexHelper().initialize(client, TableName.DATE_INDEX, Collections.singleton(auths),
                        2, 0.9f);
        helper2.setTimeTravel(true);
        
        ASTJexlScript newScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2, script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        
        Assert.assertEquals(expectedQuery, newQuery);
    }
    
    @Test
    public void testDateIndexExpansion1() throws Exception {
        String originalQuery = "filter:betweenDates(UPTIME, '20100101', '20100101')";
        String expectedQuery = "(filter:betweenDates(UPTIME, '20100101', '20100101') && (SHARDS_AND_DAYS = '20100101_1,20100102_2,20100102_4,20100102_5'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        Date startDate = DateHelper.parse("20100101");
        Date endDate = DateHelper.parse("20100102");
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setBeginDate(startDate);
        config.setEndDate(endDate);
        DateIndexHelper helper2 = new DateIndexHelperFactory().createDateIndexHelper().initialize(client, TableName.DATE_INDEX, Collections.singleton(auths),
                        2, 0.9f);
        
        ASTJexlScript newScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2, script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        
        Assert.assertEquals(expectedQuery, newQuery);
    }
    
    @Test
    public void testDateIndexExpansion2() throws Exception {
        String originalQuery = "filter:betweenDates(UPTIME, '20100101_200000', '20100101_210000')";
        String expectedQuery = "(filter:betweenDates(UPTIME, '20100101_200000', '20100101_210000') && (SHARDS_AND_DAYS = '20100101_1,20100102_2,20100102_4,20100102_5'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        Date startDate = DateHelper.parse("20100101_200000");
        Date endDate = DateHelper.parse("20100102_210000");
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setBeginDate(startDate);
        config.setEndDate(endDate);
        DateIndexHelper helper2 = new DateIndexHelperFactory().createDateIndexHelper().initialize(client, TableName.DATE_INDEX, Collections.singleton(auths),
                        2, 0.9f);
        
        ASTJexlScript newScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2, script);
        
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        
        Assert.assertEquals(expectedQuery, newQuery);
    }
    
    private void createTables() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        TableOperations tops = client.tableOperations();
        deleteAndCreateTable(tops, TableName.DATE_INDEX);
    }
    
    private void deleteAndCreateTable(TableOperations tops, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
                    TableExistsException {
        if (tops.exists(tableName)) {
            tops.delete(tableName);
        }
        tops.create(tableName);
    }
}
