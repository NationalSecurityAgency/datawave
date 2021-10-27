package datawave.query.jexl.visitors;

import datawave.helpers.PrintUtility;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.DateIndexTestIngest;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.test.JexlNodeAssert;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;

/**
 * Test the function index query expansion
 */
public class DateIndexQueryExpansionVisitorTest {
    
    Authorizations auths = new Authorizations("HUSH");
    
    private static Connector connector = null;
    
    private Date startDate;
    private Date endDate;
    private MetadataHelper metadataHelper;
    private DateIndexHelper dateIndexHelper;
    
    @BeforeClass
    public static void before() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        InMemoryInstance i = new InMemoryInstance(DateIndexQueryExpansionVisitorTest.class.getName());
        connector = i.getConnector("root", new PasswordToken(""));
    }
    
    @Before
    public void setupTests() throws Exception {
        this.metadataHelper = new MetadataHelperFactory().createMetadataHelper(connector, TableName.DATE_INDEX, Collections.singleton(auths));
        this.deleteAndCreateTable();
        DateIndexTestIngest.writeItAll(connector);
        PrintUtility.printTable(connector, auths, TableName.DATE_INDEX);
        dateIndexHelper = new DateIndexHelperFactory().createDateIndexHelper().initialize(connector, TableName.DATE_INDEX, Collections.singleton(auths), 2,
                        0.9f);
    }
    
    private void deleteAndCreateTable() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        TableOperations tops = connector.tableOperations();
        if (tops.exists(TableName.DATE_INDEX)) {
            tops.delete(TableName.DATE_INDEX);
        }
        tops.create(TableName.DATE_INDEX);
    }
    
    @Test
    public void testDateIndexExpansion() throws Exception {
        givenStartDate("20100701");
        givenEndDate("20100710");
        
        String originalQuery = "filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000')";
        String expectedQuery = "(filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000') && (SHARDS_AND_DAYS = '20100703_0,20100704_0,20100704_2,20100705_1'))";
        
        assertExpansion(originalQuery, expectedQuery);
    }
    
    @Test
    public void testDateIndexExpansionWithTimeTravel() throws Exception {
        givenStartDate("20100701");
        givenEndDate("20100710");
        dateIndexHelper.setTimeTravel(true);
        
        String originalQuery = "filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000')";
        String expectedQuery = "(filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000') && (SHARDS_AND_DAYS = '20100702_0,20100703_0,20100704_0,20100704_2,20100705_1'))";
        
        assertExpansion(originalQuery, expectedQuery);
    }
    
    @Test
    public void testDateIndexExpansion1() throws Exception {
        givenStartDate("20100101");
        givenEndDate("20100102");
        
        String originalQuery = "filter:betweenDates(UPTIME, '20100101', '20100101')";
        String expectedQuery = "(filter:betweenDates(UPTIME, '20100101', '20100101') && (SHARDS_AND_DAYS = '20100101_1,20100102_2,20100102_4,20100102_5'))";
        
        assertExpansion(originalQuery, expectedQuery);
    }
    
    @Test
    public void testDateIndexExpansion2() throws Exception {
        givenStartDate("20100101_200000");
        givenEndDate("20100102_210000");
        
        String originalQuery = "filter:betweenDates(UPTIME, '20100101_200000', '20100101_210000')";
        String expectedQuery = "(filter:betweenDates(UPTIME, '20100101_200000', '20100101_210000') && (SHARDS_AND_DAYS = '20100101_1,20100102_2,20100102_4,20100102_5'))";
        
        assertExpansion(originalQuery, expectedQuery);
    }
    
    private void givenStartDate(String startDate) {
        this.startDate = DateHelper.parse(startDate);
    }
    
    private void givenEndDate(String endDate) {
        this.endDate = DateHelper.parse(endDate);
    }
    
    private void assertExpansion(String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setBeginDate(startDate);
        config.setEndDate(endDate);
        
        ASTJexlScript result = FunctionIndexQueryExpansionVisitor.expandFunctions(config, metadataHelper, dateIndexHelper, originalScript);
        
        JexlNodeAssert.assertThat(result).isEqualTo(expected).hasValidLineage();
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();
    }
    
}
