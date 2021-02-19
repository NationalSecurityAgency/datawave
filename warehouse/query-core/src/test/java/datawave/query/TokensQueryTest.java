package datawave.query;

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GroupsDataType;
import datawave.query.testframework.GroupsDataType.GroupField;
import datawave.query.testframework.GroupsDataType.GroupsEntry;
import datawave.query.testframework.GroupsIndexConfiguration;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

/**
 * Tests for tokenized fields. The {@link GroupField#TOKENS} field was added to the {@link GroupsDataType} data. It is the only tokenized event.
 */
public class TokensQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(TokensQueryTest.class);
    
    private static final String[] TEST_CITIES = new String[] {"'salem'", "'olympia'", "'yuma'"};
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig fields = new GroupsIndexConfiguration();
        // add tokens to index fields so that any field expected results return correct entries
        fields.addIndexField(GroupField.TOKENS.getQueryField());
        dataTypes.add(new GroupsDataType(GroupsEntry.cities, fields));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public TokensQueryTest() {
        super(GroupsDataType.getManager());
    }
    
    @Test
    public void testTokenMatch() throws Exception {
        for (String city : TEST_CITIES) {
            String query = GroupField.TOKENS.name() + EQ_OP + city;
            runTest(query, query);
        }
    }
    
    @Test
    public void testFieldMatch() throws Exception {
        String token = "'nashua yuma'";
        String query = GroupField.TOKENS.name() + EQ_OP + token;
        runTest(query, query);
    }
    
    @Test
    public void testLuceneToken() throws Exception {
        String token = "yuma";
        String query = GroupField.TOKENS.name() + ":\"" + token + "\"~4";
        String expect = GroupField.TOKENS.name() + EQ_OP + "'" + token + "'";
        this.logic.setParser(new LuceneToJexlQueryParser());
        runTest(query, expect);
    }
    
    @Test
    public void testLuceneField() throws Exception {
        String token = "nashua yuma";
        String query = GroupField.TOKENS.name() + ":\"" + token + "\"~4";
        String expect = GroupField.TOKENS.name() + EQ_OP + "'" + token + "'";
        this.logic.setParser(new LuceneToJexlQueryParser());
        runTest(query, expect);
    }
    
    @Test
    public void testAnyField() throws Exception {
        for (String city : TEST_CITIES) {
            String cityPhrase = EQ_OP + city;
            String query = Constants.ANY_FIELD + cityPhrase;
            String anyCity = this.dataManager.convertAnyField(cityPhrase);
            runTest(query, anyCity);
        }
    }
    
    @Test
    public void testOrAnyField() throws Exception {
        String durham = EQ_OP + "'durham'";
        String olympia = EQ_OP + "'olympia'";
        String query = Constants.ANY_FIELD + olympia + OR_OP + Constants.ANY_FIELD + durham;
        String anyDurham = this.dataManager.convertAnyField(durham);
        String anyOlympia = this.dataManager.convertAnyField(olympia);
        String anyField = anyDurham + OR_OP + anyOlympia;
        runTest(query, anyField);
    }
    
    @Test
    public void testRegexAnyField() throws Exception {
        String regex = RE_OP + "'c.*'";
        String query = Constants.ANY_FIELD + regex;
        String anyCity = this.dataManager.convertAnyField(regex);
        runTest(query, anyCity);
    }
    
    // end of unit tests
    // ============================================
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = GroupsDataType.getTestAuths();
        this.documentKey = GroupField.EVENT_ID.name();
    }
}
