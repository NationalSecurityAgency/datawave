package datawave.query.tables.facets;

import com.google.common.collect.Sets;
import datawave.helpers.PrintUtility;
import datawave.marking.MarkingFunctions;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper.INTERRUPT;
import datawave.query.RebuildingScannerTestHelper.TEARDOWN;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.QueryLogicTestHarness;
import datawave.query.testframework.QueryLogicTestHarness.DocumentChecker;
import datawave.query.testframework.cardata.CarsDataType;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertTrue;

public class FacetedQueryLogicTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(FacetedQueryLogicTest.class);
    
    public FacetedQueryLogicTest() {
        super(CitiesDataType.getManager());
    }
    
    @BeforeClass
    public static void setupClass() throws Exception {
        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.COUNTRY.name());
        dataTypes.add(new FacetedCitiesDataType(CitiesDataType.CityEntry.generic, generic));
        dataTypes.add(new FacetedCitiesDataType(CitiesDataType.CityEntry.usa, generic));
        dataTypes.add(new FacetedCitiesDataType(CitiesDataType.CityEntry.italy, generic));
        dataTypes.add(new FacetedCitiesDataType(CitiesDataType.CityEntry.london, generic));
        dataTypes.add(new FacetedCitiesDataType(CitiesDataType.CityEntry.paris, generic));
        dataTypes.add(new FacetedCitiesDataType(CitiesDataType.CityEntry.rome, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        accumuloSetup.setAuthorizations(CitiesDataType.getTestAuths());
        connector = accumuloSetup.loadTables(log, TEARDOWN.EVERY_OTHER, INTERRUPT.NEVER);
    }
    
    @Before
    public void querySetUp() throws IOException {
        log.debug("---------  querySetUp  ---------");
        
        // Super call to pick up authSet initialization
        super.querySetUp();
        
        FacetedQueryLogic facetLogic = new FacetedQueryLogic();
        facetLogic.setFacetedSearchType(FacetedSearchType.FIELD_VALUE_FACETS);
        
        facetLogic.setFacetTableName(QueryTestTableHelper.FACET_TABLE_NAME);
        facetLogic.setFacetMetadataTableName(QueryTestTableHelper.FACET_METADATA_TABLE_NAME);
        facetLogic.setFacetHashTableName(QueryTestTableHelper.FACET_HASH_TABLE_NAME);
        
        facetLogic.setMaximumFacetGrouping(200);
        facetLogic.setMinimumFacet(1);
        
        this.logic = facetLogic;
        QueryTestTableHelper.configureLogicToScanTables(this.logic);
        
        this.logic.setFullTableScanEnabled(false);
        this.logic.setIncludeDataTypeAsField(true);
        this.logic.setIncludeGroupingContext(true);
        
        this.logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        this.logic.setMarkingFunctions(new MarkingFunctions.Default());
        this.logic.setMetadataHelperFactory(new MetadataHelperFactory());
        this.logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        
        // init must set auths
        testInit();
        
        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        DatawaveUser user = new DatawaveUser(dn, DatawaveUser.UserType.USER, Sets.newHashSet(this.auths.toString().split(",")), null, null, -1L);
        this.principal = new DatawavePrincipal(Collections.singleton(user));
        
        this.testHarness = new QueryLogicTestHarness(this);
    }
    
    @Test
    public void testQueryPrecomputedFacets() throws Exception {
        log.info("------ Test precomputed facet ------");
        
        Set<String> expected = new TreeSet<>();
        expected.add("CITY; florance -- florance//1");
        expected.add("CITY; london -- london//3");
        expected.add("CITY; milan -- milan//1");
        expected.add("CITY; naples -- naples//1");
        expected.add("CITY; palermo -- palermo//1");
        expected.add("CITY; paris -- paris//9");
        expected.add("CITY; rome -- rome//8"); // although there are 8 entries for rome, only 7 doc ids are unique in the test data.
        expected.add("CITY; turin -- turin//1");
        expected.add("CITY; venice -- venice//1");
        expected.add("CONTINENT; europe -- europe//26");
        expected.add("STATE; campania -- campania//1");
        expected.add("STATE; castilla y leon -- castilla y leon//1");
        expected.add("STATE; gelderland -- gelderland//1");
        expected.add("STATE; hainaut -- hainaut//3");
        expected.add("STATE; lazio -- lazio//5");
        expected.add("STATE; lle-de-france -- lle-de-france//3");
        expected.add("STATE; lombardia -- lombardia//1");
        expected.add("STATE; london -- london//2");
        expected.add("STATE; madrid -- madrid//2");
        expected.add("STATE; piemonte -- piemonte//1");
        expected.add("STATE; rhone-alps -- rhone-alps//2");
        expected.add("STATE; sicilia -- sicilia//1");
        expected.add("STATE; toscana -- toscana//1");
        expected.add("STATE; veneto -- veneto//1");
        expected.add("STATE; viana do castelo -- viana do castelo//1");
        
        String query = CitiesDataType.CityField.CONTINENT.name() + " == 'Europe'";
        
        runTest(query, Collections.emptyMap(), expected);
    }
    
    @Test
    public void testQueryDynamicFacets() throws Exception {
        log.info("------ Test dynamic facet ------");
        
        // TODO: this test isn't working properly. I would expect a query for Italy that is configured to facet
        // the CITY field - to return a facet for rome and paris, but also return a field name.
        Set<String> expected = new TreeSet<>();
        
        // @formatter:off
        expected.add("null; paris -- paris//1");
        expected.add("null; rome -- rome//2");
        // @formatter:on
        
        String query = CityField.COUNTRY.name() + " == 'Italy'";
        
        Map<String,String> options = new HashMap<>();
        options.put(FacetedConfiguration.FACETED_FIELDS, "CITY");
        options.put(FacetedConfiguration.FACETED_SEARCH_TYPE, FacetedSearchType.FIELD_VALUE_FACETS.name());
        
        runTest(query, options, expected);
    }
    
    public void runTest(String query, Map<String,String> options, Set<String> expected) throws Exception {
        final Date[] startEndDate = this.dataManager.getShardStartEndDate();
        
        // all results are verified bu the FacetDocumentChecker, although dummyExpected does imply that only
        // one document is expected as the result.
        final List<DocumentChecker> queryChecker = Collections.singletonList(new FacetDocumentChecker(expected));
        final Set<String> dummyExpected = Collections.singleton("");
        runTestQuery(dummyExpected, query, startEndDate[0], startEndDate[1], options, queryChecker);
    }
    
    @Override
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
    
    @Override
    public String parse(Key key, Document document) {
        // no-op. Everything handled in the FacetDocumentChecker below.
        return "";
    }
    
    public static class FacetDocumentChecker implements DocumentChecker {
        Set<String> expectedFacets;
        
        public FacetDocumentChecker(Set<String> expectedFacets) {
            this.expectedFacets = expectedFacets;
        }
        
        @Override
        public void assertValid(Document document) {
            Set<String> observedFacets = new TreeSet<>();
            document.getAttributes().forEach(k -> observedFacets.addAll(getValues(k)));
            
            Set<String> observedClone = new TreeSet<>(observedFacets);
            
            observedFacets.removeAll(expectedFacets);
            expectedFacets.removeAll(observedClone);
            
            StringBuilder errors = new StringBuilder();
            
            if (!observedFacets.isEmpty()) {
                errors.append("Observed unexpected results: " + observedFacets.toString());
            }
            
            if (!expectedFacets.isEmpty()) {
                if (errors.length() > 0) {
                    errors.append(", ");
                }
                errors.append("Did not observe expected results: " + expectedFacets.toString());
            }
            
            assertTrue(errors.toString(), observedFacets.isEmpty() && expectedFacets.isEmpty());
        }
        
        private static Set<String> getValues(Attribute<?> attr) {
            Set<String> values = new HashSet<>();
            if (attr instanceof Attributes) {
                for (Attribute<?> child : ((Attributes) attr).getAttributes()) {
                    values.addAll(getValues(child));
                }
            } else {
                String a = String.valueOf(attr.getData());
                String[] bits = a.split(", ");
                values.addAll(Arrays.asList(bits));
            }
            return values;
        }
    }
}
