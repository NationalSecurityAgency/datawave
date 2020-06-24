package datawave.query.tables.facets;

import com.google.common.collect.Sets;
import datawave.data.type.Type;
import datawave.helpers.PrintUtility;
import datawave.marking.MarkingFunctions;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.RebuildingScannerTestHelper.INTERRUPT;
import datawave.query.RebuildingScannerTestHelper.TEARDOWN;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.planner.FacetedQueryPlanner;
import datawave.query.tables.IndexQueryLogicTest;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class FacetedQueryLogicTest extends AbstractFunctionalQuery {
    private static final Logger log = Logger.getLogger(FacetedQueryLogicTest.class);
    
    public FacetedQueryLogicTest() {
        super(CarsDataType.getManager());
    }
    
    @BeforeClass
    public static void setupClass() throws Exception {
        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.COUNTRY.name());
        dataTypes.add(new FacetedCitiesDataType(CitiesDataType.CityEntry.generic, generic));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log, TEARDOWN.EVERY_OTHER, INTERRUPT.NEVER);
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
        
        Set<String> expected = new HashSet<>(2);
        // @formatter:off
        expected.add("[" +
                "CITY; london -- london//2, " +
                "CITY; paris -- paris//2, " +
                "CITY; rome -- rome//2, " +
                "CONTINENT; europe -- europe//2, " +
                "STATE; hainaut -- hainaut//1, " +
                "STATE; lazio -- lazio//2, " +
                "STATE; lle-de-france -- lle-de-france//2, " +
                "STATE; london -- london//1" +
                "]");
        // @formatter:on
        String query = CitiesDataType.CityField.CONTINENT.name() + " == 'Europe'";
        
        runTest(query, Collections.emptyMap(), expected);
    }
    
    @Test
    public void testQueryDynamicFacets() throws Exception {
        log.info("------ Test dynamic facet ------");
        
        Set<String> expected = new HashSet<>(2);
        // TODO: this test isn't working properly. I would expect a query for Italy that is configured to facet
        // the CITY field - to return a facet for rome and paris (because there is a paris, italy entry in
        // generic-cities.csv
        // expected.add("[italy -- italy//1]");
        String query = CityField.COUNTRY.name() + " == 'Italy'";
        
        Map<String,String> options = new HashMap<>();
        options.put(FacetedConfiguration.FACETED_FIELDS, "CITY");
        options.put(FacetedConfiguration.FACETED_SEARCH_TYPE, FacetedSearchType.FIELD_VALUE_FACETS.name());
        
        runTest(query, options, expected);
    }
    
    public void runTest(String query, Map<String,String> options, Set<String> expected) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        final List<DocumentChecker> queryChecker = new ArrayList<>();
        runTestQuery(expected, query, startEndDate[0], startEndDate[1], options, queryChecker);
    }
    
    @Override
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
    
    @Override
    public String parse(Key key, Document document) {
        Set<String> sortedSet = new TreeSet<>();
        document.getAttributes().forEach(k -> sortedSet.addAll(getValues(k)));
        String result = sortedSet.toString();
        log.debug("Query result: " + result);
        return result;
    }
    
    private static Set<String> getValues(Attribute<?> attr) {
        Set<String> values = new HashSet<>();
        if (attr instanceof Attributes) {
            for (Attribute<?> child : ((Attributes) attr).getAttributes()) {
                values.addAll(getValues(child));
            }
        } else {
            values.add(String.valueOf(attr.getData()));
        }
        return values;
    }
    
}
