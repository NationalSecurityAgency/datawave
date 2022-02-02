package datawave.query;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TimingMetadata;
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
import datawave.query.testframework.QueryLogicTestHarness;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.EQ_OP;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryAuthsTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(QueryAuthsTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public QueryAuthsTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testAuthsEuro() throws Exception {
        log.info("----- testAuths Euro -----");
        String city = "'paris'";
        String query = CityField.CITY.name() + EQ_OP + city;
        
        Set<Authorizations> euroAuths = Collections.singleton(new Authorizations("Euro"));
        List<String> expectedEuro = new ArrayList<>();
        expectedEuro.add("par-ita-11");
        expectedEuro.add("par-fra-lle-7");
        
        final List<QueryLogicTestHarness.DocumentChecker> queryChecker = new ArrayList<>();
        final VisibilityChecker checker = new VisibilityChecker("Euro");
        queryChecker.add(checker);
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        runTestQuery(expectedEuro, query, startEndDate[0], startEndDate[1], Collections.emptyMap(), queryChecker, euroAuths);
    }
    
    @Test
    public void testAuthsMulti() throws Exception {
        log.info("----- testAuths Multi -----");
        String city = "'paris'";
        String query = CityField.CITY.name() + EQ_OP + city;
        
        Set<Authorizations> multiAuths = Collections.singleton(new Authorizations("Euro", "NA"));
        
        final Collection<String> expected = getExpectedKeyResponse(query);
        final List<QueryLogicTestHarness.DocumentChecker> queryChecker = new ArrayList<>();
        final VisibilityChecker checker = new VisibilityChecker("Euro", "NA");
        queryChecker.add(checker);
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        runTestQuery(expected, query, startEndDate[0], startEndDate[1], Collections.emptyMap(), queryChecker, multiAuths);
    }
    
    @Test
    public void testAuthsEmpty() throws Exception {
        log.info("----- testAuths Empty -----");
        String city = "'paris'";
        String query = CityField.CITY.name() + EQ_OP + city;
        
        Set<Authorizations> emptyAuths = Collections.singleton(new Authorizations());
        
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        runTestQuery(Collections.emptyList(), query, startEndDate[0], startEndDate[1], Collections.emptyMap(), Collections.emptyList(), emptyAuths);
    }
    
    @Test
    public void testAuthsEmptyIntersection() throws Exception {
        log.info("----- testAuths Empty Intersection -----");
        String city = "'paris'";
        String query = CityField.CITY.name() + EQ_OP + city;
        
        Set<Authorizations> emptyIntersectionAuths = new HashSet<>();
        emptyIntersectionAuths.add(new Authorizations("NA"));
        emptyIntersectionAuths.add(new Authorizations("Euro"));
        
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        runTestQuery(Collections.emptyList(), query, startEndDate[0], startEndDate[1], Collections.emptyMap(), Collections.emptyList(), emptyIntersectionAuths);
    }
    
    @Test
    public void testAuthsEuroIntersection() throws Exception {
        log.info("----- testAuths Euro Intersection -----");
        String city = "'paris'";
        String query = CityField.CITY.name() + EQ_OP + city;
        
        Set<Authorizations> euroIntersectionAuths = new HashSet<>();
        euroIntersectionAuths.add(new Authorizations("NA", "Euro"));
        euroIntersectionAuths.add(new Authorizations("Euro"));
        
        List<String> expectedEuro = new ArrayList<>();
        expectedEuro.add("par-ita-11");
        expectedEuro.add("par-fra-lle-7");
        
        final List<QueryLogicTestHarness.DocumentChecker> queryChecker = new ArrayList<>();
        final VisibilityChecker checker = new VisibilityChecker("Euro");
        queryChecker.add(checker);
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        runTestQuery(expectedEuro, query, startEndDate[0], startEndDate[1], Collections.emptyMap(), queryChecker, euroIntersectionAuths);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
    
    /**
     * Ensure that the visibilities returned match the auths that were specified for the query.
     */
    private static class VisibilityChecker implements QueryLogicTestHarness.DocumentChecker {
        
        private final String[] validVisibilities;
        private final Authorizations auths;
        private final VisibilityEvaluator filter;
        
        public VisibilityChecker(String... visibilities) {
            this.validVisibilities = visibilities;
            this.auths = new Authorizations(validVisibilities);
            this.filter = new VisibilityEvaluator(this.auths);
        }
        
        @Override
        public void assertValid(Document doc) {
            if (doc instanceof TimingMetadata) {
                return;
            }
            
            Map<String,Attribute<? extends Comparable<?>>> dict = doc.getDictionary();
            
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : dict.entrySet()) {
                String fieldName = entry.getKey();
                if (fieldName.equals(BaseRawData.EVENT_DATATYPE)) {
                    // does not include a visibility, why?
                    continue;
                }
                
                Attribute<?> attr = entry.getValue();
                
                if (attr instanceof Document) {
                    assertValid((Document) attr);
                } else if (attr instanceof Attributes) {
                    assertValid((Attributes) attr, fieldName);
                } else {
                    assertValid(attr, fieldName);
                }
            }
            
        }
        
        protected void assertValid(Attributes attrs, String fieldName) {
            for (Attribute<? extends Comparable<?>> attr : attrs.getAttributes()) {
                if (attr instanceof Document) {
                    assertValid((Document) attr);
                } else if (attr instanceof Attributes) {
                    assertValid((Attributes) attr, fieldName);
                } else {
                    assertValid(attr, fieldName);
                }
            }
        }
        
        protected void assertValid(Attribute<? extends Comparable<?>> attr, String fieldName) {
            final ColumnVisibility cv = attr.getColumnVisibility();
            if (log.isDebugEnabled()) {
                log.debug("field: '" + fieldName + "' value: '" + attr.getData().toString() + "' visibility:" + cv.toString());
            }
            
            try {
                assertTrue("Should not filter visibility: " + cv.toString(), filter.evaluate(cv));
            } catch (VisibilityParseException vpe) {
                fail("Could not parse visibility for field: " + fieldName + " visibility: " + cv.toString() + " exception: " + vpe.getMessage());
            }
        }
    }
}
