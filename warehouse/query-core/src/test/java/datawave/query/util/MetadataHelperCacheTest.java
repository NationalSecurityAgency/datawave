package datawave.query.util;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.deltaspike.core.api.config.Configuration;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import datawave.IdentityDataType;
import datawave.data.type.DateType;
import datawave.data.type.GeoType;
import datawave.query.composite.CompositeMetadataHelper;

// enable springification
@RunWith(SpringJUnit4ClassRunner.class)
// use the embedded Config class for all bean definitions
@ContextConfiguration(classes = {MetadataHelperCacheTest.Config.class})
// clear the context after each test so that the accumulo client expectations don't bleed between tests
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class MetadataHelperCacheTest extends EasyMockSupport {
    // wire up all the spring beans to get a workable cache
    @Configuration
    @EnableCaching
    static class Config {
        // autowiring the resources ensures the @Cacheable interface will activate, using new will not activate the spring cache
        @Autowired
        @Qualifier("typeMetadataHelper")
        private TypeMetadataHelper typeMetadataHelper;
        @Autowired
        @Qualifier("compositeMetadataHelper")
        private CompositeMetadataHelper compositeMetadataHelper;
        @Autowired
        @Qualifier("allFieldMetadataHelper")
        private AllFieldMetadataHelper allFieldMetadataHelper;

        @Autowired
        @Qualifier("alternateTableTypeHelper")
        private TypeMetadataHelper alternateTableTypeHelper;
        @Autowired
        @Qualifier("alternateTableCompositeHelper")
        private CompositeMetadataHelper alternateTableCompositeHelper;
        @Autowired
        @Qualifier("alternateTableAllFieldHelper")
        private AllFieldMetadataHelper alternateTableAllFieldHelper;

        @Autowired
        @Qualifier("alternateAuthsTypeHelper")
        private TypeMetadataHelper alternateAuthsTypeHelper;
        @Autowired
        @Qualifier("alternateAuthsCompositeHelper")
        private CompositeMetadataHelper alternateAuthsCompositeHelper;
        @Autowired
        @Qualifier("alternateAuthsAllFieldHelper")
        private AllFieldMetadataHelper alternateAuthsAllFieldHelper;

        @Autowired
        @Qualifier("alternateTypeHelper")
        private TypeMetadataHelper alternateTypeHelper;
        @Autowired
        @Qualifier("alternateCompositeHelper")
        private CompositeMetadataHelper alternateCompositeHelper;
        @Autowired
        @Qualifier("alternateAllFieldHelper")
        private AllFieldMetadataHelper alternateAllFieldHelper;

        public AccumuloClient accumuloClient = null;

        @Bean
        CacheManager metadataHelperCacheManager() {
            return new ConcurrentMapCacheManager();
        }

        // do not attempt to close this on destroy otherwise it triggers unexpected method calls on the mock
        @Bean(destroyMethod = "")
        // synchronized to ensure only one client is created per test
        synchronized AccumuloClient accumuloClient() {
            if (accumuloClient == null) {
                // since the context will be torn down with @DirtiesContext create a new mock for each new set of tests that will have clean expectations
                accumuloClient = EasyMock.createMock(AccumuloClient.class);
            }

            return accumuloClient;
        }

        @Bean
        Set<Authorizations> allMetadataAuths() {
            Set<Authorizations> auths = new HashSet<>();
            auths.add(new Authorizations("ALL"));

            return auths;
        }

        @Bean
        Set<Authorizations> alternateAllMetadataAuths() {
            Set<Authorizations> auths = new HashSet<>();
            auths.add(new Authorizations("ALTERNATE"));

            return auths;
        }

        @Bean
        TypeMetadataHelper typeMetadataHelper() {
            return new TypeMetadataHelper(Collections.EMPTY_MAP, allMetadataAuths(), accumuloClient(), "table", allMetadataAuths(), false);
        }

        @Bean
        TypeMetadataHelper alternateTableTypeHelper() {
            return new TypeMetadataHelper(Collections.EMPTY_MAP, allMetadataAuths(), accumuloClient(), "table2", allMetadataAuths(), false);
        }

        @Bean
        TypeMetadataHelper alternateAuthsTypeHelper() {
            return new TypeMetadataHelper(Collections.EMPTY_MAP, alternateAllMetadataAuths(), accumuloClient(), "table", alternateAllMetadataAuths(), false);
        }

        @Bean
        TypeMetadataHelper alternateTypeHelper() {
            return new TypeMetadataHelper(Collections.EMPTY_MAP, alternateAllMetadataAuths(), accumuloClient(), "table2", alternateAllMetadataAuths(), false);
        }

        @Bean
        CompositeMetadataHelper compositeMetadataHelper() {
            return new CompositeMetadataHelper(accumuloClient(), "table", allMetadataAuths());
        }

        @Bean
        CompositeMetadataHelper alternateTableCompositeHelper() {
            return new CompositeMetadataHelper(accumuloClient(), "table2", allMetadataAuths());
        }

        @Bean
        CompositeMetadataHelper alternateAuthsCompositeHelper() {
            return new CompositeMetadataHelper(accumuloClient(), "table", alternateAllMetadataAuths());
        }

        @Bean
        CompositeMetadataHelper alternateCompositeHelper() {
            return new CompositeMetadataHelper(accumuloClient(), "table2", alternateAllMetadataAuths());
        }

        @Bean
        AllFieldMetadataHelper allFieldMetadataHelper() {
            return new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper, accumuloClient(), "table", allMetadataAuths(), allMetadataAuths());
        }

        @Bean
        AllFieldMetadataHelper alternateTableAllFieldHelper() {
            return new AllFieldMetadataHelper(alternateTableTypeHelper, alternateTableCompositeHelper, accumuloClient(), "table2", allMetadataAuths(),
                            allMetadataAuths());
        }

        @Bean
        AllFieldMetadataHelper alternateAuthsAllFieldHelper() {
            return new AllFieldMetadataHelper(alternateAuthsTypeHelper, alternateAuthsCompositeHelper, accumuloClient(), "table", alternateAllMetadataAuths(),
                            alternateAllMetadataAuths());
        }

        @Bean
        AllFieldMetadataHelper alternateAllFieldHelper() {
            return new AllFieldMetadataHelper(alternateTypeHelper, alternateCompositeHelper, accumuloClient(), "table2", alternateAllMetadataAuths(),
                            alternateAllMetadataAuths());
        }

        @Bean
        MetadataHelper metadataHelper() {
            return new MetadataHelper(allFieldMetadataHelper, allMetadataAuths(), accumuloClient(), "table", allMetadataAuths(), allMetadataAuths());
        }

        @Bean
        MetadataHelper alternateTableMetadataHelper() {
            return new MetadataHelper(alternateTableAllFieldHelper, allMetadataAuths(), accumuloClient(), "table2", allMetadataAuths(), allMetadataAuths());
        }

        @Bean
        MetadataHelper alternateAuthsHelper() {
            return new MetadataHelper(alternateAuthsAllFieldHelper, alternateAllMetadataAuths(), accumuloClient(), "table", alternateAllMetadataAuths(),
                            alternateAllMetadataAuths());
        }

        @Bean
        MetadataHelper alternateHelper() {
            return new MetadataHelper(alternateAllFieldHelper, alternateAllMetadataAuths(), accumuloClient(), "table2", alternateAllMetadataAuths(),
                            alternateAllMetadataAuths());
        }

        @Bean
        TestCache testCache() {
            return new TestCache();
        }
    }

    // inject all beans into the test class so they will be spring cache enabled
    @Autowired
    @Qualifier("metadataHelper")
    private MetadataHelper metadataHelper;

    @Autowired
    @Qualifier("alternateTableMetadataHelper")
    private MetadataHelper alternateTableMetadataHelper;

    @Autowired
    @Qualifier("alternateAuthsHelper")
    private MetadataHelper alternateAuthsHelper;

    @Autowired
    @Qualifier("alternateHelper")
    private MetadataHelper alternateHelper;

    @Autowired
    private AccumuloClient accumuloClient;

    @Autowired
    private CacheManager cacheManager;

    @Before
    public void setup() {
        // ensure @Autowired is working
        assertNotNull(accumuloClient);
        assertNotNull(metadataHelper);
        assertNotNull(alternateTableMetadataHelper);
        assertNotNull(alternateAuthsHelper);
        assertNotNull(alternateHelper);
        assertNotNull(cacheManager);
    }

    private AccumuloClient getAccumuloClient() {
        return accumuloClient;
    }

    // this test will fail until MetadataHelper.getQueryModelNames uses the right key
    @Test
    public void getModelTableNames_param_test() throws TableNotFoundException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        expectScanner("t1", s, entries);

        expectScanner("t2", s, entries);

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getQueryModelNames("t1");
        // this call should *NOT* hit cache
        metadataHelper.getQueryModelNames("t2");
        // this call should hit cache
        metadataHelper.getQueryModelNames("t2");

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getFacets_param_test() throws TableNotFoundException, InstantiationException, IllegalAccessException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("t1", s, entries);

        // call 2
        expectScanner("t2", s, entries);

        // call 3 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getFacets("t1");
        // this call should *NOT* hit cache
        metadataHelper.getFacets("t2");
        // this call should hit cache t2
        metadataHelper.getFacets("t2");

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getTermCounts_param_test() throws TableNotFoundException, InstantiationException, IllegalAccessException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);

        // call 2 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getTermCounts();
        // this call should hit cache
        metadataHelper.getTermCounts();

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getTermCountsWithRootAuths_param_test()
                    throws TableNotFoundException, InstantiationException, IllegalAccessException, AccumuloException, AccumuloSecurityException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);

        // call 2 cached

        // mock the security check for call 1
        SecurityOperations so = createMock(SecurityOperations.class);
        expect(accumuloClient.securityOperations()).andReturn(so);
        expect(accumuloClient.whoami()).andReturn("dwv");
        expect(so.getUserAuthorizations("dwv")).andReturn(new Authorizations("steve"));

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getTermCountsWithRootAuths();
        // this call should hit cache
        metadataHelper.getTermCountsWithRootAuths();

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getTermCounts_getTermCountsWithRootAuths_notShared_test()
                    throws TableNotFoundException, InstantiationException, IllegalAccessException, AccumuloException, AccumuloSecurityException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);

        // call 2
        expectScanner("table", s, entries);
        SecurityOperations so = createMock(SecurityOperations.class);
        expect(accumuloClient.securityOperations()).andReturn(so);
        expect(accumuloClient.whoami()).andReturn("dwv");
        expect(so.getUserAuthorizations("dwv")).andReturn(new Authorizations("steve"));

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getTermCounts();
        // this call should *not* hit cache
        metadataHelper.getTermCountsWithRootAuths();

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getAllNormalized_param_test() throws TableNotFoundException, InstantiationException, IllegalAccessException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);

        // call 2 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getAllNormalized();
        // this call should hit cache
        metadataHelper.getAllNormalized();

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getEdges_param_test() throws TableNotFoundException, InstantiationException, IllegalAccessException, ExecutionException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);
        // extra scanner expectations
        s.addScanIterator(anyObject());
        s.addScanIterator(anyObject());

        // call 2 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getEdges();
        // this call should hit cache
        metadataHelper.getEdges();

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getTermFrequencyFields_param_test() throws TableNotFoundException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);

        // call 2
        expectScanner("table", s, entries);

        // call 3
        expectScanner("table", s, entries);

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getTermFrequencyFields(null);
        // this call should *NOT* hit cache
        metadataHelper.getTermFrequencyFields(Collections.emptySet());
        // this call should *NOT* hit cache
        metadataHelper.getTermFrequencyFields(Collections.singleton("mega"));

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getTermFrequencyFields_alternateKey_test() throws TableNotFoundException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);

        // call 2
        expectScanner("table2", s, entries);

        // call 3
        expectScanner("table", s, entries);

        // call 4
        expectScanner("table2", s, entries);

        // call 5-8 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        assertNotNull(alternateTableMetadataHelper);
        assertNotNull(alternateAuthsHelper);
        assertNotNull(alternateHelper);

        metadataHelper.getTermFrequencyFields(null);
        // this call should *NOT* hit cache
        alternateTableMetadataHelper.getTermFrequencyFields(null);
        // this call should *NOT* hit cache
        alternateAuthsHelper.getTermFrequencyFields(null);
        // this call should *NOT* hit cache
        alternateHelper.getTermFrequencyFields(null);

        // cached calls
        metadataHelper.getTermFrequencyFields(null);
        alternateTableMetadataHelper.getTermFrequencyFields(null);
        alternateAuthsHelper.getTermFrequencyFields(null);
        alternateHelper.getTermFrequencyFields(null);

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getAllFields_param_test() throws TableNotFoundException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);

        // call 2 cached

        // call 3 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getAllFields(null);
        // this call should hit cache
        metadataHelper.getAllFields(Collections.emptySet());
        // this call should hit cache
        metadataHelper.getAllFields(Collections.singleton("mega"));

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    // this test will fail until AllFieldMetadataHelper.getIndexOnlyFields() properly closes its scanner
    @Test
    public void getNonEventFields_param_test() throws TableNotFoundException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        // getIndexOnlyFields
        expectScanner("table", s, entries);
        // getTermFrequencyFields
        expectScanner("table", s, entries);
        // getCompositeToFieldMap
        expectScanner("table", s, entries);

        // call 2
        // getIndexOnlyFields cached
        // getTermFrequencyFields
        expectScanner("table", s, entries);
        // getCompositeToFieldMap
        expectScanner("table", s, entries);

        // call 3
        // getIndexOnlyFields cached
        // getTermFrequencyFields
        expectScanner("table", s, entries);
        // getCompositeToFieldMap
        expectScanner("table", s, entries);

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getNonEventFields(null);
        // this call should *NOT* hit cache
        metadataHelper.getNonEventFields(Collections.emptySet());
        // this call should *NOT* hit cache
        metadataHelper.getNonEventFields(Collections.singleton("mega"));

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    // this test will fail until AllFieldMetadataHelper.getIndexOnlyFields() properly closes its scanner
    @Test
    public void getIndexOnlyFields_param_test() throws TableNotFoundException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        // getIndexOnlyFields
        expectScanner("table", s, entries);

        // call 2 cached

        // call 3 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getIndexOnlyFields(null);
        // this call should hit cache
        metadataHelper.getIndexOnlyFields(Collections.emptySet());
        // this call should hit cache
        metadataHelper.getIndexOnlyFields(Collections.singleton("mega"));

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void isReverseIndexed_param_test() throws TableNotFoundException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);
        // call 2
        expectScanner("table", s, entries);
        // call 3 cached
        // call 4
        expectScanner("table", s, entries);

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.isReverseIndexed("myField", Collections.emptySet());
        // this call should *NOT* hit the cache
        metadataHelper.isReverseIndexed("myField2", Collections.emptySet());
        // this call should hit the cache
        metadataHelper.isReverseIndexed("myField2", Collections.emptySet());
        // this call should *NOT* hit the cache
        metadataHelper.isReverseIndexed("myField2", Collections.singleton("ingestTypeFilter1"));

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void isIndexed_param_test() throws TableNotFoundException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);
        // call 2 cached
        // call 3
        expectScanner("table", s, entries);
        // call 4
        expectScanner("table", s, entries);

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.isIndexed("myField", Collections.emptySet());
        // this call should hit the cache
        metadataHelper.isIndexed("myField", Collections.emptySet());
        // this call should *NOT* hit the cache
        metadataHelper.isIndexed("myField2", Collections.emptySet());
        // this call should *NOT* hit the cache
        metadataHelper.isIndexed("myField2", Collections.singleton("ingestTypeFilter1"));

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void isTokenized_param_test() throws TableNotFoundException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);
        // call 2 cached
        // call 3
        expectScanner("table", s, entries);
        // call 4
        expectScanner("table", s, entries);

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.isTokenized("myField", Collections.emptySet());
        // this call should hit the cache
        metadataHelper.isTokenized("myField", Collections.emptySet());
        // this call should *NOT* hit the cache
        metadataHelper.isTokenized("myField2", Collections.emptySet());
        // this call should *NOT* hit the cache
        metadataHelper.isTokenized("myField2", Collections.singleton("ingestTypeFilter1"));

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getAllDatatypes_param_test() throws TableNotFoundException, InstantiationException, IllegalAccessException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);
        // call 2 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getAllDatatypes();
        // this call should hit the cache
        metadataHelper.getAllDatatypes();

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getCompositeToFieldMap_param_test() throws TableNotFoundException, InstantiationException, IllegalAccessException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);
        // call 2 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getCompositeToFieldMap();
        // this call should hit the cache
        metadataHelper.getCompositeToFieldMap();

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    @Test
    public void getCompositeToFieldMap_param2_test() throws TableNotFoundException, InstantiationException, IllegalAccessException {
        Scanner s = createMock(Scanner.class);

        List<Map.Entry<Key,Value>> entries = new ArrayList<>();

        // call 1
        expectScanner("table", s, entries);
        // call 2
        expectScanner("table", s, entries);
        // call 3
        expectScanner("table", s, entries);
        // call 4
        expectScanner("table", s, entries);
        // call 5 cached
        // call 6 cached

        EasyMock.replay(accumuloClient);
        replayAll();

        assertNotNull(metadataHelper);
        metadataHelper.getCompositeToFieldMap();
        // this call should *NOT* hit the cache
        metadataHelper.getCompositeToFieldMap(null);
        // this call should *NOT* hit the cache
        metadataHelper.getCompositeToFieldMap(Collections.emptySet());
        // this call should *NOT* hit the cache
        metadataHelper.getCompositeToFieldMap(Collections.singleton("test"));
        // this call should hit the cache
        metadataHelper.getCompositeToFieldMap(null);
        // this call should hit the cache
        metadataHelper.getCompositeToFieldMap(Collections.singleton("test"));

        EasyMock.verify(accumuloClient);
        verifyAll();
    }

    // key = "{#root.target.auths,#modelTableName}"
    private MethodParamsAndExpectations[] getExpectedQueryModelNames() {
        MethodParamsAndExpectations base = new MethodParamsAndExpectations(metadataHelper, new Object[] {"modelName"}) {
            public void expect() throws TableNotFoundException {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("modelName", s, entries);
            }
        };

        MethodParamsAndExpectations newModelName = new MethodParamsAndExpectations(metadataHelper, new Object[] {"newModelName"}) {
            public void expect() throws TableNotFoundException {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("newModelName", s, entries);
            }
        };

        MethodParamsAndExpectations altAuths = new MethodParamsAndExpectations(alternateAuthsHelper, new Object[] {"modelName"}) {
            public void expect() throws TableNotFoundException {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("modelName", s, entries);
            }
        };

        // @formatter:off
        return new MethodParamsAndExpectations[] {
                // base case
                base,
                // different metadataTable
                new MethodParamsAndExpectations(alternateTableMetadataHelper, new Object[] {"modelName"}, base),
                // different auths, not cached
                altAuths,
                // different table and auths, but should be cached by the previous alt auths call
                new MethodParamsAndExpectations(alternateHelper, new Object[] {"modelName"}, altAuths),
                // different modelTableName
                newModelName,
                // same modelTableName repeated
                new MethodParamsAndExpectations(metadataHelper, new Object[] {"newModelName"}, newModelName)
        };
        // @formatter:on
    }

    // key = "{#root.target.auths,#table}"
    private MethodParamsAndExpectations[] getExpectedFacets() {
        MethodParamsAndExpectations base = new MethodParamsAndExpectations(metadataHelper, new Object[] {"t1"}) {
            public void expect() throws TableNotFoundException {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("t1", s, entries);
            }
        };

        MethodParamsAndExpectations newTableName = new MethodParamsAndExpectations(metadataHelper, new Object[] {"t2"}) {
            public void expect() throws TableNotFoundException {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("t2", s, entries);
            }
        };

        MethodParamsAndExpectations altAuths = new MethodParamsAndExpectations(alternateAuthsHelper, new Object[] {"t1"}) {
            public void expect() throws TableNotFoundException {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("t1", s, entries);
            }
        };

        // @formatter:off
        return new MethodParamsAndExpectations[] {
                // base case
                base,
                // different metadataTable
                new MethodParamsAndExpectations(alternateTableMetadataHelper, new Object[] {"t1"}, base),
                // different auths, not cached
                altAuths,
                // different table and auths, but should be cached by the previous alt auths call
                new MethodParamsAndExpectations(alternateHelper, new Object[] {"t1"}, altAuths),
                // different modelTableName
                newTableName,
                // same modelTableName repeated
                new MethodParamsAndExpectations(metadataHelper, new Object[] {"t2"}, newTableName)
        };
        // @formatter:on
    }

    /**
     * Create expectations for no argument method calls for each of the metadataHelper objects. Verify a second call returns the same result as the first
     * without additional accumulo calls. When extra is set, also expect this on the initial call
     *
     * @param extra
     * @return
     */
    private MethodParamsAndExpectations[] getNoArgMetadataTableNameVariations(ExtraExpectation extra) {
        MethodParamsAndExpectations base = new MethodParamsAndExpectations(metadataHelper, new Object[] {}) {
            public void expect() throws Exception {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("table", s, entries);

                if (extra != null) {
                    extra.extraExpectation(s);
                }
            }
        };

        MethodParamsAndExpectations altTable = new MethodParamsAndExpectations(alternateTableMetadataHelper, new Object[] {}) {
            public void expect() throws Exception {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("table2", s, entries);

                if (extra != null) {
                    extra.extraExpectation(s);
                }
            }
        };

        // @formatter:off
        return new MethodParamsAndExpectations[] {
                // base case
                base,
                // different metadataTable
                altTable,
                // different auths
                new MethodParamsAndExpectations(alternateAuthsHelper, new Object[] {}, base),
                // different table and auths,
                new MethodParamsAndExpectations(alternateHelper, new Object[] {}, altTable),
                new MethodParamsAndExpectations(metadataHelper, new Object[] {}, base),
                new MethodParamsAndExpectations(alternateTableMetadataHelper, new Object[] {}, altTable),
                new MethodParamsAndExpectations(alternateAuthsHelper, new Object[] {}, base),
                new MethodParamsAndExpectations(alternateHelper, new Object[] {}, altTable),
        };
        // @formatter:on
    }

    // validate key = "{#root.target.auths,#root.target.metadataTableName}"
    private MethodParamsAndExpectations[] getNoArgAuthMetadataTableNameVariations(ExtraExpectation extra) {
        MethodParamsAndExpectations base = new MethodParamsAndExpectations(metadataHelper, new Object[] {}) {
            public void expect() throws Exception {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("table", s, entries);

                if (extra != null) {
                    extra.extraExpectation(s);
                }
            }
        };

        MethodParamsAndExpectations altAuths = new MethodParamsAndExpectations(alternateAuthsHelper, new Object[] {}) {
            public void expect() throws Exception {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("table", s, entries);

                if (extra != null) {
                    extra.extraExpectation(s);
                }
            }
        };

        MethodParamsAndExpectations altTable = new MethodParamsAndExpectations(alternateTableMetadataHelper, new Object[] {}) {
            public void expect() throws Exception {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("table2", s, entries);

                if (extra != null) {
                    extra.extraExpectation(s);
                }
            }
        };

        MethodParamsAndExpectations altTableAndAuths = new MethodParamsAndExpectations(alternateHelper, new Object[] {}) {
            public void expect() throws Exception {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner("table2", s, entries);

                if (extra != null) {
                    extra.extraExpectation(s);
                }
            }
        };

        // @formatter:off
        return new MethodParamsAndExpectations[] {
                // base case
                base,
                // different metadataTable
                altTable,
                // different auths
                altAuths,
                // different table and auths,
                altTableAndAuths,
                new MethodParamsAndExpectations(metadataHelper, new Object[] {}, base),
                new MethodParamsAndExpectations(alternateTableMetadataHelper, new Object[] {}, altTable),
                new MethodParamsAndExpectations(alternateAuthsHelper, new Object[] {}, altAuths),
                new MethodParamsAndExpectations(alternateHelper, new Object[] {}, altTableAndAuths),
        };
        // @formatter:on
    }

    private MethodParamsAndExpectations[] getAuthMetadataTableNameVariations(Args[] args, boolean argsInKey) {
        return getAuthMetadataTableNameVariations(args, argsInKey, true);
    }

    // validate key = "{#root.target.auths,#root.target.metadataTableName}"
    private MethodParamsAndExpectations[] getAuthMetadataTableNameVariations(Args[] args, boolean argsInKey, boolean exactMatch) {
        if (args == null) {
            args = new Args[] {new Args(new Object[] {})};
        }
        List<MethodParamsAndExpectations> results = new ArrayList<>();

        boolean first = true;

        MethodParamsAndExpectations base = null;
        MethodParamsAndExpectations altAuths = null;
        MethodParamsAndExpectations altTable = null;
        MethodParamsAndExpectations altTableAndAuths = null;

        for (Args a : args) {
            if (first || argsInKey) {
                base = new MethodParamsAndExpectations(metadataHelper, a.args) {
                    public void expect() throws TableNotFoundException {
                        Scanner s = createMock(Scanner.class);
                        List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                        expectScanner("table", s, entries);
                    }
                };

                altAuths = new MethodParamsAndExpectations(alternateAuthsHelper, a.args) {
                    public void expect() throws TableNotFoundException {
                        Scanner s = createMock(Scanner.class);
                        List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                        expectScanner("table", s, entries);
                    }
                };

                altTable = new MethodParamsAndExpectations(alternateTableMetadataHelper, a.args) {
                    public void expect() throws TableNotFoundException {
                        Scanner s = createMock(Scanner.class);
                        List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                        expectScanner("table2", s, entries);
                    }
                };

                altTableAndAuths = new MethodParamsAndExpectations(alternateHelper, a.args) {
                    public void expect() throws TableNotFoundException {
                        Scanner s = createMock(Scanner.class);
                        List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                        expectScanner("table2", s, entries);
                    }
                };

                results.add(base);
                results.add(altTable);
                results.add(altAuths);
                results.add(altTableAndAuths);

                first = false;
            }

            results.add(new MethodParamsAndExpectations(metadataHelper, a.args, base, exactMatch));
            results.add(new MethodParamsAndExpectations(alternateTableMetadataHelper, a.args, altTable, exactMatch));
            results.add(new MethodParamsAndExpectations(alternateAuthsHelper, a.args, altAuths, exactMatch));
            results.add(new MethodParamsAndExpectations(alternateHelper, a.args, altTableAndAuths, exactMatch));
        }

        return results.toArray(new MethodParamsAndExpectations[0]);
    }

    private MethodParamsAndExpectations[] getAuthMetadataTableNameVariations(Args[] args, boolean argsInKey, boolean exactMatch, Expectation alwaysExpect) {
        return getAuthMetadataTableNameVariations(args, argsInKey, exactMatch, alwaysExpect, 1);
    }

    private MethodParamsAndExpectations[] getAuthMetadataTableNameVariations(Args[] args, boolean argsInKey, boolean exactMatch, Expectation alwaysExpect,
                    int callsPerCache) {
        if (args == null) {
            args = new Args[] {new Args(new Object[] {})};
        }
        List<MethodParamsAndExpectations> results = new ArrayList<>();

        boolean first = true;

        MethodParamsAndExpectations base = null;
        MethodParamsAndExpectations altAuths = null;
        MethodParamsAndExpectations altTable = null;
        MethodParamsAndExpectations altTableAndAuths = null;

        for (Args a : args) {
            if (first || argsInKey) {
                base = new MethodParamsAndExpectations(metadataHelper, a.args) {
                    public void expect() throws Exception {
                        for (int i = 0; i < callsPerCache; i++) {
                            Scanner s = createMock(Scanner.class);
                            List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                            expectScanner("table", s, entries);
                        }

                        if (alwaysExpect != null) {
                            alwaysExpect.expect();
                        }
                    }
                };

                altAuths = new MethodParamsAndExpectations(alternateAuthsHelper, a.args) {
                    public void expect() throws Exception {
                        for (int i = 0; i < callsPerCache; i++) {
                            Scanner s = createMock(Scanner.class);
                            List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                            expectScanner("table", s, entries);
                        }

                        if (alwaysExpect != null) {
                            alwaysExpect.expect();
                        }
                    }
                };

                altTable = new MethodParamsAndExpectations(alternateTableMetadataHelper, a.args) {
                    public void expect() throws Exception {
                        for (int i = 0; i < callsPerCache; i++) {
                            Scanner s = createMock(Scanner.class);
                            List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                            expectScanner("table2", s, entries);
                        }

                        if (alwaysExpect != null) {
                            alwaysExpect.expect();
                        }
                    }
                };

                altTableAndAuths = new MethodParamsAndExpectations(alternateHelper, a.args) {
                    public void expect() throws Exception {
                        for (int i = 0; i < callsPerCache; i++) {
                            Scanner s = createMock(Scanner.class);
                            List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                            expectScanner("table2", s, entries);
                        }

                        if (alwaysExpect != null) {
                            alwaysExpect.expect();
                        }
                    }
                };

                results.add(base);
                results.add(altTable);
                results.add(altAuths);
                results.add(altTableAndAuths);

                first = false;
            }

            results.add(new MethodParamsAndExpectations(metadataHelper, a.args, base, exactMatch, alwaysExpect));
            results.add(new MethodParamsAndExpectations(alternateTableMetadataHelper, a.args, altTable, exactMatch, alwaysExpect));
            results.add(new MethodParamsAndExpectations(alternateAuthsHelper, a.args, altAuths, exactMatch, alwaysExpect));
            results.add(new MethodParamsAndExpectations(alternateHelper, a.args, altTableAndAuths, exactMatch, alwaysExpect));
        }

        return results.toArray(new MethodParamsAndExpectations[0]);
    }

    private MethodParamsAndExpectations getWithExpectation(MetadataHelper helper, Object[] args, MethodParamsAndExpectations cachedObject, boolean exactMatch,
                    Expectation expectation) {
        return new MethodParamsAndExpectations(helper, args, cachedObject, exactMatch) {
            public void expect() throws Exception {
                expectation.expect();
            }
        };
    }

    // this is a special case because the cache layers are different on the dependent methods if they come into line this can be folded into the generic
    // expectation setter
    // getTermFrequencyFields and getCompositeToFieldMap cache on key = auths,metadataTable,ingestTypeFilers
    // getIndexOnlyFields caches on key = auths,metadataTable
    private MethodParamsAndExpectations[] getNonEventFieldExpectations(Args[] args) {
        List<MethodParamsAndExpectations> results = new ArrayList<>();

        MethodParamsAndExpectations base = getWithExpectation(metadataHelper, args[0].args, null, false, new ScannerExpectation("table", 3));
        MethodParamsAndExpectations baseAltTable = getWithExpectation(alternateTableMetadataHelper, args[0].args, null, false,
                        new ScannerExpectation("table2", 3));
        MethodParamsAndExpectations baseAltAuths = getWithExpectation(alternateAuthsHelper, args[0].args, null, false, new ScannerExpectation("table", 3));
        MethodParamsAndExpectations baseAlt = getWithExpectation(alternateHelper, args[0].args, null, false, new ScannerExpectation("table2", 3));

        // no cache hits across any variation here
        results.add(base);
        results.add(baseAltTable);
        results.add(baseAltAuths);
        results.add(baseAlt);

        // should have cache hits that match values on all replays, but getTermFrequencyFields() is a special case
        // spring doesn't intercept cache calls within the same class. See
        // https://spring.io/blog/2012/05/23/transactions-caching-and-aop-understanding-proxy-usage-in-spring
        results.add(getWithExpectation(metadataHelper, args[0].args, base, false, new ScannerExpectation("table", 1)));
        results.add(getWithExpectation(alternateTableMetadataHelper, args[0].args, baseAltTable, false, new ScannerExpectation("table2", 1)));
        results.add(getWithExpectation(alternateAuthsHelper, args[0].args, baseAltAuths, false, new ScannerExpectation("table", 1)));
        results.add(getWithExpectation(alternateHelper, args[0].args, baseAlt, false, new ScannerExpectation("table2", 1)));

        // after the first pass all the others should be the same
        for (int i = 1; i < args.length; i++) {
            // for each object test first call and cached call
            base = getWithExpectation(metadataHelper, args[i].args, null, false, new ScannerExpectation("table", 2));
            results.add(base);
            results.add(getWithExpectation(metadataHelper, args[i].args, base, false, new ScannerExpectation("table", 1)));

            baseAltTable = getWithExpectation(alternateTableMetadataHelper, args[i].args, null, false, new ScannerExpectation("table2", 2));
            results.add(baseAltTable);
            results.add(getWithExpectation(alternateTableMetadataHelper, args[i].args, baseAltTable, false, new ScannerExpectation("table2", 1)));

            baseAltAuths = getWithExpectation(alternateAuthsHelper, args[i].args, null, false, new ScannerExpectation("table", 2));
            results.add(baseAltAuths);
            results.add(getWithExpectation(alternateAuthsHelper, args[i].args, baseAltAuths, false, new ScannerExpectation("table", 1)));

            baseAlt = getWithExpectation(alternateHelper, args[i].args, null, false, new ScannerExpectation("table2", 2));
            results.add(baseAlt);
            results.add(getWithExpectation(alternateHelper, args[i].args, baseAlt, false, new ScannerExpectation("table2", 1)));
        }

        return results.toArray(new MethodParamsAndExpectations[0]);
    }

    private MethodParamsAndExpectations[] getScansPerCall(Args[] args, int scansPerTable) {
        return getScansPerCall(args, scansPerTable, null);
    }

    private MethodParamsAndExpectations[] getScansPerCall(Args[] args, int scansPerTable, ExtraExpectation extra) {
        List<MethodParamsAndExpectations> results = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            MethodParamsAndExpectations base = getWithExpectation(metadataHelper, args[i].args, null, false,
                            new ScannerExpectation("table", scansPerTable, extra));
            MethodParamsAndExpectations baseAltTable = getWithExpectation(alternateTableMetadataHelper, args[i].args, null, false,
                            new ScannerExpectation("table2", scansPerTable, extra));
            MethodParamsAndExpectations baseAltAuths = getWithExpectation(alternateAuthsHelper, args[i].args, null, false,
                            new ScannerExpectation("table", scansPerTable, extra));
            MethodParamsAndExpectations baseAlt = getWithExpectation(alternateHelper, args[i].args, null, false,
                            new ScannerExpectation("table2", scansPerTable, extra));

            // no cache hits across any variation here
            results.add(base);
            results.add(baseAltTable);
            results.add(baseAltAuths);
            results.add(baseAlt);

            // no caching is happening so should be exactly the same for any follow up calls
            results.add(getWithExpectation(metadataHelper, args[i].args, base, false, new ScannerExpectation("table", scansPerTable, extra)));
            results.add(getWithExpectation(alternateTableMetadataHelper, args[i].args, baseAltTable, false,
                            new ScannerExpectation("table2", scansPerTable, extra)));
            results.add(getWithExpectation(alternateAuthsHelper, args[i].args, baseAltAuths, false, new ScannerExpectation("table", scansPerTable, extra)));
            results.add(getWithExpectation(alternateHelper, args[i].args, baseAlt, false, new ScannerExpectation("table2", scansPerTable, extra)));
        }

        return results.toArray(new MethodParamsAndExpectations[0]);
    }

    // test all public, non-static caching methods for cache consistency. Ensure caching is defined and works as expected for all methods. This test will fail
    // if cache keys are wrong leading to unexpected accumulo calls
    @Test
    public void cachingConsistencyTest() throws Exception {
        // @formatter:off
        Map<String,MethodParamsAndExpectations[]> methodToParams = new HashMap<>();
        methodToParams.put("1.getQueryModelNames", getExpectedQueryModelNames());
        methodToParams.put("1.getFacets", getExpectedFacets());
        methodToParams.put("0.getTermCounts", getNoArgAuthMetadataTableNameVariations(null));
        methodToParams.put("0.getTermCountsWithRootAuths", getNoArgMetadataTableNameVariations((s) -> {
            SecurityOperations so = createMock(SecurityOperations.class);
            expect(getAccumuloClient().securityOperations()).andReturn(so);
            expect(getAccumuloClient().whoami()).andReturn("dwv");
            expect(so.getUserAuthorizations("dwv")).andReturn(new Authorizations("steve"));
        }));
        methodToParams.put("0.getAllNormalized", getNoArgAuthMetadataTableNameVariations(null));
        methodToParams.put("0.getEdges", getNoArgAuthMetadataTableNameVariations((s) -> {
            s.addScanIterator(anyObject());
            s.addScanIterator(anyObject());
        }));
        methodToParams.put("1.getIndexOnlyFields", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, false, false));
        methodToParams.put("1.getTermFrequencyFields", getAuthMetadataTableNameVariations(
                new Args[] {new Args(new Object[] {null})}, true));
        methodToParams.put("1.getAllFields", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, false, false));
        methodToParams.put("1.getNonEventFields", getNonEventFieldExpectations(new Args[] {new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}));
        methodToParams.put("2.isReverseIndexed", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {"f1", Collections.emptySet()}),
                new Args(new Object[] {"f1", Collections.singleton("filter")}),
                new Args(new Object[] {"f2", Collections.singleton("filter")})}, true, false));
        methodToParams.put("2.isIndexed", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {"f1", Collections.emptySet()}),
                new Args(new Object[] {"f1", Collections.singleton("filter")}),
                new Args(new Object[] {"f2", Collections.singleton("filter")})}, true, false));
        methodToParams.put("2.isTokenized", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {"f1", Collections.emptySet()}),
                new Args(new Object[] {"f1", Collections.singleton("filter")}),
                new Args(new Object[] {"f2", Collections.singleton("filter")})}, true, false));
        methodToParams.put("0.getAllDatatypes", getNoArgAuthMetadataTableNameVariations(null));
        methodToParams.put("0.getCompositeToFieldMap", getNoArgAuthMetadataTableNameVariations(null));
        methodToParams.put("1.getCompositeToFieldMap", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, true, false));
        methodToParams.put("0.getCompositeTransitionDateMap", getNoArgAuthMetadataTableNameVariations(null));
        methodToParams.put("1.getCompositeTransitionDateMap", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, true, false));
        methodToParams.put("0.getWhindexCreationDateMap", getNoArgAuthMetadataTableNameVariations(null));
        methodToParams.put("1.getWhindexCreationDateMap", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, true, false));
        methodToParams.put("0.getCompositeFieldSeparatorMap", getNoArgAuthMetadataTableNameVariations(null));
        methodToParams.put("1.getCompositeFieldSeparatorMap", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, true, false));
        methodToParams.put("1.getDatatypesForField", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {"f1"}),
                new Args(new Object[] {"f2"}),
                new Args(new Object[] {"f3"})},false, false));
        methodToParams.put("2.getDatatypesForField", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {"f1", Collections.emptySet()}),
                new Args(new Object[] {"f1", Collections.singleton("filter")}),
                new Args(new Object[] {"f2", Collections.singleton("filter2")})}, true, false));
        methodToParams.put("0.getTypeMetadata", getNoArgAuthMetadataTableNameVariations(null));
        methodToParams.put("1.getTypeMetadata", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, true, false));
        methodToParams.put("0.getCompositeMetadata", getNoArgAuthMetadataTableNameVariations(null));
        methodToParams.put("1.getCompositeMetadata", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, true, false));
        methodToParams.put("1.getFieldsToDatatypes", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, true, true));
        // verifying filters would require even more test setup to mock the type metadata that matches the classes being passed
        methodToParams.put("2.getFieldsForDatatype", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {IdentityDataType.class, Collections.emptySet()}),
                new Args(new Object[] {DateType.class, Collections.emptySet()}),
                new Args(new Object[] {GeoType.class, Collections.emptySet()})}, false, false));
        methodToParams.put("1.getFieldsForDatatype", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {IdentityDataType.class}),
                new Args(new Object[] {DateType.class}),
                new Args(new Object[] {GeoType.class})}, false, false));
        methodToParams.put("1.getIndexedFields", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, false, false));
        methodToParams.put("1.getReverseIndexedFields", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, false, false));
        methodToParams.put("1.getExpansionFields", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, false, false));
        methodToParams.put("1.getContentFields", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, false, false));
        methodToParams.put("1.getDatatypes", getAuthMetadataTableNameVariations(new Args[] {
                new Args(new Object[] {null}),
                new Args(new Object[] {Collections.emptySet()}),
                new Args(new Object[] {Collections.singleton("filter")})}, false, false));
        methodToParams.put("0.loadIndexOnlyFields", getNoArgAuthMetadataTableNameVariations(null));
        // 1 scanner + getAllFields + getIndexOnlyFields caching
        methodToParams.put("2.getQueryModel",  getAuthMetadataTableNameVariations(new Args[]{
                new Args(new Object[] {"table", "model"}),
                new Args(new Object[] {"table", "model2"}),
                new Args(new Object[] {"table", "model3"})}, false, false, new ScannerExpectation("table", 1), 2));
        // 1 scanner + getAllFields caching
        methodToParams.put("3.getQueryModel", getAuthMetadataTableNameVariations(new Args[]{
                new Args(new Object[] {"table", "model", Collections.emptySet()}),
                new Args(new Object[] {"table", "model", Collections.singleton("f1")}),
                new Args(new Object[] {"table", "model2", Collections.singleton("f1")}),
                new Args(new Object[] {"table", "model2", Collections.singleton("f2")}),
                new Args(new Object[] {"table", "model3", Collections.singleton("f1")}),
                new Args(new Object[] {"t3", "model3", Collections.singleton("f1")})}, false, false, new ScannerExpectation(null, 1)));
        // 1 scanner + getAllFields caching
        methodToParams.put("4.getQueryModel", getAuthMetadataTableNameVariations(new Args[]{
                new Args(new Object[] {"table", "model", Collections.emptySet(), Collections.emptySet()}),
                new Args(new Object[] {"table", "model", Collections.singleton("f1"), Collections.emptySet()}),
                new Args(new Object[] {"table", "model2", Collections.singleton("f1"), Collections.singleton("filter")}),
                new Args(new Object[] {"table", "model2", Collections.singleton("f2"), Collections.singleton("filter")}),
                new Args(new Object[] {"table", "model3", Collections.singleton("f1"), Collections.singleton("filter")}),
                new Args(new Object[] {"t3", "model3", Collections.singleton("f1"), Collections.singleton("filter")})}, false, false, new ScannerExpectation(null, 1)));
        // loadIndexedFields, but loadIndexedFields is call from within AllFieldsMetadataHelper so is NOT cached
        methodToParams.put("3.getFieldIndexHoles", getScansPerCall(new Args[]{
                // empty set of fields does an extra scan (always) so can't be tested in this test
//                new Args(new Object[] {Collections.emptySet(), Collections.emptySet(), 3}),
                new Args(new Object[] {Collections.singleton("f1"), Collections.emptySet(), 3}),
                new Args(new Object[] {Collections.singleton("f1"), Collections.singleton("filter"), 3}),
                new Args(new Object[] {Collections.singleton("f1"), Collections.singleton("filter2"), 3}),
                new Args(new Object[] {Collections.singleton("f2"), Collections.singleton("filter2"), 3})}, 1));
        // loadIndexedFields, but loadIndexedFields is call from within AllFieldsMetadataHelper so is NOT cached
        methodToParams.put("3.getReversedFieldIndexHoles", getScansPerCall(new Args[]{
                // empty set of fields does an extra scan (always) so can't be tested in this test
//                new Args(new Object[] {Collections.emptySet(), Collections.emptySet(), 3}),
                new Args(new Object[] {Collections.singleton("f1"), Collections.emptySet(), 3}),
                new Args(new Object[] {Collections.singleton("f1"), Collections.singleton("filter"), 3}),
                new Args(new Object[] {Collections.singleton("f1"), Collections.singleton("filter2"), 3}),
                new Args(new Object[] {Collections.singleton("f2"), Collections.singleton("filter2"), 3})}, 1));

        // uncached methods that can safely be skipped
        methodToParams.put("0.toString", new MethodParamsAndExpectations[] {});
        methodToParams.put("1.getDatatype", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getUsersMetadataAuthorizationSubset", new MethodParamsAndExpectations[] {});
        methodToParams.put("2.getUsersMetadataAuthorizationSubset", new MethodParamsAndExpectations[] {});
        methodToParams.put("1.getAllMetadataAuthsPowerSet", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getAllMetadataAuths", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getAuths", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getFullUserAuths", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getAllFieldMetadataHelper", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getMetadata", new MethodParamsAndExpectations[] {});
        methodToParams.put("1.getMetadata", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getEvaluationOnlyFields", new MethodParamsAndExpectations[] {});
        methodToParams.put("1.setEvaluationOnlyFields", new MethodParamsAndExpectations[] {});
        methodToParams.put("1.getDatatypeFromClass", new MethodParamsAndExpectations[] {});
        methodToParams.put("3.getCountsByFieldForDays", new MethodParamsAndExpectations[] {});
        methodToParams.put("4.getCountsByFieldForDays", new MethodParamsAndExpectations[] {});
        methodToParams.put("4.createFieldCountRanges", new MethodParamsAndExpectations[] {});
        methodToParams.put("1.readLongFromValue", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getMetadataTableName", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getTypeCacheSize", new MethodParamsAndExpectations[] {});
        methodToParams.put("1.setTypeCacheSize", new MethodParamsAndExpectations[] {});
        methodToParams.put("0.getTypeCacheExpirationInMinutes", new MethodParamsAndExpectations[] {});
        methodToParams.put("1.setTypeCacheExpirationInMinutes", new MethodParamsAndExpectations[] {});

        // not currently cached, verify scanners so if caching is added things will fail
        methodToParams.put("0.getTypeMetadataMap", getScansPerCall(new Args[] {
                new Args(new Object[] {})}, 2));
        methodToParams.put("0.loadTermFrequencyFields", getScansPerCall(new Args[] {
                new Args(new Object[] {})}, 1));
        methodToParams.put("0.loadAllFields", getScansPerCall(new Args[] {
                new Args(new Object[] {})}, 1));
        methodToParams.put("3.getCardinalityForField", getScansPerCall(new Args[] {
                new Args(new Object[] {"f1", new Date(), new Date()})}, 1));
        methodToParams.put("4.getCardinalityForField", getScansPerCall(new Args[] {
                new Args(new Object[] {"f1", "type1", new Date(), new Date()})}, 1));
        methodToParams.put("2.getCountsByFieldInDay", getScansPerCall(new Args[] {
                new Args(new Object[] {"f1", "20241119"}),
                new Args(new Object[] {"f2", "20241119"}),
                new Args(new Object[] {"f1", "20241118"})}, 1, (s)-> {
            s.addScanIterator(anyObject());
        }));
        methodToParams.put("3.getCountsByFieldInDayWithTypes", getScansPerCall(new Args[] {
                new Args(new Object[] {"f1", "20241119", Collections.emptySet()}),
                new Args(new Object[] {"f2", "20241119", Collections.emptySet()}),
                new Args(new Object[] {"f1", "20241119", Collections.singleton("filter")})}, 1, (s)-> {
                    s.addScanIterator(anyObject());
        }));
        // These use batch scanners...
        methodToParams.put("3.getCountsForFieldsInDateRange", getScansPerCall(new Args[] {
                new Args(new Object[] {Collections.emptySet(), new Date(), new Date()})}, 0, (s)-> {
            BatchScanner bs = createMock(BatchScanner.class);
            expect(getAccumuloClient().createBatchScanner(anyObject(), anyObject())).andReturn(bs);
            bs.close();
        }));
        // can't do this one, doesn't support overloaded params for same param count
        methodToParams.put("4.getCountsForFieldsInDateRange", new MethodParamsAndExpectations[] {});
        methodToParams.put("1.getEarliestOccurrenceOfField", getScansPerCall(new Args[] {
                new Args(new Object[] {"f1"}),
                new Args(new Object[] {"f2"}),
                new Args(new Object[] {"f3"})}, 1));
        methodToParams.put("2.getEarliestOccurrenceOfFieldWithType", getScansPerCall(new Args[] {
                new Args(new Object[] {"f1", "type1"}),
                new Args(new Object[] {"f2", "type1"}),
                new Args(new Object[] {"f1", "type2"}),
                new Args(new Object[] {"f2", "type2"})}, 1, (s)-> {
                    s.addScanIterator(anyObject());
        }));
        // @formatter:on

        // test that all methods are tested
        for (Method method : MetadataHelper.class.getDeclaredMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                // no need to verify statics
                continue;
            }
            if (Modifier.isPrivate(method.getModifiers())) {
                // no need to verify private
                continue;
            }
            if (Modifier.isProtected(method.getModifiers())) {
                // no need to verify protected
                continue;
            }

            // isolate a single test
            // if (!method.getName().equals("getCountsByFieldInDayWithTypes")) {
            // continue;
            // }
            // if (method.getParameterCount() != 2) {
            // continue;
            // }

            String lookupName = method.getParameterCount() + "." + method.getName();

            // verify all non static methods are tested
            assertNotNull("MetadataHelper method '" + method.getName() + "()' with " + method.getParameterCount() + " args not tested",
                            methodToParams.get(lookupName));

            // clear spring caches to prevent cross contamination between calls
            cleanupCache();

            // setup expectations
            for (MethodParamsAndExpectations expectation : methodToParams.get(lookupName)) {
                expectation.expect();
            }

            // replay expectations
            EasyMock.replay(getAccumuloClient());
            replayAll();

            // invoke method on all objects in order
            for (MethodParamsAndExpectations expectation : methodToParams.get(lookupName)) {
                Object result = method.invoke(expectation.get(), expectation.args);
                expectation.setResult(result);

                // test cache value matches if it was supposed to be a cache hit
                if (expectation.cachedOf != null) {
                    if (expectation.exactMatch) {
                        assertTrue(method.getName() + "didn't get exact expected cached value", expectation.cachedOf.getResult() == result);
                    } else {
                        assertEquals(method.getName() + "didn't get equivalent value", expectation.cachedOf.getResult(), result);
                    }
                }
            }

            EasyMock.verify(getAccumuloClient());
            verifyAll();

            // reset mocks to use again
            EasyMock.reset(getAccumuloClient());
            resetAll();
        }
    }

    /**
     * Reset the spring cache's to prevent contamination
     */
    private void cleanupCache() {
        for (String cacheName : cacheManager.getCacheNames()) {
            cacheManager.getCache(cacheName).clear();
        }
    }

    private static class Args {
        public Object[] args;

        public Args(Object[] args) {
            this.args = args;
        }
    }

    private interface ExtraExpectation {
        void extraExpectation(Scanner s) throws Exception;
    }

    private interface Expectation {
        void expect() throws Exception;
    }

    private class ScannerExpectation implements Expectation {

        private final String table;
        private final int count;
        private ExtraExpectation extra;

        private ScannerExpectation(String table, int count) {
            this.table = table;
            this.count = count;
        }

        private ScannerExpectation(String table, int count, ExtraExpectation extra) {
            this.table = table;
            this.count = count;
            this.extra = extra;
        }

        @Override
        public void expect() throws Exception {
            for (int i = 0; i < count; i++) {
                Scanner s = createMock(Scanner.class);
                List<Map.Entry<Key,Value>> entries = new ArrayList<>();
                expectScanner(table, s, entries);

                if (extra != null) {
                    extra.extraExpectation(s);
                }
            }
        }
    }

    /**
     * Used to set expectations and cache settings of a method call. Used by cachingConsistencyTest()
     */
    private static class MethodParamsAndExpectations implements Expectation {
        public MetadataHelper metadataHelper;
        public Object[] args;
        public MethodParamsAndExpectations cachedOf;
        public Object result = null;
        public boolean exactMatch;
        private Expectation expectation;

        private MethodParamsAndExpectations(MetadataHelper helper, Object[] args) {
            this(helper, args, null);
        }

        private MethodParamsAndExpectations(MetadataHelper helper, Object[] args, MethodParamsAndExpectations cachedOf) {
            this(helper, args, cachedOf, true);
        }

        private MethodParamsAndExpectations(MetadataHelper helper, Object[] args, MethodParamsAndExpectations cachedOf, boolean exactMatch) {
            this(helper, args, cachedOf, exactMatch, null);
        }

        private MethodParamsAndExpectations(MetadataHelper helper, Object[] args, MethodParamsAndExpectations cachedOf, boolean exactMatch,
                        Expectation expectation) {
            this.metadataHelper = helper;
            this.args = args;
            this.cachedOf = cachedOf;
            this.exactMatch = exactMatch;
            this.expectation = expectation;
        }

        public void expect() throws Exception {
            if (expectation != null) {
                expectation.expect();
            }
        }

        public Object get() {
            return metadataHelper;
        }

        public void setResult(Object result) {
            this.result = result;
        }

        public Object getResult() {
            return this.result;
        }
    }

    /**
     * expect/mock the scanner creation and return of results from an iterator
     *
     * @param table
     *            if not null expects a specific table, otherwise any table
     * @param mockScanner
     * @param entries
     * @throws TableNotFoundException
     */
    private void expectScanner(String table, Scanner mockScanner, List<Map.Entry<Key,Value>> entries) throws TableNotFoundException {
        if (table != null) {
            expect(getAccumuloClient().createScanner(eq(table), anyObject())).andReturn(mockScanner);
        } else {
            expect(getAccumuloClient().createScanner(isA(String.class), anyObject())).andReturn(mockScanner);
        }
        mockScanner.setRange(anyObject());
        mockScanner.fetchColumnFamily(isA(Text.class));
        expectLastCall().anyTimes();
        expect(mockScanner.iterator()).andReturn(entries.iterator());
        mockScanner.close();
    }

    // just to prove out how to wire up a spring cache
    public static class TestCache {
        int i = 0;

        @Cacheable(value = "someCache")
        public String getData(String key) {
            return key + i++;
        }
    }

    @Autowired
    private TestCache c;

    /**
     * If this test fails the spring cache wiring is broken
     */
    @Test
    public void testCache() {
        assertEquals(c.getData("a"), c.getData("a"));
    }
}
