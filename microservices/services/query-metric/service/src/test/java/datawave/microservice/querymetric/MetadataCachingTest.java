package datawave.microservice.querymetric;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.util.AllFieldMetadataHelper;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.TypeMetadataHelper;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryMetricTest", "hazelcast-writethrough"})
public class MetadataCachingTest extends QueryMetricTestBase {

    @Autowired
    protected MetadataHelperFactory metadataHelperFactory;

    @Autowired
    @Qualifier("metadataHelperCacheManager")
    protected CacheManager metadataHelperCacheManager;

    @Autowired
    @Qualifier("dateIndexHelperCacheManager")
    protected CacheManager dateIndexHelperCacheManager;

    @Autowired
    protected DateIndexHelperFactory dateIndexHelperFactory;

    @BeforeEach
    public void setup() {
        super.setup();
    }

    @AfterEach
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void VerifyMetadataMethodsCacheable() throws Exception {
        Set<Authorizations> authorizations = auths.stream().map(Authorizations::new).collect(Collectors.toSet());
        MetadataHelper metadataHelper = metadataHelperFactory.createMetadataHelper(accumuloClient, queryMetricHandlerProperties.getMetadataTableName(),
                        authorizations);
        metadataHelper.getAllNormalized();
        metadataHelper.getEdges();
        metadataHelper.getFacets(queryMetricHandlerProperties.getShardTableName());
        metadataHelper.getAllDatatypes();
        metadataHelper.isIndexed("QUERY_ID", Collections.singleton("querymetrics"));
        metadataHelper.getIndexOnlyFields(Collections.singleton("querymetrics"));
        metadataHelper.getIndexedFields(Collections.singleton("querymetrics"));
        metadataHelper.getReverseIndexedFields(Collections.singleton("querymetrics"));
        metadataHelper.getTermFrequencyFields(Collections.singleton("querymetrics"));
        metadataHelper.getDatatypes(Collections.singleton("querymetrics"));
        metadataHelper.getFieldsForDatatype(LcNoDiacriticsType.class);
        metadataHelper.getFieldsToDatatypes(Collections.singleton("querymetrics"));
        metadataHelper.getContentFields(Collections.singleton("querymetrics"));
        metadataHelper.getExpansionFields(Collections.singleton("querymetrics"));
        metadataHelper.getQueryModel(queryMetricHandlerProperties.getMetadataTableName(), "querymetrics");
        metadataHelper.getQueryModelNames(queryMetricHandlerProperties.getMetadataTableName());
        metadataHelper.getTermCounts();
        metadataHelper.getTermCountsWithRootAuths();
        metadataHelper.getCompositeMetadata();
        metadataHelper.getCompositeFieldSeparatorMap();
        metadataHelper.getCompositeToFieldMap();
        metadataHelper.getCompositeTransitionDateMap();
        metadataHelper.getWhindexCreationDateMap();

        DateIndexHelper dateIndexHelper = dateIndexHelperFactory.createDateIndexHelper();
        Date today = new Date();
        Date yesterday = new Date(today.getTime() - 24 * 60 * 60 * 1000);
        accumuloClient.tableOperations().create("QueryMetrics_d");
        dateIndexHelper.initialize(accumuloClient, "QueryMetrics_d", authorizations, 4, 0.99f);
        dateIndexHelper.getTypeDescription("BEGIN", yesterday, today, Collections.singleton("querymetrics"));
        dateIndexHelper.getShardsAndDaysHint("QUERY_ID", yesterday, today, yesterday, today, Collections.singleton("querymetrics"));

        TreeSet<String> cacheNames = new TreeSet<>();
        cacheNames.addAll(metadataHelperCacheManager.getCacheNames());
        cacheNames.addAll(dateIndexHelperCacheManager.getCacheNames());

        Set<String> expectedCacheNames = new TreeSet<>();
        expectedCacheNames.addAll(getCacheNamesForCacheableMethods(MetadataHelper.class));
        expectedCacheNames.addAll(getCacheNamesForCacheableMethods(CompositeMetadataHelper.class));
        expectedCacheNames.addAll(getCacheNamesForCacheableMethods(TypeMetadataHelper.class));
        expectedCacheNames.addAll(getCacheNamesForCacheableMethods(AllFieldMetadataHelper.class));
        expectedCacheNames.addAll(getCacheNamesForCacheableMethods(DateIndexHelper.class));

        Assert.assertEquals(expectedCacheNames, cacheNames);
    }

    private List<String> getCacheNamesForCacheableMethods(Class clazz) {
        List<String> cacheNames = new ArrayList<>();
        Arrays.stream(clazz.getMethods()).forEach(method -> {
            for (Annotation a : method.getDeclaredAnnotations()) {
                if (a.annotationType().equals(Cacheable.class)) {
                    cacheNames.addAll(Arrays.asList(((Cacheable) a).value()));
                }
            }
        });
        return cacheNames;
    }
}
