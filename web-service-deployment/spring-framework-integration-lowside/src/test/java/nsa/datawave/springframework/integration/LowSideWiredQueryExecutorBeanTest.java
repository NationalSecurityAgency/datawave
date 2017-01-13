package nsa.datawave.springframework.integration;

import java.io.InputStream;
import java.util.Properties;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import nsa.datawave.query.metrics.QueryMetricQueryLogic;
import nsa.datawave.query.rewrite.discovery.DiscoveryLogic;
import nsa.datawave.query.rewrite.planner.BooleanChunkingQueryPlanner;
import nsa.datawave.query.rewrite.planner.DefaultQueryPlanner;
import nsa.datawave.query.rewrite.planner.FacetedQueryPlanner;
import nsa.datawave.query.rewrite.tables.IndexQueryLogic;
import nsa.datawave.query.rewrite.tables.ParentQueryLogic;
import nsa.datawave.query.rewrite.tables.RefactoredCountingShardQueryLogic;
import nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import nsa.datawave.query.rewrite.tables.RefactoredTLDQueryLogic;
import nsa.datawave.query.rewrite.tables.facets.FacetedQueryLogic;
import nsa.datawave.query.tables.shard.FieldIndexCountQueryLogic;
import nsa.datawave.query.tables.shard.IndexStatsQueryLogic;
import nsa.datawave.query.transformer.EventQueryDataDecoratorTransformer;
import nsa.datawave.query.util.DateIndexHelperFactory;
import nsa.datawave.security.authorization.BasePrincipalFactoryConfiguration;
import nsa.datawave.security.authorization.DatawavePrincipalLookup;
import nsa.datawave.webservice.edgedictionary.DatawaveEdgeDictionary;
import nsa.datawave.webservice.edgedictionary.DefaultDatawaveEdgeDictionaryImpl;
import nsa.datawave.webservice.operations.configuration.LookupBeanConfiguration;
import nsa.datawave.webservice.query.cache.QueryExpirationConfiguration;
import nsa.datawave.webservice.query.logic.DatawaveRoleManager;
import nsa.datawave.webservice.query.logic.EasyRoleManager;
import nsa.datawave.webservice.query.logic.QueryLogic;
import nsa.datawave.webservice.query.logic.QueryLogicFactoryImpl;
import nsa.datawave.webservice.query.logic.composite.CompositeQueryLogic;
import nsa.datawave.webservice.query.metric.NoOpQueryMetricHandler;
import nsa.datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import nsa.datawave.webservice.results.cached.CachedResultsConfiguration;

import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;

/**
 * this tests the validity of the QueryLogicFactoryLowSide.xml file
 */
@RunWith(Arquillian.class)
public class LowSideWiredQueryExecutorBeanTest {
    
    private Logger log = Logger.getLogger(LowSideWiredQueryExecutorBeanTest.class);
    
    @Inject
    ApplicationContext ctx;
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        System.setProperty("cdi.bean.context", "springFrameworkBeanRefContext.xml");
        // Set system properties that are normally set by the Wildfly container, and needed for test deployment
        try (InputStream is = LowSideWiredQueryExecutorBeanTest.class.getResourceAsStream("/test-system-properties.properties")) {
            Properties systemProperties = System.getProperties();
            systemProperties.load(is);
            System.setProperties(systemProperties);
        }
        
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "nsa.datawave.data.type",
                                        "nsa.datawave.query.language.parser.jexl", "nsa.datawave.query.language.functions.jexl",
                                        "nsa.datawave.webservice.query.configuration")
                        .addClasses(DefaultResponseObjectFactory.class, BasePrincipalFactoryConfiguration.class, QueryExpirationConfiguration.class,
                                        FacetedQueryPlanner.class, FacetedQueryLogic.class, DefaultQueryPlanner.class, BooleanChunkingQueryPlanner.class,
                                        RefactoredShardQueryLogic.class, RefactoredCountingShardQueryLogic.class, EventQueryDataDecoratorTransformer.class,
                                        FieldIndexCountQueryLogic.class, CompositeQueryLogic.class, IndexStatsQueryLogic.class, QueryMetricQueryLogic.class,
                                        RefactoredTLDQueryLogic.class, ParentQueryLogic.class, DiscoveryLogic.class, IndexQueryLogic.class,
                                        QueryLogicFactoryImpl.class, NoOpQueryMetricHandler.class, DatawaveRoleManager.class, EasyRoleManager.class,
                                        CachedResultsConfiguration.class, LookupBeanConfiguration.class, DateIndexHelperFactory.class,
                                        DefaultDatawaveEdgeDictionaryImpl.class).addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }
    
    @Test
    public void testCreatingContext() throws Exception {
        DefaultQueryPlanner defaultQueryPlanner = ctx.getBean("DefaultQueryPlanner", DefaultQueryPlanner.class);
        Assert.assertNotNull(defaultQueryPlanner);
    }
    
    @Test
    public void testCreatingPrototypeBeans() {
        String[] names = ctx.getBeanNamesForType(QueryLogic.class);
        for (String name : names) {
            QueryLogic<?> ql = ctx.getBean(name, QueryLogic.class);
            if (ql.getRoleManager() == null) {
                log.error("role manager is null for " + name + " and " + ql + " named " + ql.getLogicName() + " and " + ql.getClass());
            }
            Assert.assertNotNull(ql.getRoleManager());
            log.debug("got " + ql);
        }
    }
    
    private static DatawavePrincipalLookup datawavePrincipalLookup = EasyMock.createMock(DatawavePrincipalLookup.class);
    
    public static class Producer {
        @Produces
        public static DatawavePrincipalLookup produceDatawavePrincipalLookup() {
            return datawavePrincipalLookup;
        }
    }
}
