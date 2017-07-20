package datawave.springframework.integration;

import java.io.InputStream;
import java.util.Properties;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import datawave.query.metrics.QueryMetricQueryLogic;
import datawave.query.rewrite.discovery.DiscoveryLogic;
import datawave.query.rewrite.planner.BooleanChunkingQueryPlanner;
import datawave.query.rewrite.planner.DefaultQueryPlanner;
import datawave.query.rewrite.planner.FacetedQueryPlanner;
import datawave.query.rewrite.tables.IndexQueryLogic;
import datawave.query.rewrite.tables.ParentQueryLogic;
import datawave.query.rewrite.tables.RefactoredCountingShardQueryLogic;
import datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import datawave.query.rewrite.tables.RefactoredTLDQueryLogic;
import datawave.query.rewrite.tables.facets.FacetedQueryLogic;
import datawave.query.tables.shard.FieldIndexCountQueryLogic;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;
import datawave.query.util.DateIndexHelperFactory;
import datawave.security.authorization.BasePrincipalFactoryConfiguration;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.edgedictionary.DefaultDatawaveEdgeDictionaryImpl;
import datawave.webservice.operations.configuration.LookupBeanConfiguration;
import datawave.webservice.query.cache.QueryExpirationConfiguration;
import datawave.webservice.query.logic.DatawaveRoleManager;
import datawave.webservice.query.logic.EasyRoleManager;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.query.logic.composite.CompositeQueryLogic;
import datawave.webservice.query.metric.NoOpQueryMetricHandler;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.results.cached.CachedResultsConfiguration;

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
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.data.type", "datawave.query.language.parser.jexl",
                                        "datawave.query.language.functions.jexl", "datawave.webservice.query.configuration")
                        .addClasses(DefaultResponseObjectFactory.class, BasePrincipalFactoryConfiguration.class, QueryExpirationConfiguration.class,
                                        FacetedQueryPlanner.class, FacetedQueryLogic.class, DefaultQueryPlanner.class, BooleanChunkingQueryPlanner.class,
                                        RefactoredShardQueryLogic.class, RefactoredCountingShardQueryLogic.class, EventQueryDataDecoratorTransformer.class,
                                        FieldIndexCountQueryLogic.class, CompositeQueryLogic.class, QueryMetricQueryLogic.class, RefactoredTLDQueryLogic.class,
                                        ParentQueryLogic.class, DiscoveryLogic.class, IndexQueryLogic.class, QueryLogicFactoryImpl.class,
                                        NoOpQueryMetricHandler.class, DatawaveRoleManager.class, EasyRoleManager.class, CachedResultsConfiguration.class,
                                        LookupBeanConfiguration.class, DateIndexHelperFactory.class, DefaultDatawaveEdgeDictionaryImpl.class)
                        .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
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
    
    private static DatawavePrincipal mockDatawavePrincipal = EasyMock.createMock(DatawavePrincipal.class);
    
    public static class Producer {
        @Produces
        @CallerPrincipal
        public static DatawavePrincipal produceDatawavePrincipal() {
            return mockDatawavePrincipal;
        }
    }
}
