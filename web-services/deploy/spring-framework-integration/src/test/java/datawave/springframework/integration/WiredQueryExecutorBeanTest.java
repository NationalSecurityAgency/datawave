package datawave.springframework.integration;

import java.io.InputStream;
import java.util.Properties;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.security.JSSESecurityDomain;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;

import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.composite.CompositeQueryLogic;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.query.discovery.DiscoveryLogic;
import datawave.query.metrics.QueryMetricQueryLogic;
import datawave.query.planner.BooleanChunkingQueryPlanner;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.FacetedQueryPlanner;
import datawave.query.tables.CountingShardQueryLogic;
import datawave.query.tables.IndexQueryLogic;
import datawave.query.tables.ParentQueryLogic;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.TLDQueryLogic;
import datawave.query.tables.facets.FacetedQueryLogic;
import datawave.query.tables.shard.FieldIndexCountQueryLogic;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;
import datawave.query.util.DateIndexHelperFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.security.system.ServerPrincipal;
import datawave.webservice.common.json.DefaultMapperDecorator;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.results.cached.CachedResultsConfiguration;

/**
 * this test ensures that our various spring contexts can be deployed successfully to Wildfly
 */
@RunWith(Arquillian.class)
public class WiredQueryExecutorBeanTest {

    private Logger log = Logger.getLogger(WiredQueryExecutorBeanTest.class);

    @Inject
    ApplicationContext ctx;

    @Produces
    @ServerPrincipal
    public DatawavePrincipal produceServerPrincipal() {
        return new DatawavePrincipal();
    }

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        System.setProperty("cdi.bean.context", "springFrameworkBeanRefContext.xml");
        // Set system properties that are normally set by the Wildfly container, and needed for test deployment
        try (InputStream is = WiredQueryExecutorBeanTest.class.getResourceAsStream("/test-system-properties.properties")) {
            Properties systemProperties = System.getProperties();
            systemProperties.load(is);
            System.setProperties(systemProperties);
        }

        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.data.type", "datawave.query.language.parser.jexl",
                                        "datawave.query.language.functions.jexl", "datawave.webservice.query.configuration", "datawave.configuration")
                        .addClasses(DefaultResponseObjectFactory.class, QueryExpirationProperties.class, FacetedQueryPlanner.class, FacetedQueryLogic.class,
                                        DefaultQueryPlanner.class, BooleanChunkingQueryPlanner.class, ShardQueryLogic.class, CountingShardQueryLogic.class,
                                        EventQueryDataDecoratorTransformer.class, FieldIndexCountQueryLogic.class, CompositeQueryLogic.class,
                                        QueryMetricQueryLogic.class, TLDQueryLogic.class, ParentQueryLogic.class, DiscoveryLogic.class, IndexQueryLogic.class,
                                        QueryLogicFactoryImpl.class, CachedResultsConfiguration.class, DateIndexHelperFactory.class,
                                        DefaultMapperDecorator.class)
                        .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Test
    public void testCreatingContext() throws Exception {
        DefaultQueryPlanner defaultQueryPlanner = ctx.getBean("DefaultQueryPlanner", DefaultQueryPlanner.class);
        Assert.assertNotNull(defaultQueryPlanner);
    }

    // This test ensures that we can
    // 1) generate a query logic via xml configuration
    // 2) inject an xml generated reference bean into the generated query logic
    @Test
    public void testCreatingPrototypeBeans() {
        String[] names = ctx.getBeanNamesForType(QueryLogic.class);
        for (String name : names) {
            QueryLogic<?> ql = ctx.getBean(name, QueryLogic.class);
            if (ql.getResponseObjectFactory() == null) {
                log.error("response object factory is null for " + name + " and " + ql + " named " + ql.getLogicName() + " and " + ql.getClass());
            }
            Assert.assertNotNull(ql.getResponseObjectFactory());
            log.debug("got " + ql);
        }
    }

    private static JSSESecurityDomain mockJsseSecurityDomain = EasyMock.createMock(JSSESecurityDomain.class);
    private static DatawavePrincipal mockDatawavePrincipal = EasyMock.createMock(DatawavePrincipal.class);

    private static RemoteEdgeDictionary mockRemoteEdgeDictionary = EasyMock.createMock(RemoteEdgeDictionary.class);

    public static class Producer {
        @Produces
        public static JSSESecurityDomain produceSecurityDomain() {
            return mockJsseSecurityDomain;
        }

        @Produces
        @CallerPrincipal
        public static DatawavePrincipal produceDatawavePrincipal() {
            return mockDatawavePrincipal;
        }

        @Produces
        public static RemoteEdgeDictionary produceRemoteEdgeDictionary() {
            return mockRemoteEdgeDictionary;
        }
    }
}
