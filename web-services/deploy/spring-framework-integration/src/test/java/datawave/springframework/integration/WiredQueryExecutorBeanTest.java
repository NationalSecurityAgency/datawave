package datawave.springframework.integration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Properties;
import java.util.Set;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.BoundConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesBindingPostProcessor;
import org.springframework.context.ApplicationContext;

import datawave.configuration.spring.SpringCDIExtension;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.composite.CompositeQueryLogic;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.microservice.metadata.config.MetadataHelperConfiguration;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.query.logic.config.BaseEventQueryConfiguration;
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
import datawave.query.tables.keyword.KeywordQueryState;
import datawave.query.tables.shard.FieldIndexCountQueryLogic;
import datawave.query.tables.ssdeep.SSDeepSimilarityQueryState;
import datawave.query.transformer.EventQueryDataDecoratorTransformer;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.QueryStopwatch;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.cert.SSLStores;
import datawave.security.system.CallerPrincipal;
import datawave.security.system.SSLStoresProducer;
import datawave.security.system.ServerPrincipal;
import datawave.util.keyword.DefaultTagCloudUtils;
import datawave.webservice.common.json.DefaultMapperDecorator;
import datawave.webservice.datadictionary.DataDictionaryResponseTypeProducer;
import datawave.webservice.edgedictionary.EdgeDictionaryResponseTypeProducer;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.results.cached.CachedResultsConfiguration;

/**
 * this test ensures that our various spring contexts can be deployed successfully to Wildfly
 */
@ExtendWith(ArquillianExtension.class)
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
        // tells SpringConfiurationProperties to use the cdi.bean.context instead of configuring the context from properties.
        System.setProperty("datawave.configuration.spring.configure-from-properties", "false");
        System.setProperty("cdi.bean.context", "springFrameworkBeanRefContext.xml");

        // Set system properties that are normally set by the Wildfly container, and needed for test deployment
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        try (InputStream is = WiredQueryExecutorBeanTest.class.getResourceAsStream("/test-system-properties.properties")) {
            Properties systemProperties = System.getProperties();
            systemProperties.load(is);
            System.setProperties(systemProperties);
        }
        String tempDirPrefix = WiredQueryExecutorBeanTest.class.getSimpleName();
        // this property is used by Spring to determine where to write debug information about the context
        System.setProperty("spring.context.debug.dir", createTempDirectory(tempDirPrefix));

        System.setProperty("spring.cloud.config.enabled", "false");
        // this tells Spring to load the yml files from the target directory instead of from the classpath
        Path springYmlFilesDir = Path.of(System.getProperty("user.dir"), "target").resolve("springymlfiles");
        assertTrue(Files.isDirectory(springYmlFilesDir), "springYmlFilesDir does not exist: " + springYmlFilesDir);
        System.setProperty("spring.config.location", springYmlFilesDir.toUri().toString());

        // this Spring Boot property allows you to change the root file name of your YAML files from the default application to a custom name
        // When listing multiple names, Spring Boot processes them from left to right, with the leftmost files acting as the base and
        // subsequent files overriding previous values.
        System.setProperty("spring.config.name", "application,wildfly");
        // apply the query profile to all root filenames when looking for yml files
        System.setProperty("spring.profiles.active", "query");

        // @formatter:off
        return ShrinkWrap.create(JavaArchive.class)
                .addPackages(true,
                        "com.codahale.metrics",
                        "datawave.configuration",
                        "datawave.data.type",
                        "datawave.microservice.query.logic.config",
                        "datawave.query.language.functions.jexl",
                        "datawave.query.language.parser.jexl",
                        "datawave.webservice.common",
                        "datawave.webservice.query.configuration",
                        "datawave.webservice.spring.configuration",
                        "io.astefanutti.metrics.cdi",
                        "org.apache.deltaspike"
                )
                .deletePackage("datawave.webservice.common.connection")
                .addClasses(
                        ApplicationContext.class,
                        BaseEventQueryConfiguration.class,
                        BooleanChunkingQueryPlanner.class,
                        BoundConfigurationProperties.class,
                        CachedResultsConfiguration.class,
                        CompositeQueryLogic.class,
                        ConfigurationPropertiesBindingPostProcessor.class,
                        CountingShardQueryLogic.class,
                        DataDictionaryResponseTypeProducer.class,
                        DateIndexHelperFactory.class,
                        DefaultMapperDecorator.class,
                        DefaultQueryPlanner.class,
                        DefaultResponseObjectFactory.class,
                        DefaultTagCloudUtils.class,
                        DiscoveryLogic.class,
                        EdgeDictionaryResponseTypeProducer.class,
                        EventQueryDataDecoratorTransformer.class,
                        FacetedQueryLogic.class,
                        FacetedQueryPlanner.class,
                        FieldIndexCountQueryLogic.class,
                        IndexQueryLogic.class,
                        KeywordQueryState.class,
                        MetadataHelperConfiguration.class,
                        ParentQueryLogic.class,
                        ContextComparisonTest.Producer.class,
                        PropertyPlaceholderAutoConfiguration.class,
                        PropertyPlaceholderConfigurer.class,
                        QueryExpirationProperties.class,
                        QueryLogicFactoryImpl.class,
                        QueryMetricQueryLogic.class,
                        QueryStopwatch.class,
                        SSDeepSimilarityQueryState.class,
                        SSLStores.class,
                        SSLStoresProducer.class,
                        ShardQueryLogic.class,
                        SpringCDIExtension.class,
                        TLDQueryLogic.class
                )
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
        // @formatter:on
    }

    @Test
    public void testCreatingContext() throws Exception {
        DefaultQueryPlanner defaultQueryPlanner = ctx.getBean("DefaultQueryPlanner", DefaultQueryPlanner.class);
        assertNotNull(defaultQueryPlanner);
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
            assertNotNull(ql.getResponseObjectFactory());
            log.debug("got " + ql);
        }
    }

    private static SSLStores mockSSLStores = EasyMock.createMock(SSLStores.class);
    private static DatawavePrincipal mockDatawavePrincipal = EasyMock.createMock(DatawavePrincipal.class);

    private static RemoteEdgeDictionary mockRemoteEdgeDictionary = EasyMock.createMock(RemoteEdgeDictionary.class);

    public static class Producer {
        @Produces
        public static SSLStores produceSSLStores() {
            return mockSSLStores;
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

    private static String createTempDirectory(String prefix) throws IOException {
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxr-xr-x");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        return String.valueOf(Files.createTempDirectory(prefix + "_", attr));
    }
}
