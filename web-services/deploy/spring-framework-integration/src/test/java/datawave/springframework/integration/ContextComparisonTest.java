package datawave.springframework.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.CDI;
import javax.jms.JMSContext;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.arquillian.container.test.api.Deployer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.BoundConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesBindingPostProcessor;
import org.springframework.context.ApplicationContext;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringCDIExtension;
import datawave.configuration.spring.SpringContextProperties;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
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
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.datadictionary.DataDictionaryResponseTypeProducer;
import datawave.webservice.edgedictionary.EdgeDictionaryResponseTypeProducer;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.results.cached.CachedResultsConfiguration;

/**
 * Arquillian variant of the context comparison test.
 *
 * <p>
 * This class demonstrates how to capture bean snapshots from two named deployments and compare them after the container has started. The two deployments are
 * intentionally built from the same archive helper so the comparison focuses on the test pattern; if you want to compare two different archives, update the
 * deployment helpers to include different resources or classes.
 * </p>
 */
@ExtendWith(ArquillianExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class ContextComparisonTest {

    private static final Logger log = Logger.getLogger(ContextComparisonTest.class);

    private static final String DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE = "DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE";
    private static final String DEPLOYMENT_SPRING_BOOT_PROPERTIES = "DEPLOYMENT_SPRING_BOOT_PROPERTIES";

    private static final Map<String,Map<String,Object>> SNAPSHOTS = new ConcurrentHashMap<>();
    private static final AtomicBoolean DEPLOYED = new AtomicBoolean(false);
    private static Deployer deploymentDeployer;
    private static Path springXmlFilesDir;
    private static Path testClassesDir;

    @ArquillianResource
    private Deployer deployer;

    public static class Producer {
        private static final ManagedExecutorService mockManagedExecutorService = EasyMock.createMock(ManagedExecutorService.class);
        private static DatawavePrincipal mockDatawavePrincipal = EasyMock.createMock(DatawavePrincipal.class);
        private static JMSContext mockJMSContext = EasyMock.createMock(JMSContext.class);

        @Produces
        public static JMSContext produceJMSContext() {
            return mockJMSContext;
        }

        @Produces
        @CallerPrincipal
        @RequestScoped
        public DatawavePrincipal produceCallerPrincipal() {
            return mockDatawavePrincipal;
        }

        @Produces
        @ServerPrincipal
        public static DatawavePrincipal produceServerPrincipal() {
            return mockDatawavePrincipal;
        }

        @Produces
        public static ManagedExecutorService produceManagedExecutorService() {
            return mockManagedExecutorService;
        }

        @Produces
        public static AccumuloConnectionFactory produceAccumuloConnectionFactory() {
            return new MockAccumuloConnectionFactory("root", "PVT,PUBLIC");
        }
    }

    @Deployment(name = DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE, order = 1, managed = false)
    public static JavaArchive createDeploymentOne() {
        return createDeploymentArchive();
    }

    @Deployment(name = DEPLOYMENT_SPRING_BOOT_PROPERTIES, order = 2, managed = false)
    public static JavaArchive createDeploymentTwo() {
        return createDeploymentArchive();
    }

    @BeforeAll
    public static void beforeClass() throws IOException {
        configureDirectories();
        configureSystemProperties();
        fixClasspathReferencesInSpringConfigFiles("dev");
    }

    @AfterAll
    public static void undeployArchives() {
        if (DEPLOYED.compareAndSet(true, false) && deploymentDeployer != null) {
            try {
                deploymentDeployer.undeploy(DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE);
            } catch (RuntimeException e) {
                log.warn(String.format("Ignoring %s undeploy failure during teardown: %s", DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE, e.getMessage()));
            }
            try {
                deploymentDeployer.undeploy(DEPLOYMENT_SPRING_BOOT_PROPERTIES);
            } catch (RuntimeException e) {
                log.warn(String.format("Ignoring %s undeploy failure during teardown: %s", DEPLOYMENT_SPRING_BOOT_PROPERTIES, e.getMessage()));
            }
        }
    }

    @Test
    @OperateOnDeployment(DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE)
    public void test01CaptureDeploymentOneSnapshot() throws IOException {
        System.setProperty("spring.cloud.config.enabled", "false");
        System.setProperty("cdi.bean.context", writeBeanRefContextForCompileTimeProperties("CompileTimeProperties", "dev"));
        deployer.deploy(DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE);
        SNAPSHOTS.put(DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE, snapshotContext(getApplicationContext()));
        assertTrue(!SNAPSHOTS.get(DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE).isEmpty(),
                        String.format("%s snapshot should not be empty", DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE));
    }

    @Test
    @OperateOnDeployment(DEPLOYMENT_SPRING_BOOT_PROPERTIES)
    public void test02CaptureDeploymentTwoSnapshot() throws IOException {
        System.setProperty("spring.cloud.config.enabled", "false");
        System.setProperty("spring.profiles.active", "query,federation");
        System.setProperty("cdi.bean.context", writeBeanRefContextForSpringBootProperties("SpringBootProperties", "dev"));
        deployer.deploy(DEPLOYMENT_SPRING_BOOT_PROPERTIES);
        SNAPSHOTS.put(DEPLOYMENT_SPRING_BOOT_PROPERTIES, snapshotContext(getApplicationContext()));
        assertTrue(!SNAPSHOTS.get(DEPLOYMENT_SPRING_BOOT_PROPERTIES).isEmpty(),
                        String.format("%s snapshot should not be empty", DEPLOYMENT_SPRING_BOOT_PROPERTIES));
    }

    @Test
    public void test03CompareDeployments() {
        assertTrue(SNAPSHOTS.containsKey(DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE),
                        String.format("%s snapshot missing", DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE));
        assertTrue(SNAPSHOTS.containsKey(DEPLOYMENT_SPRING_BOOT_PROPERTIES), String.format("%s snapshot missing", DEPLOYMENT_SPRING_BOOT_PROPERTIES));

        Map<String,Object> deploymentOne = SNAPSHOTS.get(DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE);
        Map<String,Object> deploymentTwo = SNAPSHOTS.get(DEPLOYMENT_SPRING_BOOT_PROPERTIES);

        Set<String> onlyInOne = new TreeSet<>(deploymentOne.keySet());
        onlyInOne.removeAll(deploymentTwo.keySet());
        Set<String> onlyInTwo = new TreeSet<>(deploymentTwo.keySet());
        onlyInTwo.removeAll(deploymentOne.keySet());

        assertTrue(onlyInOne.isEmpty() && onlyInTwo.isEmpty(), String.format("bean name mismatch: only in %s=%s, only in %s=%s",
                        DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE, onlyInOne, DEPLOYMENT_SPRING_BOOT_PROPERTIES, onlyInTwo));

        Collection<Class<?>> ignoredClasses = comparisonIgnoredClasses();
        List<SpringContextComparator.ComparisonDifference> diffs = new ArrayList<>();
        for (String beanName : new TreeSet<>(deploymentOne.keySet())) {
            Object beanOne = deploymentOne.get(beanName);
            Object beanTwo = deploymentTwo.get(beanName);
            log.info("Comparing bean " + beanName + " of type " + beanOne.getClass().getCanonicalName() + " to type " + beanTwo.getClass().getCanonicalName());
            diffs.addAll(SpringContextComparator.compareObjectsRecursive(beanName, beanOne, beanTwo, ignoredClasses));
        }

        if (!diffs.isEmpty()) {
            log.error("Bean value differences found between deployments:");
            diffs.forEach(d -> log.error("  " + d));
            fail("Deployments have different bean values: " + diffs);
        }
    }

    private static void configureDirectories() throws IOException {
        // base directory in target for springXmlFiles
        springXmlFilesDir = Path.of(System.getProperty("user.dir"), "target").resolve("springxmlfiles");
        assertTrue(Files.isDirectory(springXmlFilesDir), "springXmlFilesDir does not exist: " + springXmlFilesDir);
        testClassesDir = Path.of(System.getProperty("user.dir"), "target").resolve("test-classes");
        assertTrue(Files.isDirectory(testClassesDir), "testClassesDir does not exist: " + testClassesDir);
    }

    private static void configureSystemProperties() throws IOException {
        System.setProperty("datawave.configuration.spring.configure-from-properties", "false");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");

        Path repoRoot = Path.of(System.getProperty("user.dir")).getParent().getParent().getParent();
        System.setProperty("pki.dir", repoRoot.resolve("docker/pki").toUri().toString());

        String tempDirPrefix = ContextComparisonTest.class.getSimpleName();
        System.setProperty("spring.context.debug.dir", createTempDirectory(tempDirPrefix));

        Path springYmlFilesDir = Path.of(System.getProperty("user.dir"), "target").resolve("springymlfiles");
        assertTrue(Files.isDirectory(springYmlFilesDir), "springYmlFilesDir does not exist: " + springYmlFilesDir);
        System.setProperty("spring.config.location", springYmlFilesDir.toUri().toString());
        System.setProperty("spring.config.name", "application,wildfly");

    }

    private static JavaArchive createDeploymentArchive() {
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
                                        Producer.class,
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

    private static Map<String,Object> snapshotContext(ApplicationContext context) {
        Map<String,Object> snapshot = new TreeMap<>();
        Arrays.stream(BeanFactoryUtils.beanNamesIncludingAncestors(context)).forEach(name -> {
            Class<?> type = context.getType(name);
            if (type == null || type.getCanonicalName() == null) {
                return;
            }
            if (!shouldInspectBean(name, type)) {
                return;
            }
            try {
                snapshot.put(name, context.getBean(name));
            } catch (Exception e) {
                log.warn("Unable to obtain bean " + name + " of type " + type.getCanonicalName(), e);
            }
        });
        return snapshot;
    }

    private static ApplicationContext getApplicationContext() {
        return CDI.current().select(ApplicationContext.class).get();
    }

    private static boolean shouldInspectBean(String beanName, Class<?> beanType) {
        String typeName = beanType.getCanonicalName();
        if (typeName == null) {
            return false;
        }

        if (!typeName.startsWith("datawave")) {
            return false;
        }

        if (typeName.contains("MetadataHelper") || typeName.contains("TypeMetadata")) {
            return false;
        }

        List<String> ignoredBeanNames = Arrays.asList("RemoteEventQuery", "stringToTypeConverter", "springCDIExtension", "springApplicationArguments",
                        "springBootLog.*", "org.springframework.*", "spring.cloud.*", "InternalQuery.*", "internalQuery.*", "bootstrap.*",
                        "DefaultUUIDQueryLogics", "MyRemoteUserOps");
        for (String ignored : ignoredBeanNames) {
            if (beanName.matches(ignored)) {
                return false;
            }
        }

        return true;
    }

    private static String writeBeanRefContextForCompileTimeProperties(String baseName, String profile) throws IOException {
        Path profilePath = springXmlFilesDir.resolve("profiles").resolve(profile);
        assertTrue(Files.isDirectory(profilePath), "profile directory does not exist: " + profilePath);
        String profilePathUri = profilePath.toUri().toString();
        // @formatter:off
        SpringContextProperties springContextProperties = new SpringContextProperties();
        List<String> sources = Arrays.asList(
                "classpath*:datawave/configuration/spring/CDIBeanPostProcessor.xml",
                profilePathUri + "datawave/marking/MarkingFunctionsContext.xml",
                profilePathUri + "datawave/annotation/AnnotationManagerFactory.xml",
                profilePathUri + "datawave/metadata/MetadataHelperContext.xml",
                profilePathUri + "datawave/modification/ModificationServices.xml",
                profilePathUri + "datawave/query/CachedResults*.xml",
                profilePathUri + "datawave/query/QueryExpiration.xml",
                profilePathUri + "datawave/query/QueryLimiterFactory.xml",
                profilePathUri + "datawave/query/QueryMetricsWriter.xml",
                profilePathUri + "datawave/security/TestDatawaveUserServiceConfiguration.xml",
                "classpath*:org/apache/accumulo/operations/LookupBean.xml",
                "classpath*:CacheContext.xml",
                profilePathUri + "datawave/query/QueryLogicFactory.xml",
                profilePathUri + "datawave/query/EdgeQueryLogicFactory.xml",
                profilePathUri + "datawave/query/KeywordExtractionQueryLogicFactory.xml",
                profilePathUri + "datawave/query/SSDeepQueryLogicFactory.xml",
                profilePathUri + "datawave/RemoteServices.xml",
                profilePathUri + "datawave/security/SSLStoresProducer.xml"
        );

        springContextProperties.setScanBasePackages(Collections.emptyList());

        springContextProperties.setSources(sources);
        springContextProperties.setUseBootstrapContext(false);

        Path beanRefContextPath = testClassesDir.resolve("beanRefContext-" + baseName + "-" + profile + ".xml");
        writeSpringContextPropertiesXml(springContextProperties, beanRefContextPath);
        return beanRefContextPath.getFileName().toString();
    }

    private static String writeBeanRefContextForSpringBootProperties(String baseName, String profile) throws IOException {
        Path profilePath = springXmlFilesDir.resolve("profiles").resolve(profile);
        assertTrue(Files.isDirectory(profilePath), "profile directory does not exist: " + profilePath);
        String profilePathUri = profilePath.toUri().toString();
        // @formatter:off
        SpringContextProperties springContextProperties = new SpringContextProperties();
        List<String> sources = Arrays.asList(
                "classpath*:datawave-new/configuration/spring/CDIBeanPostProcessor.xml",
                profilePathUri + "datawave-new/marking/MarkingFunctionsContext.xml",
                profilePathUri + "datawave-new/annotation/AnnotationManagerFactory.xml",
                profilePathUri + "datawave-new/modification/ModificationServices.xml",
                profilePathUri + "datawave-new/query/CachedResults*.xml",
                profilePathUri + "datawave-new/query/QueryExpiration.xml",
                profilePathUri + "datawave-new/query/QueryLimiterFactory.xml",
                profilePathUri + "datawave-new/security/TestDatawaveUserServiceConfiguration.xml",
                profilePathUri + "datawave-new/RemoteServices.xml",
                profilePathUri + "datawave-new/security/SSLStoresProducer.xml",
                "classpath*:org/apache/accumulo/operations/LookupBean.xml",
                "classpath*:CacheContext.xml",
                "classpath*:QueryLogicFactory.xml",
                "classpath*:EdgeQueryLogicFactory.xml",
                "classpath*:KeywordExtractionQueryLogicFactory.xml",
                "classpath*:SSDeepQueryLogicFactory.xml",
                "classpath*:FederatedLogicFactory.xml"
        );

        springContextProperties.setSources(sources);
        springContextProperties.setScanBasePackages(Arrays.asList(
                "datawave.microservice.metadata.config",
                "datawave.microservice.query.edge.config",
                "datawave.microservice.query.logic",
                "datawave.webservice.config",
                "datawave.microservice.query.federation.config",
                "datawave.microservice.config.web"
        ));
        springContextProperties.setUseBootstrapContext(true);
        // @formatter:on

        Path beanRefContextPath = testClassesDir.resolve("beanRefContext-" + baseName + "-" + profile + ".xml");
        writeSpringContextPropertiesXml(springContextProperties, beanRefContextPath);
        return beanRefContextPath.getFileName().toString();
    }

    private static void writeSpringContextPropertiesXml(SpringContextProperties springContextProperties, Path outputPath) throws IOException {
        XMLOutputFactory outputFactory = XMLOutputFactory.newFactory();

        try (OutputStream outputStream = Files.newOutputStream(outputPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE)) {
            XMLStreamWriter writer = outputFactory.createXMLStreamWriter(outputStream, "UTF-8");
            try {
                writer.writeStartDocument("UTF-8", "1.0");
                writer.writeCharacters("\n\n");

                writer.writeStartElement("beans");
                writer.writeDefaultNamespace("http://www.springframework.org/schema/beans");
                writer.writeNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
                writer.writeAttribute("http://www.w3.org/2001/XMLSchema-instance", "schemaLocation",
                                "http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-4.0.xsd");
                writer.writeAttribute("default-autowire", "no");

                writer.writeCharacters("\n    ");
                writer.writeStartElement("bean");
                writer.writeAttribute("class", SpringContextProperties.class.getName());
                writer.writeAttribute("autowire", "no");

                writer.writeCharacters("\n        ");
                writer.writeEmptyElement("property");
                writer.writeAttribute("name", "useBootstrapContext");
                writer.writeAttribute("value", String.valueOf(springContextProperties.getUseBootstrapContext()));

                writeListProperty(writer, "scanBasePackages", springContextProperties.getScanBasePackages());
                writeListProperty(writer, "sources", springContextProperties.getSources());

                writer.writeCharacters("\n    ");
                writer.writeEndElement();
                writer.writeCharacters("\n");
                writer.writeEndElement();
                writer.writeCharacters("\n");
                writer.writeEndDocument();
                writer.flush();
            } finally {
                writer.close();
            }
        } catch (XMLStreamException e) {
            throw new IOException("Failed to write SpringContextProperties XML to " + outputPath, e);
        }
    }

    private static void writeListProperty(XMLStreamWriter writer, String propertyName, List<String> values) throws XMLStreamException {
        writer.writeCharacters("\n        ");
        writer.writeStartElement("property");
        writer.writeAttribute("name", propertyName);

        writer.writeCharacters("\n            ");
        writer.writeStartElement("list");

        for (String value : values) {
            writer.writeCharacters("\n                ");
            writer.writeStartElement("value");
            writer.writeCharacters(value);
            writer.writeEndElement();
        }

        writer.writeCharacters("\n            ");
        writer.writeEndElement();
        writer.writeCharacters("\n        ");
        writer.writeEndElement();
    }

    private static void replaceTextInFile(String filePath, Map<String,String> replacemeents) throws IOException {
        Path path = Path.of(filePath);
        String fileContents = Files.readString(path);
        for (Map.Entry<String,String> entry : replacemeents.entrySet()) {
            fileContents = fileContents.replace(entry.getKey(), entry.getValue());
        }
        Files.writeString(path, fileContents);
    }

    private static List<Path> findFileInTargetDirectoryContaining(Path targetDir, String text) throws IOException {
        List<Path> matches = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(targetDir)) {
            paths.filter(Files::isRegularFile).forEach(path -> {
                try {
                    String fileContents = Files.readString(path);
                    if (fileContents.contains(text)) {
                        matches.add(path);
                    }
                } catch (IOException e) {
                    log.error("Error reading file: " + path, e);
                }
            });
            return matches;
        }
    }

    /*
     * fixes internal classpath references within this profile's directory
     */
    private static void fixClasspathReferencesInSpringConfigFiles(String profile) throws IOException {

        try {
            Path profilePath = springXmlFilesDir.resolve("profiles").resolve(profile);
            assertTrue(Files.isDirectory(profilePath), "profile directory does not exist: " + profilePath);
            String profilePathUri = profilePath.toUri().toString();
            List<Path> matches = findFileInTargetDirectoryContaining(profilePath, "classpath:datawave");
            Map<String,String> replacements = new HashMap<>();

            replacements.put("classpath:datawave", String.format("%sdatawave", profilePathUri));
            for (Path match : matches) {
                replaceTextInFile(match.toString(), replacements);
            }
        } catch (IOException e) {
            log.error("Exception while replacing text in files: " + e.getMessage(), e);
            throw e;
        }
    }

    private static Collection<Class<?>> comparisonIgnoredClasses() {
        // @formatter:off
        return Arrays.asList(
                QueryStopwatch.class,
                DefaultTagCloudUtils.class,
                SSDeepSimilarityQueryState.class,
                KeywordQueryState.class,
                RemoteHttpService.class,
                datawave.configuration.spring.SpringContextReport.class,
                datawave.configuration.spring.SpringCDIConfiguration.class,
                datawave.configuration.spring.SpringContextProperties.class,
                org.springframework.context.ApplicationContext.class,
                org.springframework.beans.factory.config.ConfigurableListableBeanFactory.class,
                java.util.logging.Logger.class,
                org.slf4j.Logger.class,
                org.apache.log4j.Logger.class,
                org.apache.commons.logging.Log.class,
                org.apache.logging.log4j.status.StatusLogger.class,
                java.lang.System.Logger.class,
                java.util.Locale.class,
                com.fasterxml.jackson.databind.ObjectMapper.class,
                java.lang.Module.class,
                java.lang.ModuleLayer.class,
                java.math.RoundingMode.class,
                java.util.Locale.class,
                java.util.concurrent.locks.Lock.class,
                java.lang.ThreadGroup.class,
                java.lang.reflect.Method.class,
                java.lang.ref.SoftReference.class,
                java.text.SimpleDateFormat.class
        );

    }

    private static String createTempDirectory(String prefix) throws IOException {
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxr-xr-x");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        return String.valueOf(Files.createTempDirectory(prefix + "_", attr));
    }

    private static class MockAccumuloConnectionFactory implements AccumuloConnectionFactory {
        private InMemoryInstance inMemoryInstance = new InMemoryInstance();

        public MockAccumuloConnectionFactory(String username, String authorizations) {
            try (AccumuloClient client = new InMemoryAccumuloClient(username, inMemoryInstance)) {
                String[] authArray = Arrays.stream(authorizations.split(",")).toArray(String[]::new);
                client.securityOperations().changeUserAuthorizations(client.whoami(), new Authorizations(authArray));
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, Priority priority, Map<String,String> trackingMap) throws Exception {
            return new InMemoryAccumuloClient("root", inMemoryInstance);
        }

        @Override
        public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, String poolName, Priority priority, Map<String,String> trackingMap)
                throws Exception {
            return new InMemoryAccumuloClient("root", inMemoryInstance);
        }

        @Override
        public void returnClient(AccumuloClient client) {

        }

        @Override
        public String report() {
            return null;
        }

        @Override
        public List<ConnectionPool> getConnectionPools() {
            return null;
        }

        @Override
        public int getConnectionUsagePercent() {
            return 0;
        }

        @Override
        public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
            return new HashMap<>();
        }

        @Override
        public void close() throws Exception {

        }
    }
}
