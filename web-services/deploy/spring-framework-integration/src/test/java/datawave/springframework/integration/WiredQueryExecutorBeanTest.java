package datawave.springframework.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import datawave.configuration.spring.SpringCDIExtension;
import datawave.configuration.spring.SpringContextProperties;
import datawave.marking.SecurityMarkingProducer;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.tables.keyword.KeywordQueryState;
import datawave.query.tables.ssdeep.SSDeepSimilarityQueryState;
import datawave.query.util.QueryStopwatch;
import datawave.security.cache.CredentialsCacheBean;
import datawave.security.cache.ElytronCacheManagerImpl;
import datawave.security.user.UserOperationsBean;
import datawave.util.keyword.DefaultTagCloudUtils;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.config.DropwizardMetricsConfiguration;

@ExtendWith(ArquillianExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class WiredQueryExecutorBeanTest {

    private static final Logger log = Logger.getLogger(WiredQueryExecutorBeanTest.class);

    @ArquillianResource
    private Deployer deployer;

    private static final String COMPILED_PROPERTIES_PROFILE_DEV = "DEPLOYMENT_COMPILED_PROPERTIES_DEV_PROFILE";
    private static final String SPRING_BOOT_PROPERTIES = "DEPLOYMENT_SPRING_BOOT_PROPERTIES";

    private static final Map<String,Map<String,Object>> SNAPSHOTS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String,Boolean> DEPLOYED = new ConcurrentHashMap<>();
    // @formatter:off
    private static final List<ContextConfig> CONTEXT_CONFIGS = List.of(
            new ContextConfig("DEV", "dev", "query,dev,federation")
    );
    // @formatter:on
    private static TestingServer zookeeperServer;
    private static Deployer deploymentDeployer;

    @Deployment(name = SPRING_BOOT_PROPERTIES, managed = false)
    public static WebArchive createDeploymentWebArchive() {
        // @formatter:off
        return ShrinkWrap.create(WebArchive.class)
                .addPackages(true,
                        "datawave.configuration",
                        "datawave.core.query",
                        "datawave.data.type",
                        "datawave.microservice.query.logic.config",
                        "datawave.query",
                        "datawave.springframework.integration",
                        "datawave.webservice",
                        "io.astefanutti.metrics.cdi",
                        "org.apache.deltaspike"
                )
                .deletePackage("datawave.webservice.common.connection")
                // add discrete classes that are not in the above packages because bean discovery
                // in the CDI deployment is handled differently than when Wildfly is run. We do not
                // want all of the classes from the packages of the classes below to be created as CDI beans.
                .addClasses(
                        SecurityMarkingProducer.class,
                        QueryMetricFactoryImpl.class,
                        UserOperationsBean.class,
                        CredentialsCacheBean.class,
                        DefaultQueryParameters.class,
                        ElytronCacheManagerImpl.class
                )
                .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
        // @formatter:on
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        configureSystemProperties();
        // There are a few classpath references in the spring XML files that need to be updated to use
        // the package/path names that correspond to where the file are located in the target directory.
        ContextHelper.fixClasspathReferencesInSpringConfigFiles("spring-boot", "datawave-new");
        for (ContextConfig c : CONTEXT_CONFIGS) {
            ContextHelper.fixClasspathReferencesInSpringConfigFiles(c.getCompileContext(), "datawave");
        }
        // Start a Zookeeper server for the duration of the tests. This is required for the AccumuloTableCache
        // SharedCacheCoordinator which is enabled in default.properties.
        zookeeperServer = new TestingServer(2181, true);
    }

    @BeforeEach
    public void beforeTest() {
        // deployer is injected per-test, but we want to clean up after all tests
        // have run, so we will save the first injected deployer for use in @AfterAll
        if (deploymentDeployer == null) {
            deploymentDeployer = deployer;
        }
    }

    @AfterAll
    public static void undeployArchives() throws IOException {
        try {
            if (deploymentDeployer != null) {
                if (!Boolean.TRUE.equals(DEPLOYED.get(SPRING_BOOT_PROPERTIES))) {
                    return;
                }
                try {
                    deploymentDeployer.undeploy(SPRING_BOOT_PROPERTIES);
                } catch (RuntimeException e) {
                    log.warn(String.format("Ignoring %s undeploy failure during teardown: %s", SPRING_BOOT_PROPERTIES, e.getMessage()));
                } finally {
                    DEPLOYED.put(SPRING_BOOT_PROPERTIES, false);
                }
            }
        } finally {
            if (zookeeperServer != null) {
                zookeeperServer.close();
            }
        }
    }

    /*
     * Deploy the WebArchive using the first context configuration's Spring Boot properties to verify that the CDI beans required for created SpringBeans get
     * properly injected into those beans. We can then use the BeanManager from this deployment to satisfy injection for all contexts in the subsequent Spring
     * contexts.
     */
    @Test
    @OperateOnDeployment(SPRING_BOOT_PROPERTIES)
    public void test01DeployWebArchive() throws IOException {
        Properties certificateProperties = new Properties();
        certificateProperties.load(WiredQueryExecutorBeanTest.class.getResourceAsStream("/certificate.properties"));
        System.setProperty("server.ssl.key-store", "file://" + certificateProperties.getProperty("KEYSTORE"));
        System.setProperty("server.ssl.trust-store", "file://" + certificateProperties.getProperty("TRUSTSTORE"));
        System.setProperty("spring.cloud.config.enabled", "false");
        System.setProperty("spring.profiles.active", CONTEXT_CONFIGS.get(0).getActiveProfiles());
        SpringContextProperties springContextProperties = getSpringContextForSpringBootProperties("spring-boot");
        System.setProperty("cdi.bean.context",
                        ContextHelper.writeBeanRefContextForSpringBootProperties(springContextProperties, "SpringBootProperties", "spring-boot"));
        deployer.deploy(SPRING_BOOT_PROPERTIES);
    }

    @Test
    @OperateOnDeployment(SPRING_BOOT_PROPERTIES)
    public void test02CreateContextsUsingSpringBootProperties() throws IOException {
        // Since we deployed using Spring Boot properties, the BeanProvider is initialized and active. These contexts
        // should use the BeanManager from that deployment via the BeanProvider to @Inject any required CDI beans.
        // Starting and stopping multiple CDI contexts in the same JVM is not well-supported, so we will use the
        // BeanManager from the first deployment to satisfy injection for all contexts.
        for (ContextConfig c : CONTEXT_CONFIGS) {
            ClassPathXmlApplicationContext context = createSpringApplicationSpringBoot(c.getActiveProfiles());
            // snapshot the beans that we want to compare between the two deployments
            // this can be removed or modified when compile time properties are no longer used
            SNAPSHOTS.put("SPRING_BOOT_" + c.getName(), snapshotContext(context));
        }

    }

    @Test
    @OperateOnDeployment(SPRING_BOOT_PROPERTIES)
    public void test03CreateContextsUsingCompiledProperties() throws IOException {
        // Since we deployed using Spring Boot properties, the BeanProvider is initialized and active. These contexts
        // should use the BeanManager from that deployment via the BeanProvider to @Inject any required CDI beans.
        // Starting and stopping multiple CDI contexts in the same JVM is not well-supported, so we will use the
        // BeanManager from the first deployment to satisfy injection for all contexts.
        for (ContextConfig c : CONTEXT_CONFIGS) {
            ClassPathXmlApplicationContext context = createSpringApplicationCompileProperties(c.getCompileContext());
            // snapshot the beans that we want to compare between the two deployments
            // this can be removed or modified when compile time properties are no longer used
            SNAPSHOTS.put("COMPILED_" + c.getName(), snapshotContext(context));
        }
    }

    @Test
    public void test04CompareDeployments() {
        for (ContextConfig c : CONTEXT_CONFIGS) {
            String contextSpringBoot = "SPRING_BOOT_" + c.getName();
            String contextCompiled = "COMPILED_" + c.getName();
            compareDeployments(contextSpringBoot, contextCompiled);
        }
    }

    private ClassPathXmlApplicationContext createSpringApplicationSpringBoot(String springBootProfiles) throws IOException {
        Properties certificateProperties = new Properties();
        certificateProperties.load(WiredQueryExecutorBeanTest.class.getResourceAsStream("/certificate.properties"));
        System.setProperty("server.ssl.key-store", "file://" + certificateProperties.getProperty("KEYSTORE"));
        System.setProperty("server.ssl.trust-store", "file://" + certificateProperties.getProperty("TRUSTSTORE"));
        System.setProperty("spring.cloud.config.enabled", "false");
        System.setProperty("spring.profiles.active", springBootProfiles);
        return SpringCDIExtension.createApplicationContext(getSpringContextForSpringBootProperties("spring-boot"));
    }

    private ClassPathXmlApplicationContext createSpringApplicationCompileProperties(String compileTimeProfile) throws IOException {
        System.setProperty("spring.cloud.config.enabled", "false");
        System.setProperty("table.cache.enabled", "false");
        return SpringCDIExtension.createApplicationContext(getSpringContextForCompileTimeProperties(compileTimeProfile));
    }

    private void compareDeployments(String springBootContext, String compileTimeContext) {
        Map<String,Object> springBootDeployment = SNAPSHOTS.get(springBootContext);
        Map<String,Object> compileTimeDeployment = SNAPSHOTS.get(compileTimeContext);

        Set<String> onlyInOne = new TreeSet<>(compileTimeDeployment.keySet());
        onlyInOne.removeAll(springBootDeployment.keySet());
        Set<String> onlyInTwo = new TreeSet<>(springBootDeployment.keySet());
        onlyInTwo.removeAll(compileTimeDeployment.keySet());

        assertTrue(onlyInOne.isEmpty(), String.format("bean name mismatch: only in %s=%s, only in %s=%s", COMPILED_PROPERTIES_PROFILE_DEV, onlyInOne,
                        SPRING_BOOT_PROPERTIES, onlyInTwo));

        Collection<Class<?>> ignoredClasses = comparisonIgnoredClasses();
        List<SpringContextComparator.ComparisonDifference> diffs = new ArrayList<>();
        for (String beanName : new TreeSet<>(compileTimeDeployment.keySet())) {
            Object beanOne = springBootDeployment.get(beanName);
            Object beanTwo = compileTimeDeployment.get(beanName);
            log.info("Comparing bean " + beanName + " of type " + beanOne.getClass().getCanonicalName() + " to type " + beanTwo.getClass().getCanonicalName());
            diffs.addAll(SpringContextComparator.compareObjectsRecursive(beanName, beanOne, beanTwo, ignoredClasses));
        }

        if (!diffs.isEmpty()) {
            log.error("Bean value differences found between deployments:");
            diffs.forEach(d -> log.error("  " + d));
            fail("Deployments have different bean values: " + diffs);
        }
    }

    private static void configureSystemProperties() {
        System.setProperty("datawave.configuration.spring.configure-from-properties", "false");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");

        Path repoRoot = Path.of(System.getProperty("user.dir")).getParent().getParent().getParent();
        System.setProperty("pki.dir", repoRoot.resolve("docker/pki").toUri().toString());

        String tempDirPrefix = WiredQueryExecutorBeanTest.class.getSimpleName();
        System.setProperty("datawave.configuration.spring.debug-dir", SpringContextProperties.debugDir(System.getProperty("java.io.tmpdir"), tempDirPrefix));

        Path springYmlFilesDir = Path.of(System.getProperty("user.dir"), "target").resolve("springymlfiles");
        assertTrue(Files.isDirectory(springYmlFilesDir), "springYmlFilesDir does not exist: " + springYmlFilesDir);
        System.setProperty("spring.config.location", springYmlFilesDir.toUri().toString());
        System.setProperty("spring.config.name", "application,wildfly");

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

        // @formatter:off
        List<String> ignoredBeanNames = Arrays.asList(
                "stringToTypeConverter",
                "springCDIExtension",
                "springApplicationArguments",
                "springBootLog.*",
                "org.springframework.*",
                "spring.cloud.*",
                "InternalQuery.*",
                "internalQuery.*",
                "bootstrap.*",
                "DefaultUUIDQueryLogics",
                "MyRemoteUserOps",
                "dateIndexHelper",
                "wiredQueryModelProvider",
                "sslStoresBuilder.*",
                "proxiedUserDetailsSupplier",
                "serverPrincipalProducer",
                "injectCDIBeanPostProcessor",
                "accumuloTableCache.*"
        );
        // @formatter:on
        for (String ignored : ignoredBeanNames) {
            if (beanName.matches(ignored)) {
                return false;
            }
        }
        return true;
    }

    private static Collection<Class<?>> comparisonIgnoredClasses() {
        // @formatter:off
        return Arrays.asList(
                // Suppliers can have different implementations; do not inspect captures or invoke remote lookups.
                java.util.function.Supplier.class,
                QueryStopwatch.class,
                DefaultTagCloudUtils.class,
                DropwizardMetricsConfiguration.class,
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
                java.text.SimpleDateFormat.class,
                java.util.concurrent.TimeUnit.class,
                java.util.concurrent.Future.class,
                java.util.concurrent.FutureTask.class,
                java.util.concurrent.ThreadPoolExecutor.class,
                datawave.core.common.cache.TableCache.class,
                datawave.core.common.cache.SharedCacheCoordinator.class
        );
    }

    public static SpringContextProperties getSpringContextForCompileTimeProperties(String profile) {
        Path profilePath = ContextHelper.springXmlFilesDir.resolve("profiles").resolve(profile);
        assertTrue(Files.isDirectory(profilePath), "profile directory does not exist: " + profilePath);
        String profilePathUri = profilePath.toUri().toString();
        // @formatter:off
        SpringContextProperties springContextProperties = new SpringContextProperties();
        List<String> sources = Arrays.asList(
                "classpath*:datawave/configuration/spring/CDIBeanPostProcessor.xml",
                profilePathUri + "datawave/marking/MarkingFunctionsContext.xml",
                profilePathUri + "datawave/annotation/AnnotationManagerFactory.xml",
                profilePathUri + "datawave/atom/Atom.xml",
                profilePathUri + "datawave/metadata/MetadataHelperContext.xml",
                profilePathUri + "datawave/model/Model.xml",
                profilePathUri + "datawave/modification/ModificationServices.xml",
                profilePathUri + "datawave/query/CachedResults*.xml",
                profilePathUri + "datawave/query/QueryExpiration.xml",
                profilePathUri + "datawave/query/QueryLimiterFactory.xml",
                profilePathUri + "datawave/query/QueryMetrics.xml",
                profilePathUri + "datawave/query/QueryLogicFactory.xml",
                profilePathUri + "datawave/query/EdgeQueryLogicFactory.xml",
                profilePathUri + "datawave/query/KeywordExtractionQueryLogicFactory.xml",
                profilePathUri + "datawave/query/SSDeepQueryLogicFactory.xml",
                profilePathUri + "datawave/security/SecurityProducer.xml",
                profilePathUri + "datawave/security/TestDatawaveUserServiceConfiguration.xml",
                profilePathUri + "datawave/AccumuloConnectionFactory.xml",
                profilePathUri + "datawave/DropwizardMetrics.xml",
                profilePathUri + "datawave/RemoteServices.xml",
                "classpath*:org/apache/accumulo/operations/LookupBean.xml",
                "classpath*:CacheContext.xml"
        );

        springContextProperties.setScanBasePackages(Collections.emptyList());

        springContextProperties.setSources(sources);
        springContextProperties.setUseBootstrapContext(false);
        // @formatter:on
        return springContextProperties;
    }

    public static SpringContextProperties getSpringContextForSpringBootProperties(String profile) {
        Path profilePath = ContextHelper.springXmlFilesDir.resolve("profiles").resolve(profile);
        assertTrue(Files.isDirectory(profilePath), "profile directory does not exist: " + profilePath);
        String profilePathUri = profilePath.toUri().toString();
        // @formatter:off
        SpringContextProperties springContextProperties = new SpringContextProperties();
        List<String> sources = Arrays.asList(
                "classpath*:datawave/configuration/spring/CDIBeanPostProcessor.xml",
                profilePathUri + "datawave-new/marking/MarkingFunctionsContext.xml",
                profilePathUri + "datawave-new/annotation/AnnotationManagerFactory.xml",
                profilePathUri + "datawave-new/modification/ModificationServices.xml",
                profilePathUri + "datawave-new/query/CachedResults*.xml",
                profilePathUri + "datawave-new/query/QueryExpiration.xml",
                profilePathUri + "datawave-new/query/QueryLimiterFactory.xml",
                profilePathUri + "datawave-new/security/TestDatawaveUserServiceConfiguration.xml",
                profilePathUri + "datawave-new/RemoteServices.xml",
                "classpath*:org/apache/accumulo/operations/LookupBean.xml",
                "classpath*:CacheContext.xml",
                "classpath*:QueryLogicFactory.xml",
                "classpath*:EdgeQueryLogicFactory.xml",
                "classpath*:KeywordExtractionQueryLogicFactory.xml",
                "classpath*:SSDeepQueryLogicFactory.xml"
        );

        springContextProperties.setSources(sources);
        springContextProperties.setScanBasePackages(Arrays.asList(
                "datawave.microservice.config.marking",
                "datawave.microservice.metadata.config",
                "datawave.microservice.query.edge.config",
                "datawave.microservice.query.logic",
                "datawave.webservice.config"
        ));
        springContextProperties.setUseBootstrapContext(true);
        // @formatter:on
        return springContextProperties;
    }

    private static class ContextConfig {
        private final String name;
        private final String compileContext;
        private final String activeProfiles;

        public ContextConfig(String name, String compileContext, String activeProfiles) {
            this.name = name;
            this.compileContext = compileContext;
            this.activeProfiles = activeProfiles;
        }

        public String getName() {
            return name;
        }

        public String getCompileContext() {
            return compileContext;
        }

        public String getActiveProfiles() {
            return activeProfiles;
        }
    }
}
