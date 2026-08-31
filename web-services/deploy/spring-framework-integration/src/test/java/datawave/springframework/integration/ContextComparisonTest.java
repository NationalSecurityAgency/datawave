package datawave.springframework.integration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import datawave.configuration.spring.SpringCDIExtension;
import datawave.configuration.spring.SpringContextProperties;
import datawave.configuration.spring.SpringContextReport;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.query.tables.keyword.KeywordQueryState;
import datawave.query.tables.ssdeep.SSDeepSimilarityQueryState;
import datawave.query.util.QueryStopwatch;
import datawave.util.keyword.DefaultTagCloudUtils;

public class ContextComparisonTest {

    private static Logger log = LoggerFactory.getLogger(ContextComparisonTest.class);

    private static final String CLASSNAME = ContextComparisonTest.class.getSimpleName();
    private static final String PACKAGED_QLF_COMPILE_TIME_PROPERTIES = CLASSNAME + "_Packaged_QLF_CompileTime_Properties";
    private static final String QUERYSTARTER_QLF_RUNTIME_PROPERTIES = CLASSNAME + "_QueryStarter_QLF_Runtime_Properties";
    private static Set<Class> skipClasses;

    @BeforeAll
    public static void setupClass() {
        // these are stateful objects that are found in query logics or helper objects that so not have configuration
        // @formatter:off
        skipClasses = new HashSet<>(Arrays.asList(
                QueryStopwatch.class,
                DefaultTagCloudUtils.class,
                SSDeepSimilarityQueryState.class,
                KeywordQueryState.class));
        // @formatter:on
    }

    @Test
    public void testContextComparison() throws Exception {

        System.setProperty("spring.profiles.active", "query");
        SpringContextProperties springContextProperties = new SpringContextProperties();

        // context1 gets XML files from the datawave-ws-deploy-configuration module and does not search packages for spring beans / configuration
        // @formatter:off
        springContextProperties.setSources(Arrays.asList(
                        "classpath*:NoOpMarkingFunctionsContext.xml",
                        "classpath*:CacheContext.xml",
                        "classpath*:datawave-compile-time-properties/metadata/MetadataHelperContext.xml",
                        "classpath*:datawave-compile-time-properties/security/PrincipalFactory.xml",
                        "classpath*:datawave-compile-time-properties/query/QueryExpiration.xml",
                        "classpath*:datawave-compile-time-properties/query/*QueryLogicFactory.xml",
                        "classpath*:datawave-compile-time-properties/query/CachedResults*.xml",
                        "classpath*:PropertyPlaceholder.xml"
        ));
        // @formatter:on
        springContextProperties.setUseBootstrapContext(false);

        // this is set in system properties in standalone-full.xml
        System.setProperty("dw.metadatahelper.all.auths", "PUBLIC");
        // set property.dir to allow substitution of properties when this test is run from an IDE, and the XML files have not yet been filtered
        Path baseDir = Path.of(System.getProperty("user.dir"));
        Path propertyDir = Path.of(baseDir + "/../../..", "properties");
        System.setProperty("property.dir", propertyDir.toFile().getCanonicalFile().getAbsolutePath());
        System.setProperty("spring.context.debug.dir", createTempDirectory(PACKAGED_QLF_COMPILE_TIME_PROPERTIES));
        ClassPathXmlApplicationContext context1 = SpringCDIExtension.createApplicationContext(springContextProperties);
        if (log.isTraceEnabled()) {
            SpringContextReport springContextReport = context1.getBean(SpringContextReport.class);
            log.trace(String.format("%s report:\n%s", PACKAGED_QLF_COMPILE_TIME_PROPERTIES, springContextReport.getReport()));
        }

        // context2 gets query logic XML files from the query-starter module, searches packages for spring beans / configuration,
        // and gets the property values from the bootstrap context (yml files from classpath or operationally from configuration service)
        // @formatter:off
        springContextProperties.setSources(Arrays.asList(
                        "classpath*:NoOpMarkingFunctionsContext.xml",
                        "classpath*:QueryLogicFactory.xml",
                        "classpath*:EdgeQueryLogicFactory.xml",
                        "classpath*:SSDeepQueryLogicFactory.xml",
                        "classpath*:KeywordExtractionQueryLogicFactory.xml"
        ));
        springContextProperties.setScanBasePackages(Arrays.asList(
                        "datawave.microservice.metadata.config",
                        "datawave.microservice.query.edge.config",
                        "datawave.microservice.query.logic.config"
        ));
        springContextProperties.setUseBootstrapContext(true);
        // @formatter:on
        System.setProperty("spring.context.debug.dir", createTempDirectory(QUERYSTARTER_QLF_RUNTIME_PROPERTIES));
        ClassPathXmlApplicationContext context2 = SpringCDIExtension.createApplicationContext(springContextProperties);
        if (log.isTraceEnabled()) {
            SpringContextReport springContextReport = context2.getBean(SpringContextReport.class);
            log.trace(String.format("%s report:\n%s", QUERYSTARTER_QLF_RUNTIME_PROPERTIES, springContextReport.getReport()));
        }
        compareContexts(context1, PACKAGED_QLF_COMPILE_TIME_PROPERTIES, context2, QUERYSTARTER_QLF_RUNTIME_PROPERTIES,
                        Arrays.asList("RemoteEventQuery", "InternalQueryMetricsQuery"));
    }

    private void compareContexts(ClassPathXmlApplicationContext context1, String context1Description, ClassPathXmlApplicationContext context2,
                    String context2Description, List<String> ignoreBeans) {
        List<String> beanNamesContext1 = Arrays.stream(context1.getBeanNamesForType(BaseQueryLogic.class)).filter(o -> !ignoreBeans.contains(o))
                        .collect(Collectors.toList());
        List<String> beanNamesContext2 = Arrays.stream(context2.getBeanNamesForType(BaseQueryLogic.class)).filter(o -> !ignoreBeans.contains(o))
                        .collect(Collectors.toList());

        List<String> beanNames = beanNamesContext1.stream().filter(o -> !beanNamesContext2.contains(o)).collect(Collectors.toList());
        Assert.assertTrue(String.format("beans that exist in %s but not %s: %s", context1Description, context2Description, beanNames), beanNames.isEmpty());
        beanNames = beanNamesContext2.stream().filter(o -> !beanNamesContext1.contains(o)).collect(Collectors.toList());
        Assert.assertTrue(String.format("beans that exist in %s but not %s: %s", context1Description, context2Description, beanNames), beanNames.isEmpty());
        beanNames = beanNamesContext1.stream().filter(o -> beanNamesContext2.contains(o)).collect(Collectors.toList());

        Map<String,Set<String>> differences = new HashMap<>();
        for (String beanName : beanNames) {
            BaseQueryLogic<Map.Entry<Key,Value>> bql1 = context1.getBean(beanName, BaseQueryLogic.class);
            BaseQueryLogic<Map.Entry<Key,Value>> bql2 = context2.getBean(beanName, BaseQueryLogic.class);

            if (bql2 == null) {
                Assert.fail(String.format("bean %s does not exist in %s", beanName, context2Description));
            }

            for (Field f : bql1.getConfig().getClass().getDeclaredFields()) {
                Object o1;
                Object o2;
                try {
                    f.setAccessible(true);
                    o1 = f.get(bql1.getConfig());
                    o2 = f.get(bql2.getConfig());
                    // convert AtomicInteger objects into Integer so that they can be compared with equals
                    if (o1 instanceof AtomicInteger) {
                        o1 = ((AtomicInteger) o1).get();
                        o2 = ((AtomicInteger) o2).get();
                    }
                    if (o1 == null || o2 == null) {
                        // if either object is null, then they should both be null
                        if (o1 != o2) {
                            log.debug(String.format("%s.%s DIFFERENCE context1: %s context2: %s", beanName, f.getName(), o1, o2));
                            Set<String> set = differences.getOrDefault(f.getName(), new TreeSet<>());
                            set.add(beanName);
                            differences.put(f.getName(), set);
                        } else {
                            log.trace(String.format("%s.%s EQUALS at value: %s", beanName, f.getName(), o1));
                        }
                    } else if (!skipClasses.contains(o1.getClass()) && !f.getName().startsWith("this")) {
                        if (!o1.equals(o2)) {
                            log.debug(String.format("%s.%s DIFFERENCE context1: %s context2: %s", beanName, f.getName(), o1, o2));
                            Set<String> set = differences.getOrDefault(f.getName(), new TreeSet<>());
                            set.add(beanName);
                            differences.put(f.getName(), set);
                        } else {
                            log.trace(String.format("%s.%s EQUALS at value: %s", beanName, f.getName(), o1));
                        }
                    }
                } catch (Exception e) {
                    log.error(String.format("%s.%s Exception: %s", beanName, f.getName(), e.getMessage()), e);
                    Assert.fail(String.format("%s.%s Exception: %s", beanName, f.getName(), e.getMessage()));
                }
            }
        }
        Assert.assertTrue(String.format("differences between %s and %s: %s", context1Description, context2Description, differences), differences.isEmpty());
    }

    private String createTempDirectory(String prefix) throws IOException {
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxr-xr-x");
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
        return String.valueOf(Files.createTempDirectory(prefix + "_", attr));
    }
}
