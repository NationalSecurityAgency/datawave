package datawave.configuration.spring;

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.enterprise.context.Dependent;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.inject.spi.PassivationCapable;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import javax.enterprise.util.AnnotationLiteral;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.cloud.bootstrap.config.BootstrapPropertySource;
import org.springframework.cloud.config.client.ConfigServerBootstrapper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.PropertySourcesPropertyResolver;
import org.springframework.core.io.Resource;

import com.google.common.collect.Sets;

import datawave.configuration.ConfigurationEvent;
import datawave.configuration.RefreshableScope;

/**
 * A portable CDI extension that exposes Spring Beans as CDI beans. This extension expects to find a beanRefContext.xml file on the classpath. This file should
 * define a single {@link ClassPathXmlApplicationContext} which then contains the beans that will be exposed as CDI beans. The single
 * {@link ClassPathXmlApplicationContext} can be injected as a standard CDI bean. Any beans provided by Spring should be qualified with {@link SpringBean}.
 */
public class SpringCDIExtension implements Extension {
    private static Logger log = LoggerFactory.getLogger(SpringCDIExtension.class);
    private ClassPathXmlApplicationContext springContext = null;
    private final ReadWriteLock springContextLock = new ReentrantReadWriteLock(true);
    private final HashMap<String,SpringCDIBean> springBeans = new HashMap<>();

    @SuppressWarnings("unused")
    <T> void processInjectionTarget(@Observes ProcessInjectionTarget<T> pit, BeanManager bm) {
        log.trace("processInjectionTarget({},{})", pit, bm);

        synchronized (springBeans) {
            Set<InjectionPoint> injectionPoints = pit.getInjectionTarget().getInjectionPoints();
            for (InjectionPoint ip : injectionPoints) {
                Type type = ip.getType();
                // Skip primitives
                if (!(type instanceof Class<?> || type instanceof ParameterizedType))
                    continue;

                SpringBean sb = ip.getAnnotated().getAnnotation(SpringBean.class);
                if (sb != null) {
                    String key = sb.name() + ":" + type;
                    if (!springBeans.containsKey(key)) {
                        SpringCDIBean scb = sb.refreshable() ? new RefreshableSpringCDIBean(sb, type, bm) : new SpringCDIBean(sb, type, bm);
                        springBeans.put(key, scb);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unused")
    void beforeBeanDiscovery(@Observes BeforeBeanDiscovery bbd, BeanManager bm) {}

    private static String getContextDebugPath(ClassPathXmlApplicationContext context) {
        String debugDir = context.getEnvironment().getProperty("spring.context.debug.dir");
        if (debugDir == null) {
            debugDir = System.getProperty("spring.context.debug.dir");
        }
        return debugDir;
    }

    private static Path createContextDebugPath(String debugDir) {
        if (debugDir != null) {
            final Path debugPath = Paths.get(debugDir);
            try {
                List<String> subdirs = Arrays.asList("original", "filtered");
                try {
                    Files.createDirectories(debugPath);
                } catch (FileAlreadyExistsException e) {
                    log.debug(e.getMessage(), e);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                subdirs.forEach(path -> {
                    try {
                        Files.createDirectories(Path.of(debugPath.toString(), path));
                    } catch (FileAlreadyExistsException e) {
                        log.debug(e.getMessage(), e);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
                return debugPath;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            return debugPath;
        } else {
            return null;
        }
    }

    private static List<Resource> getContextFiles(ClassPathXmlApplicationContext context, List<String> configFiles) {
        List<Resource> resources = new ArrayList<>();
        try {
            for (String configFile : configFiles) {
                for (Resource r : context.getResources(configFile)) {
                    resources.add(r);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return resources;
    }

    /**
     * Write context XML files to the debugPath before and after property substitution
     *
     * @param context
     *            the spring context
     * @param debugPath
     *            the debug path
     * @param resources
     *            the resources
     */
    public static void writeContextFiles(ClassPathXmlApplicationContext context, Path debugPath, List<Resource> resources) {
        if (debugPath != null) {
            try {
                // @formatter:off
                Map<String,PropertySourcesPlaceholderConfigurer> beans = context.getBeanFactory()
                                .getBeansOfType(PropertySourcesPlaceholderConfigurer.class);
                List<PropertySourcesPropertyResolver> pspcList = beans.values().stream()
                                .map(p -> new PropertySourcesPropertyResolver(p.getAppliedPropertySources()))
                                .collect(Collectors.toList());
                // @formatter:on
                for (Resource r : resources) {
                    try (InputStream is = r.getInputStream()) {
                        String content = new String(is.readAllBytes(), "UTF-8");
                        String filteredContent = content;
                        if (content != null) {
                            for (PropertySourcesPropertyResolver resolver : pspcList) {
                                filteredContent = resolver.resolvePlaceholders(filteredContent);
                            }
                        }
                        filteredContent = context.getEnvironment().resolvePlaceholders(filteredContent);
                        Path original = Path.of(debugPath.toString(), "original", r.getFilename());
                        Files.writeString(original, content);
                        Path filtered = Path.of(debugPath.toString(), "filtered", r.getFilename());
                        Files.writeString(filtered, filteredContent);
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * Write a spring context report to the debug path.
     *
     * @param springContextReport
     *            the spring context report
     * @param debugPath
     *            the debug path
     */
    public static void writeContextReport(SpringContextReport springContextReport, Path debugPath) {
        if (debugPath != null) {
            try {
                Path original = Path.of(debugPath.toString(), "springContextReport");
                Files.writeString(original, springContextReport.getReport());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private static List<String> getPropertySources(ClassPathXmlApplicationContext context) {
        List<String> sourceList = new ArrayList<>();
        context.getEnvironment().getPropertySources().forEach(ps -> {
            if (ps instanceof BootstrapPropertySource || ps instanceof OriginTrackedMapPropertySource) {
                sourceList.add(ps.getClass().getSimpleName() + ":" + ps.getName());
            }
        });
        return sourceList;
    }

    /**
     * convenience method to use BeanFactoryUtils.containsBeanForClass which searches current and parent beanFactory hierarchy
     *
     * @param context
     * @param clazz
     * @return
     */
    private static boolean containsBeanForClass(ApplicationContext context, Class clazz) {
        return BeanFactoryUtils.beanNamesForTypeIncludingAncestors(context, clazz, true, false).length > 0;
    }

    /**
     * convenience method to use BeanFactoryUtils.containsBeanName which searches current and parent beanFactory hierarchy
     *
     * @param context
     * @param beanName
     * @return
     */
    private static boolean containsBeanName(ApplicationContext context, String beanName) {
        return Arrays.asList(BeanFactoryUtils.beanNamesIncludingAncestors(context)).contains(beanName);
    }

    /**
     * Create bootstrap application context that reads yml files from the classpath and config server
     *
     * @return the configurable application context
     */
    public static ConfigurableApplicationContext createBootstrapContext() {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(SpringCDIExtension.class);
        builder.bannerMode(Banner.Mode.OFF);
        builder.web(WebApplicationType.NONE);
        builder.addBootstrapRegistryInitializer(new ConfigServerBootstrapper());
        builder.allowCircularReferences(true);
        return builder.run();
    }

    /**
     * Load application context class path xml application context.
     *
     * @param contextProperties
     *            the context properties
     * @return the class path xml application context
     */
    public static ClassPathXmlApplicationContext createApplicationContext(SpringContextProperties contextProperties) {
        return createApplicationContext(contextProperties, null);
    }

    /**
     * 1) If bootstrapContext is null and getUseBootstrapContext is true, then one is created. (useful for testing)
     *
     * 2) Use contextProperties.getScanBasePackages() and add datawave.configuration.spring to the list of packages for the AnnotationConfigApplicationContext
     * to search for spring beans. The datawave.configuration.spring package can be used to create helper objects that can be @Inject @SpringBean elsewhere. One
     * of these is the SpringContextReport singleton, which can be used for debugging how the spring context was created. The bootstrap context will be a parent
     * context/beanFactory and will be used to supply properties.
     *
     * 3) Create ClassPathXMLApplicationContext using the sources listed in contextProperties.getSources plus either UseSpringProperties.xml or
     * EnforceSpringProperties.xml which will make sure that a PropertySourcesPlaceholderConfigurer (PSPC) with the highest (last) order is present in this
     * beanFactory. Whether application loading enforces that all placeholders must be resolved will depend on property
     * datawave.configuration.spring.ignoreUnresolvablePlaceholders. Note that a PSPC is required in both the AnnotationConfigApplicationContext (created in
     * code) and in the ClassPathXMLApplicationContext (through one of the above files).
     *
     * 4) SpringContextReport is populated with information collected during the creation of the context.
     *
     * @param contextProperties
     *            the context properties
     * @param bootstrapContext
     *            - if null, and getUseBootstrapContext is true, then one is created. (useful for testing) the bootstrap context
     * @return the class path xml application context
     */
    public static ClassPathXmlApplicationContext createApplicationContext(SpringContextProperties contextProperties,
                    ConfigurableApplicationContext bootstrapContext) {
        ClassPathXmlApplicationContext springContext = null;
        AnnotationConfigApplicationContext annotationContext;
        if (bootstrapContext == null && contextProperties != null && contextProperties.getUseBootstrapContext()) {
            bootstrapContext = createBootstrapContext();
        }

        if (contextProperties != null) {
            List<String> scanBasePackages = contextProperties.getScanBasePackages();
            // Add this package to ensure that beans defined there are added to the context. This includes a
            // PropertySourcesPlaceholderConfigurer with the highest (last) order that can optionally ensure that all
            // property placeholders are resolved
            if (!scanBasePackages.contains("datawave.configuration.spring")) {
                scanBasePackages.add("datawave.configuration.spring");
            }
            String[] scanBasePackagesArray = scanBasePackages.toArray(new String[scanBasePackages.size()]);
            annotationContext = new AnnotationConfigApplicationContext();
            if (bootstrapContext != null) {
                annotationContext.setParent(bootstrapContext);
            }
            annotationContext.scan(scanBasePackagesArray);
            log.debug("scanning packages: " + scanBasePackages);
            annotationContext.refresh();

            List<String> configFiles = new ArrayList<>();
            configFiles.addAll(contextProperties.getSources());
            if (contextProperties.getIgnoreUnresolvablePlaceholders()) {
                configFiles.add("classpath*:datawave/configuration/spring/UseSpringProperties.xml");
            } else {
                configFiles.add("classpath*:datawave/configuration/spring/EnforceSpringProperties.xml");
            }

            String[] configLocations = configFiles.toArray(new String[configFiles.size()]);
            TrackingClasspathXmlApplicationContext trackingContext = new TrackingClasspathXmlApplicationContext(configLocations, false);
            trackingContext.setEnvironment(annotationContext.getEnvironment());
            trackingContext.setParent(annotationContext);
            trackingContext.refresh();
            springContext = trackingContext;

            if (containsBeanForClass(springContext, SpringContextReport.class)) {
                SpringContextReport springContextReport = springContext.getBean(SpringContextReport.class);
                springContextReport.setConfiguredXmlSources(contextProperties.getSources());
                springContextReport.setScanBasePackages(contextProperties.getScanBasePackages());
                springContextReport.setPropertySources(getPropertySources(springContext));
                springContextReport.setApplicationName(springContext.getEnvironment().getProperty("spring.application.name"));
                springContextReport.setBeanNames(Arrays.asList(BeanFactoryUtils.beanNamesIncludingAncestors(springContext)));
                springContextReport.setActiveProfiles(Arrays.asList(springContext.getEnvironment().getActiveProfiles()));
                List<Resource> resources = getContextFiles(springContext, configFiles);
                List<String> xmlSources = new ArrayList<>(trackingContext.getTrackingImportReaderEventListener().getImports());
                springContextReport.setLoadedXmlSources(xmlSources);
                Path debugPath = createContextDebugPath(getContextDebugPath(springContext));
                if (debugPath != null) {
                    writeContextFiles(springContext, debugPath, resources);
                    writeContextReport(springContextReport, debugPath);
                }
            }
        }
        return springContext;
    }

    /**
     * Create a ClassPathXmlApplicationContext with optional parent contexts (AnnotationConfigApplicationContext and ConfigurableApplicationContext) through a
     * variety of methods.
     *
     * 1) Create an AnnotationConfigApplicationContext from package datawave.configuration.spring. This will create a SpringContextProperties bean if the
     * property datawave.configuration.spring.configure-from-properties=true in all sources in bootstrap.
     *
     * 2) If SpringContextProperties was not created by the (first) AnnotationConfigApplicationContext, then the method will create a
     * ClasspathXMLApplicationContext with the file found at System.getProperty("cdi.bean.context", "beanRefContext.xml") which may contain a
     * SpringContextProperties bean.
     *
     * 3) If this object is found in either place, then it will be used to create a ClasspathXMLApplicationContext with optional parent
     * AnnotationConfigApplicationContext and bootstrap context (ConfigurableApplicationContext)
     *
     * 4) If this object is not found in either place, then the ClasspathXMLApplicationContext create din step 2 will be returned.
     *
     * @return the ClassPathXmlApplicationContext
     */
    public static ClassPathXmlApplicationContext createApplicationContext() {
        ClassPathXmlApplicationContext springContext = null;
        Boolean useBootstrapContext = Boolean.parseBoolean(System.getProperty("datawave.configuration.spring.useBootstrapContext", "true"));
        ConfigurableApplicationContext bootstrapContext = null;
        if (useBootstrapContext.equals(Boolean.TRUE)) {
            bootstrapContext = createBootstrapContext();
        }

        AnnotationConfigApplicationContext annotationContext = new AnnotationConfigApplicationContext();
        annotationContext.setParent(bootstrapContext);
        annotationContext.scan("datawave.configuration.spring");
        annotationContext.refresh();

        SpringContextProperties contextConfiguration = null;
        if (containsBeanForClass(annotationContext, SpringContextProperties.class)) {
            contextConfiguration = annotationContext.getBean(SpringContextProperties.class);
        } else {
            String configLocation = System.getProperty("cdi.bean.context", "beanRefContext.xml");
            springContext = new ClassPathXmlApplicationContext("classpath*:" + configLocation);
            if (containsBeanForClass(springContext, SpringContextProperties.class)) {
                contextConfiguration = springContext.getBean(SpringContextProperties.class);
            } else {
                // If SpringContextProperties is not found, then the context returned is the one found in this file that will not
                // have any reference to the bootstrap or annotation contexts or their properties and beans. This is to provide backward
                // compatibility.
                springContext = springContext.getBean(ClassPathXmlApplicationContext.class);
            }
        }
        if (contextConfiguration != null) {
            springContext = createApplicationContext(contextConfiguration, bootstrapContext);
        }
        return springContext;
    }

    @SuppressWarnings("unused")
    void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
        log.trace("afterBeanDiscovery({},{})", abd, bm);

        // Initialize the bean provider that is used by InjectCDIBeanPostProcessor. Do this before we create
        // the application context so that InjectCDIBeanPostProcessor can do its work when singleton beans
        // are created as part of context initialization.
        BeanProvider.initializeBeanProvider(bm);

        springContext = createApplicationContext();

        synchronized (springBeans) {
            log.trace("Setting application context on all SpringCDIBean instances.");
            for (SpringCDIBean sb : springBeans.values()) {
                sb.setApplicationContext(springContext, springContextLock);
                abd.addBean(sb);
            }
        }

        AnnotatedType<ThreadSafeClassPathXmlApplicationContext> at = bm.createAnnotatedType(ThreadSafeClassPathXmlApplicationContext.class);
        final InjectionTarget<ThreadSafeClassPathXmlApplicationContext> it = bm.createInjectionTarget(at);
        abd.addBean(new Bean<ThreadSafeClassPathXmlApplicationContext>() {
            @Override
            public Class<?> getBeanClass() {
                return ThreadSafeClassPathXmlApplicationContext.class;
            }

            @Override
            public Set<InjectionPoint> getInjectionPoints() {
                return it.getInjectionPoints();
            }

            @Override
            public boolean isNullable() {
                return false;
            }

            @Override
            public Set<Type> getTypes() {
                return Sets.newHashSet(ApplicationContext.class, ConfigurableApplicationContext.class, Object.class);
            }

            @Override
            public Set<Annotation> getQualifiers() {
                return Sets.newHashSet(new AnnotationLiteral<Default>() {}, new AnnotationLiteral<Any>() {});
            }

            @Override
            public Class<? extends Annotation> getScope() {
                return Dependent.class;
            }

            @Override
            public String getName() {
                return "classPathXmlApplicationContext";
            }

            @Override
            public Set<Class<? extends Annotation>> getStereotypes() {
                return Collections.emptySet();
            }

            @Override
            public boolean isAlternative() {
                return false;
            }

            @Override
            public ThreadSafeClassPathXmlApplicationContext create(CreationalContext<ThreadSafeClassPathXmlApplicationContext> creationalContext) {
                ThreadSafeClassPathXmlApplicationContext instance = new ThreadSafeClassPathXmlApplicationContext(springContext, springContextLock);
                it.inject(instance, creationalContext);
                it.postConstruct(instance);
                return instance;
            }

            @Override
            public void destroy(ThreadSafeClassPathXmlApplicationContext instance,
                            CreationalContext<ThreadSafeClassPathXmlApplicationContext> creationalContext) {
                it.preDestroy(instance);
                creationalContext.release();
            }
        });
    }

    @SuppressWarnings("unused")
    void onRefresh(@Observes ConfigurationEvent event, BeanManager bm) {
        if (springContext != null) {
            log.debug("Refreshing Spring application context.");
            try {
                springContextLock.writeLock().lock();
                springContext.refresh();
            } finally {
                springContextLock.writeLock().unlock();
            }
        }
    }

    static class SpringCDIBean implements Bean<Object> {
        private ApplicationContext applicationContext;
        private ReadWriteLock applicationContextLock;
        private SpringBean annotation;
        private Type targetType;
        private Class<?> rawType;
        private InjectionTarget<Object> injectionTarget;
        private String name;
        private static ConcurrentHashMap<Type,AtomicLong> nameMap = new ConcurrentHashMap<>();

        public SpringCDIBean(SpringBean sb, Type targetType, BeanManager beanManager) {
            this.annotation = sb;
            this.targetType = targetType;
            this.name = sb.name();
            if ("".equals(name.trim())) {
                name = generateName();
            }

            AnnotatedType<Object> at = beanManager.createAnnotatedType(Object.class);
            injectionTarget = beanManager.createInjectionTarget(at);

            if (targetType instanceof ParameterizedType) {
                rawType = (Class<?>) ((ParameterizedType) targetType).getRawType();
            } else {
                rawType = (Class<?>) targetType;
            }
        }

        public void setApplicationContext(ApplicationContext applicationContext, ReadWriteLock applicationContextLock) {
            this.applicationContext = applicationContext;
            this.applicationContextLock = applicationContextLock;
        }

        @Override
        public Class<?> getBeanClass() {
            return rawType;
        }

        @Override
        public Set<InjectionPoint> getInjectionPoints() {
            return injectionTarget.getInjectionPoints();
        }

        @Override
        public boolean isNullable() {
            return !annotation.required();
        }

        @Override
        public Set<Type> getTypes() {
            return Sets.newHashSet(targetType, Object.class);
        }

        @Override
        public Set<Annotation> getQualifiers() {
            return Sets.newHashSet((Annotation) annotation);
        }

        @Override
        public Class<? extends Annotation> getScope() {
            return Dependent.class;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Set<Class<? extends Annotation>> getStereotypes() {
            return Collections.emptySet();
        }

        @Override
        public boolean isAlternative() {
            return false;
        }

        @Override
        public Object create(CreationalContext<Object> creationalContext) {
            if (applicationContext == null) {
                throw new IllegalStateException("No ApplicationContext was available!");
            }

            Object instance = null;
            try {
                applicationContextLock.readLock().lock();

                // Only try to get the instance if the annotation is required or we think one exists if it's not required
                if ("".equals(annotation.name().trim())) {
                    if (annotation.required() || containsBeanForClass(applicationContext, rawType)) {
                        instance = BeanFactoryUtils.beanOfTypeIncludingAncestors(applicationContext, rawType);
                    }
                } else {
                    if (annotation.required() || containsBeanName(applicationContext, annotation.name())) {
                        instance = applicationContext.getBean(annotation.name(), rawType);
                    }
                }
            } finally {
                applicationContextLock.readLock().unlock();
            }
            creationalContext.push(instance);
            injectionTarget.inject(instance, creationalContext);
            return instance;
        }

        @Override
        public void destroy(Object instance, CreationalContext<Object> creationalContext) {
            creationalContext.release();
        }

        protected String generateName() {
            AtomicLong counter = nameMap.putIfAbsent(targetType, new AtomicLong(0L));
            if (counter == null)
                counter = nameMap.get(targetType);
            return targetType + "#" + counter.getAndIncrement();
        }
    }

    static class RefreshableSpringCDIBean extends SpringCDIBean implements PassivationCapable {

        public RefreshableSpringCDIBean(SpringBean sb, Type targetType, BeanManager beanManager) {
            super(sb, targetType, beanManager);
        }

        @Override
        public Class<? extends Annotation> getScope() {
            return RefreshableScope.class;
        }

        @Override
        public String getId() {
            String id = getName();
            if (id.indexOf('#') < 0) {
                id = generateName();
            }
            return id;
        }
    }
}
