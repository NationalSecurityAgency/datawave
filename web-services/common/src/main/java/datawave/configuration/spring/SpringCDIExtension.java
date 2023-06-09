package datawave.configuration.spring;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

import com.google.common.collect.Sets;
import datawave.configuration.ConfigurationEvent;
import datawave.configuration.RefreshableScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * A portable CDI extension that exposes Spring Beans as CDI beans. This extension expects to find a beanRefContext.xml file on the classpath. This file should
 * define a single {@link ClassPathXmlApplicationContext} which then contains the beans that will be exposed as CDI beans. The single
 * {@link ClassPathXmlApplicationContext} can be injected as a standard CDI bean. Any beans provided by Spring should be qualified with {@link SpringBean}.
 */
public class SpringCDIExtension implements Extension {
    private Logger log = LoggerFactory.getLogger(getClass());
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

    @SuppressWarnings("unused")
    void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
        log.trace("afterBeanDiscovery({},{})", abd, bm);

        // Initialize the bean provider that is used by InjectCDIBeanPostProcessor. Do this before we create
        // the application context so that InjectCDIBeanPostProcessor can do its work when singleton beans
        // are created as part of context initialization.
        BeanProvider.initializeBeanProvider(bm);

        String cdiSpringConfigs = System.getProperty("cdi.spring.configs");
        if (cdiSpringConfigs != null) {
            springContext = new ClassPathXmlApplicationContext(cdiSpringConfigs.split(","));
        } else {
            String beanRefContext = System.getProperty("cdi.bean.context", "beanRefContext.xml");
            ClassPathXmlApplicationContext bootstrap = new ClassPathXmlApplicationContext("classpath*:" + beanRefContext);
            springContext = bootstrap.getBean(ClassPathXmlApplicationContext.class);
        }
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
                    if (annotation.required() || applicationContext.getBeanNamesForType(rawType).length > 0)
                        instance = applicationContext.getBean(rawType);
                } else {
                    if (annotation.required() || applicationContext.containsBean(annotation.name()))
                        instance = applicationContext.getBean(annotation.name(), rawType);
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
