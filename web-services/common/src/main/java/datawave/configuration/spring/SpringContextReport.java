package datawave.configuration.spring;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.boot.origin.Origin;
import org.springframework.boot.origin.OriginLookup;
import org.springframework.boot.origin.TextResourceOrigin;
import org.springframework.cloud.bootstrap.config.BootstrapPropertySource;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.env.CompositePropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;

public class SpringContextReport implements ApplicationContextAware {
    private ApplicationContext applicationContext;

    public String getApplicationName() {
        return applicationContext.getEnvironment().getProperty("spring.application.name");
    }

    public List<String> getActiveProfiles() {
        return Arrays.asList(applicationContext.getEnvironment().getActiveProfiles());
    }

    /*
     * Return sources in reverse order so that the lowest priority property sources are listed first i.e. the order in which they are applied to the
     * environment. e.g. bootstrap.yml, application.yml wildfly.yml, application-{profile1} wildfly-{profile1} application-{profile2} wildfly-{profile2}
     */
    @SuppressWarnings("unchecked")
    public List<EnumerablePropertySource<Map<String,Object>>> getPropertySources() {
        List<EnumerablePropertySource<Map<String,Object>>> sourceList = new ArrayList<>();
        // This report is intended to show the property sources that are created via YML property files
        List<String> skipPropertySourceNames = new ArrayList<>(Arrays.asList("systemProperties"));
        if (applicationContext instanceof AbstractApplicationContext) {
            AbstractApplicationContext context = (AbstractApplicationContext) applicationContext;
            context.getEnvironment().getPropertySources().forEach(ps -> {
                if (ps instanceof BootstrapPropertySource || ps instanceof OriginTrackedMapPropertySource || ps instanceof PropertiesPropertySource) {
                    if (!skipPropertySourceNames.contains(ps.getName())) {
                        sourceList.add((EnumerablePropertySource<Map<String,Object>>) ps);
                    }
                }
            });
            Collections.reverse(sourceList);
        }
        return sourceList;
    }

    public String getPropertySourceNames() {
        return getPropertySources().stream().map(ps -> ps.getClass().getSimpleName() + ":" + ps.getName()).collect(Collectors.joining("\n"));
    }

    /**
     * Report properties enumerated by the configured property sources in alphabetical order. Each value has the form {@code resolvedValue -> sourceName}, using
     * the environment's highest-precedence source, including overrides outside desired property source list.
     *
     * @return property names mapped to their resolved values and winning source names
     */
    public Map<String,String> propertyReport() {
        ConfigurableEnvironment environment = (ConfigurableEnvironment) applicationContext.getEnvironment();
        Map<String,String> report = new TreeMap<>();
        for (EnumerablePropertySource<?> source : getPropertySources()) {
            for (String name : source.getPropertyNames()) {
                if (!report.containsKey(name)) {
                    PropertySource<?> authoritativeSource = authoritativePropertySource(environment.getPropertySources(), name);
                    if (authoritativeSource != null) {
                        report.put(name, environment.getProperty(name) + " ( " + sourceDisplayName(authoritativeSource, name) + " )");
                    }
                }
            }
        }
        return report;
    }

    /**
     * Return the property source that is authoritative for the given property name. This is the highest-precedence source that contains a non-null value for
     * the requested property name. If the property is not found in any source, return null.
     */
    private static PropertySource<?> authoritativePropertySource(Iterable<PropertySource<?>> sources, String name) {
        for (PropertySource<?> source : sources) {
            // Boot's attached adapter duplicates the underlying sources without identifying the winning source.
            if (ConfigurationPropertySources.isAttachedConfigurationPropertySource(source)) {
                continue;
            }
            if (source instanceof CompositePropertySource) {
                PropertySource<?> winner = authoritativePropertySource(((CompositePropertySource) source).getPropertySources(), name);
                if (winner != null) {
                    return winner;
                }
            } else if (source.getProperty(name) != null) {
                return source;
            }
        }
        return null;
    }

    private static String sourceDisplayName(PropertySource<?> source, String name) {
        Origin origin = OriginLookup.getOrigin(source, name);
        if (origin instanceof TextResourceOrigin) {
            return origin.toString();
        } else {
            String sourceName = source.getName();
            if (sourceName.startsWith("bootstrapProperties-")) {
                sourceName = sourceName.substring("bootstrapProperties-".length());
            }
            return sourceName;
        }
    }

    public List<String> getLoadedXmlSources() {
        if (applicationContext instanceof TrackingClasspathXmlApplicationContext) {
            TrackingClasspathXmlApplicationContext trackingContext = (TrackingClasspathXmlApplicationContext) applicationContext;
            return new ArrayList<>(trackingContext.getTrackingImportReaderEventListener().getImports());
        } else {
            return new ArrayList<>();
        }
    }

    public List<String> getBeanNames() {
        return Arrays.stream(BeanFactoryUtils.beanNamesIncludingAncestors(applicationContext)).sorted().collect(Collectors.toList());
    }

    @Override
    public String toString() {
        // @formatter:off
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                        .append("applicationName", getApplicationName())
                        .append("activeProfiles", getActiveProfiles())
                        .append("propertySources", getPropertySourceNames())
                        .append("loadedXmlSources", getLoadedXmlSources())
                        .append("beanNames", getBeanNames()).toString();
        // @formatter:on
    }

    public String getReport(SpringContextProperties contextProperties) {
        StringBuilder sb = new StringBuilder();
        sb.append("applicationName: ").append(getApplicationName()).append("\n");
        sb.append("activeProfiles: ").append(getActiveProfiles()).append("\n\n");
        sb.append("configured xmlSources :").append("\n");
        sb.append(StringUtils.join(contextProperties.getSources(), "\n")).append("\n\n");
        sb.append("loaded xmlSources:").append("\n");
        sb.append(StringUtils.join(getLoadedXmlSources(), "\n")).append("\n\n");
        sb.append("propertySources:").append("\n");
        sb.append(getPropertySourceNames()).append("\n\n");
        sb.append("scanBasePackages:").append("\n");
        sb.append(StringUtils.join(contextProperties.getScanBasePackages(), "\n")).append("\n\n");
        sb.append("beanNames:").append("\n");
        sb.append(StringUtils.join(getBeanNames(), "\n")).append("\n");
        sb.append("property names, values, sources:").append("\n");
        propertyReport().forEach((name, value) -> sb.append(name).append("=").append(value).append("\n"));
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
