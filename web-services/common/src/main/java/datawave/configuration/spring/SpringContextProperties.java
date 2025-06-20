package datawave.configuration.spring;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.configuration.spring")
public class SpringContextProperties {
    private boolean useBootstrapContext = true;
    private boolean ignoreUnresolvablePlaceholders = false;
    private List<String> scanBasePackages = new ArrayList<>();
    private List<String> sources = new ArrayList<>();

    public void setUseBootstrapContext(boolean useBootstrapContext) {
        this.useBootstrapContext = useBootstrapContext;
    }

    public boolean getUseBootstrapContext() {
        return useBootstrapContext;
    }

    public void setIgnoreUnresolvablePlaceholders(boolean ignoreUnresolvablePlaceholders) {
        this.ignoreUnresolvablePlaceholders = ignoreUnresolvablePlaceholders;
    }

    public boolean getIgnoreUnresolvablePlaceholders() {
        return ignoreUnresolvablePlaceholders;
    }

    public void setScanBasePackages(List<String> scanBasePackages) {
        this.scanBasePackages.clear();
        this.scanBasePackages.addAll(scanBasePackages);
    }

    public List<String> getScanBasePackages() {
        return scanBasePackages;
    }

    public void setSources(List<String> sources) {
        this.sources.clear();
        this.sources.addAll(sources);
    }

    public List<String> getSources() {
        return sources;
    }

    @Override
    public String toString() {
        // @formatter:off
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                        .append("useBootstrapContext", useBootstrapContext)
                        .append("scanBasePackages", scanBasePackages)
                        .append("sources", sources).toString();
        // @formatter:on
    }
}
