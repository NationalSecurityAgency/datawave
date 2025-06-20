package datawave.configuration.spring;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SpringContextReport {
    private String applicationName;
    private List<String> activeProfiles;
    private List<String> scanBasePackages;
    private List<String> propertySources;
    private List<String> configuredXmlSources;
    private List<String> loadedXmlSources;
    private List<String> beanNames;

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setActiveProfiles(List<String> activeProfiles) {
        this.activeProfiles = activeProfiles;
    }

    public List<String> getActiveProfiles() {
        return activeProfiles;
    }

    public void setScanBasePackages(List<String> scanBasePackages) {
        this.scanBasePackages = scanBasePackages;
    }

    public List<String> getScanBasePackages() {
        return scanBasePackages;
    }

    public void setPropertySources(List<String> propertySources) {
        this.propertySources = propertySources;
    }

    public List<String> getPropertySources() {
        return propertySources;
    }

    public void setConfiguredXmlSources(List<String> configuredXmlSources) {
        this.configuredXmlSources = configuredXmlSources;
    }

    public List<String> getConfiguredXmlSources() {
        return configuredXmlSources;
    }

    public void setLoadedXmlSources(List<String> loadedXmlSources) {
        this.loadedXmlSources = loadedXmlSources;
    }

    public List<String> getLoadedXmlSources() {
        return loadedXmlSources;
    }

    public void setBeanNames(List<String> beanNames) {
        this.beanNames = beanNames.stream().sorted().collect(Collectors.toList());
    }

    public List<String> getBeanNames() {
        return beanNames;
    }

    @Override
    public String toString() {
        // @formatter:off
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                        .append("applicationName", applicationName)
                        .append("activeProfiles", activeProfiles)
                        .append("propertySources", propertySources)
                        .append("configuredXmlSources", configuredXmlSources)
                        .append("loadedXmlSources", loadedXmlSources)
                        .append("beanNames", beanNames).toString();
        // @formatter:on
    }

    public String getReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("aplicationName: ").append(applicationName).append("\n");
        sb.append("activeProfiles: ").append(activeProfiles).append("\n\n");
        sb.append("propertySources:").append("\n");
        sb.append(StringUtils.join(propertySources, "\n")).append("\n\n");
        sb.append("scanBasePackages:").append("\n");
        sb.append(StringUtils.join(scanBasePackages, "\n")).append("\n\n");
        sb.append("configured xmlSources :").append("\n");
        sb.append(StringUtils.join(configuredXmlSources, "\n")).append("\n\n");
        sb.append("loaded xmlSources:").append("\n");
        sb.append(StringUtils.join(loadedXmlSources, "\n")).append("\n\n");
        sb.append("beanNames:").append("\n");
        sb.append(StringUtils.join(beanNames, "\n"));
        return sb.toString();
    }
}
