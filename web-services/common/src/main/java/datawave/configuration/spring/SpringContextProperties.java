package datawave.configuration.spring;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.configuration.spring")
public class SpringContextProperties {
    private boolean useBootstrapContext = true;
    private boolean ignoreUnresolvablePlaceholders = false;
    private final List<String> scanBasePackages = new ArrayList<>();
    private final List<String> sources = new ArrayList<>();
    private String debugDir;

    public static String formattedDate() {
        // Using dashes instead of colons in the time portion of the date so that it can be used
        // as a directory name without breaking bash autocomplete and other shell commands that
        // use colons as a separator due to the COMP_WORDBREAKS environment variable.
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss.SSS"));
    }

    public static String debugDir(String parentDir, String debugDirPrefix) {
        Path debugDirPath;
        if (StringUtils.isBlank(parentDir)) {
            debugDirPath = Path.of(System.getProperty("java.io.tmpdir"), debugDirPrefix + formattedDate());
        } else {
            debugDirPath = Path.of(parentDir, debugDirPrefix + formattedDate());
        }
        return debugDirPath.toString();
    }

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

    public void setDebugDir(String debugDir) {
        this.debugDir = debugDir;
    }

    public String getDebugDir() {
        return debugDir;
    }
}
