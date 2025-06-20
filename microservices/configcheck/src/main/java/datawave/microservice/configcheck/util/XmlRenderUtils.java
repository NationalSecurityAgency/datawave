package datawave.microservice.configcheck.util;

import static datawave.microservice.configcheck.util.FileUtils.getFilePath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.env.PropertiesPropertySourceLoader;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySources;
import org.springframework.core.env.PropertySourcesPropertyResolver;
import org.springframework.core.io.PathResource;
import org.springframework.util.PropertyPlaceholderHelper;

/**
 * XmlRenderUtils is used to load xml content as a string from a given file and subsequently render the property placeholders in the xml file using either yaml
 * or java properties.
 */
public class XmlRenderUtils {
    private static Logger log = LoggerFactory.getLogger(XmlRenderUtils.class);

    private static final String TRUE = "true";
    private static final String FALSE = "false";

    public static String loadContent(String filePath) {
        String xmlContent = null;
        try {
            xmlContent = Files.readString(getFilePath(filePath), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("Exception reading xml file", e);
        }
        return xmlContent;
    }

    public static PropertySources loadPropertySources(String configdir, List<String> propertiesFiles) throws IOException {
        MutablePropertySources mutablePropertySources = new MutablePropertySources();
        PropertiesPropertySourceLoader loader = new PropertiesPropertySourceLoader();
        for (String propertyFile : propertiesFiles) {
            PathResource resource = new PathResource(getFilePath(configdir, propertyFile));
            List<PropertySource<?>> propertySources = loader.load(propertyFile, resource);
            for (PropertySource<?> propertySource : propertySources) {
                mutablePropertySources.addLast(propertySource);
            }
        }
        return mutablePropertySources;
    }

    public static PropertySources getYamlPropertySources(String configdir, List<String> yamlFiles) throws IOException {
        MutablePropertySources mutablePropertySources = new MutablePropertySources();
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        for (String yamlFile : yamlFiles) {
            PathResource resource = new PathResource(getFilePath(configdir, yamlFile));
            List<PropertySource<?>> propertySources = loader.load(yamlFile, resource);
            for (PropertySource<?> propertySource : propertySources) {
                mutablePropertySources.addLast(propertySource);
            }
        }
        return mutablePropertySources;
    }

    public static String renderContent(String content, PropertySources propertySources) {
        String renderedXmlContent = null;
        if (content != null) {
            PropertyPlaceholderHelper propertyPlaceholderHelper = new PropertyPlaceholderHelper("${", "}");
            PropertySourcesPropertyResolver resolver = new PropertySourcesPropertyResolver(propertySources);
            renderedXmlContent = propertyPlaceholderHelper.replacePlaceholders(content, placeholderName -> resolver.getProperty(placeholderName));
        }
        return renderedXmlContent;
    }

    public static Object valueToObject(Object value) {
        if (value instanceof String) {
            value = ((String) value).trim();
            try {
                value = Integer.parseInt((String) value);
            } catch (Exception e1) {
                try {
                    value = Double.parseDouble((String) value);
                } catch (Exception e2) {
                    // ignored exception
                    if (value.equals(TRUE) || value.equals(FALSE)) {
                        value = Boolean.parseBoolean((String) value);
                    }
                }
            }
        }
        return value;
    }
}
