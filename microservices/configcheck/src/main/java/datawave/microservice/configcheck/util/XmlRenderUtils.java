package datawave.microservice.configcheck.util;

import static datawave.microservice.configcheck.util.FileUtils.getFilePath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.CollectionFactory;
import org.springframework.core.io.PathResource;
import org.springframework.util.PropertyPlaceholderHelper;

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
    
    public static Properties loadProperties(String configdir, List<String> propertiesFiles) {
        Properties mergedProperties = CollectionFactory.createStringAdaptingProperties();
        try {
            for (String propertiesFile : propertiesFiles) {
                Properties props = CollectionFactory.createStringAdaptingProperties();
                props.load(Files.newBufferedReader(getFilePath(configdir, propertiesFile)));
                mergedProperties.putAll(props);
            }
        } catch (Exception e) {
            log.error("Exception reading properties file", e);
        }
        return mergedProperties;
    }
    
    public static Properties loadYamlAsProperties(String configdir, List<String> yamlFiles) {
        YamlPropertiesFactoryBean yamlPropFactory = new YamlPropertiesFactoryBean();
        yamlPropFactory.setResources(yamlFiles.stream().map(yamlFile -> new PathResource(getFilePath(configdir, yamlFile))).toArray(PathResource[]::new));
        return yamlPropFactory.getObject();
    }
    
    public static String renderContent(String content, Properties properties) {
        String renderedXmlContent = null;
        if (content != null) {
            renderedXmlContent = new PropertyPlaceholderHelper("${", "}").replacePlaceholders(content, properties);
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
