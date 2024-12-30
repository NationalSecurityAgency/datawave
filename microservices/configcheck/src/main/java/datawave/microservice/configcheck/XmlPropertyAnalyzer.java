package datawave.microservice.configcheck;

import static com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.SPLIT_LINES;
import static com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.WRITE_DOC_START_MARKER;
import static datawave.microservice.configcheck.util.XmlRenderUtils.valueToObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * XmlPropertyAnalyzer is used to read an xml file and associated properties/yaml in order to produce a report of all the placeholders, their associated
 * properties, and the values of those properties that are found in the xml file.
 */
public class XmlPropertyAnalyzer {
    private static Logger log = LoggerFactory.getLogger(XmlPropertyAnalyzer.class);
    private static final String IGNORED_KEY = "IGNORED_KEY";
    private static final String DOC = "doc";
    private static final String BEANS = "beans";
    private static final String BEAN = "bean";
    private static final String PROPERTY = "property";
    private static final String VALUE = "value";
    private static final String REF = "ref";
    private static final String CONSTRUCTOR_ARG = "constructor-arg";
    private static final String UTIL_MAP = "util:map";
    private static final String MAP = "map";
    private static final String ENTRY = "entry";
    private static final String UTIL_LIST = "util:list";
    private static final String LIST = "list";
    private static final String UTIL_SET = "util:set";
    private static final String SET = "set";
    private static final String LOOKUP_METHOD = "lookup-method";
    private static final String NULL = "null";
    private static final String ID = "id";
    private static final String KEY = "key";
    private static final String NAME = "name";
    private static final String TEXT = "#text";
    private static final String COMMENT = "#comment";
    private static final String CONTEXT_PROPERTY_PLACEHOLDER = "context:property-placeholder";
    
    private static final String PLACEHOLDER_PREFIX = "${";
    private static final String PLACEHOLDER_SUFFIX = "}";
    
    private static final String KEY_COMPONENT_SEPARATOR = ".";
    
    public static final String PLACEHOLDERS_HEADER = "Placeholders (key: ${placeholder})\n----------------------------------------\n";
    public static final String VALUES_HEADER = "Values (key: value)\n----------------------------------------\n";
    public static final String REFS_HEADER = "Refs (key: ref)\n----------------------------------------\n";
    public static final String PROPERTIES_HEADER = "Effective Properties (name=value)\n----------------------------------------\n";
    public static final String YML_HEADER = "Effective Yml\n----------------------------------------\n";
    
    private String xmlContent;
    private Properties properties;
    private Map<String,String> propertyPlaceholderByKey = new LinkedHashMap<>();
    private Map<String,Object> propertyValueByKey = new LinkedHashMap<>();
    private Map<String,String> propertyRefByKey = new LinkedHashMap<>();
    
    public XmlPropertyAnalyzer(String xmlContent, Properties properties) {
        this.xmlContent = xmlContent;
        this.properties = properties;
        analyzeProperties();
    }
    
    private void analyzeProperties() {
        try {
            // find all of the placeholders and values in the original xml, and figure out what their key is
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(xmlContent.getBytes(StandardCharsets.UTF_8)));
            doc.normalize();
            
            LinkedList<Node> nodeStack = new LinkedList<>();
            LinkedList<Node> parentStack = new LinkedList<>();
            LinkedList<String> keyComponents = new LinkedList<>();
            
            // the document node is the root
            parentStack.push(doc);
            keyComponents.add(DOC);
            
            // add the initial nodes to the stack
            NodeList nodeList = doc.getChildNodes();
            for (int i = nodeList.getLength() - 1; i >= 0; i--) {
                nodeStack.push(nodeList.item(i));
            }
            
            // work through the xml document until all nodes have been processed
            while (!nodeStack.isEmpty()) {
                Node node = nodeStack.pop();
                String nodeName = node.getNodeName();
                
                // perform upkeep on the parent stack and key components
                while (node.getParentNode() != parentStack.peek()) {
                    parentStack.pop();
                    keyComponents.removeLast();
                }
                
                if (nodeName.equals(BEANS)) {
                    addChildren(node, BEANS, nodeStack, parentStack, keyComponents);
                } else if (nodeName.equals(BEAN)) {
                    addChildren(node, getBeanId(node), nodeStack, parentStack, keyComponents);
                } else if (nodeName.equals(PROPERTY)) {
                    String propertyName = getPropertyName(node);
                    if (node.hasChildNodes()) {
                        addChildren(node, propertyName, nodeStack, parentStack, keyComponents);
                    } else if (node.hasAttributes()) {
                        String key = createKey(keyComponents, propertyName);
                        if (node.getAttributes().getNamedItem(VALUE) != null) {
                            String value = getPropertyValue(node);
                            if (value.startsWith(PLACEHOLDER_PREFIX)) {
                                propertyPlaceholderByKey.put(key, value);
                                propertyValueByKey.put(key, properties.get(value.substring(2, value.length() - 1)));
                            } else {
                                propertyValueByKey.put(key, valueToObject(value));
                            }
                        } else if (node.getAttributes().getNamedItem(REF) != null) {
                            propertyRefByKey.put(key, getPropertyRef(node));
                        }
                    }
                } else if (nodeName.equals(CONSTRUCTOR_ARG)) {
                    addChildren(node, CONSTRUCTOR_ARG, nodeStack, parentStack, keyComponents);
                } else if (nodeName.equals(UTIL_MAP) || nodeName.equals(MAP)) {
                    addChildren(node, IGNORED_KEY, nodeStack, parentStack, keyComponents);
                } else if (nodeName.equals(ENTRY)) {
                    addChildren(node, getEntryKey(node), nodeStack, parentStack, keyComponents);
                } else if (nodeName.equals(VALUE)) {
                    String key = createKey(keyComponents);
                    Object value = node.getTextContent();
                    String placeholder = null;
                    
                    if (((String) value).startsWith(PLACEHOLDER_PREFIX)) {
                        placeholder = (String) value;
                        value = properties.getProperty(placeholder.substring(2, placeholder.length() - 1));
                    }
                    
                    value = valueToObject(value);
                    
                    String indexedKey = null;
                    if (keyComponents.getLast().equals(CONSTRUCTOR_ARG)) {
                        int index = 0;
                        do {
                            indexedKey = key + "[" + index + "]";
                            index++;
                        } while (propertyPlaceholderByKey.containsKey(indexedKey) || propertyValueByKey.containsKey(indexedKey));
                    }
                    
                    key = indexedKey != null ? indexedKey : key;
                    if (placeholder != null) {
                        propertyPlaceholderByKey.put(key, placeholder);
                    }
                    propertyValueByKey.put(key, value);
                } else if (nodeName.equals(UTIL_LIST) || nodeName.equals(LIST) || nodeName.equals(UTIL_SET) || nodeName.equals(SET)) {
                    addChildren(node, getBeanId(node), nodeStack, parentStack, keyComponents);
                } else if (nodeName.equals(NULL)) {
                    propertyValueByKey.put(createKey(keyComponents), null);
                } else if (nodeName.equals(LOOKUP_METHOD) || nodeName.equals(TEXT) || nodeName.equals(COMMENT)
                                || nodeName.equals(CONTEXT_PROPERTY_PLACEHOLDER)) {
                    // do nothing
                } else {
                    log.warn("Ignoring unknown node name: {}", nodeName);
                }
            }
        } catch (Exception e) {
            log.error("Encountered exception while analyzing xml", e);
        }
    }
    
    private void addChildren(Node node, String keyComponent, LinkedList<Node> nodeStack, LinkedList<Node> parentStack, LinkedList<String> keyComponents) {
        if (node.hasChildNodes()) {
            // add the children to the stack
            NodeList children = node.getChildNodes();
            for (int i = children.getLength() - 1; i >= 0; i--) {
                nodeStack.push(children.item(i));
            }
            
            // add the parent node info
            parentStack.push(node);
            keyComponents.add(keyComponent);
        }
    }
    
    private String getBeanId(Node node) {
        return getAttributeByName(node, ID);
    }
    
    private String getEntryKey(Node node) {
        return getAttributeByName(node, KEY);
    }
    
    private String getPropertyName(Node node) {
        return getAttributeByName(node, NAME);
    }
    
    private String getPropertyValue(Node node) {
        return getAttributeByName(node, VALUE);
    }
    
    private String getPropertyRef(Node node) {
        return getAttributeByName(node, REF);
    }
    
    private String getAttributeByName(Node node, String name) {
        String beanId = IGNORED_KEY;
        if (node.hasAttributes()) {
            NamedNodeMap attributes = node.getAttributes();
            Node idAttribute = attributes.getNamedItem(name);
            if (idAttribute != null) {
                beanId = idAttribute.getNodeValue();
            }
        }
        return beanId;
    }
    
    private String createKey(List<String> keyComponents) {
        return createKey(keyComponents, null);
    }
    
    private String createKey(List<String> keyComponents, String suffix) {
        StringBuilder key = new StringBuilder();
        for (String keyComponent : keyComponents) {
            if (!keyComponent.equals(IGNORED_KEY) && !keyComponent.equals(CONSTRUCTOR_ARG)) {
                if (key.length() > 0) {
                    key.append(KEY_COMPONENT_SEPARATOR);
                }
                key.append(keyComponent);
            }
        }
        if (suffix != null) {
            key.append(KEY_COMPONENT_SEPARATOR).append(suffix);
        }
        return key.toString();
    }
    
    public String getKeyedValues() {
        StringBuilder sb = new StringBuilder();
        // @formatter:off
        propertyValueByKey.keySet().stream()
                .sorted()
                .forEach(key -> {
                    Object value = valueToObject(propertyValueByKey.get(key));
                    if (value instanceof String) {
                        value = "\"" + value + "\"";
                    }
                    sb.append(key).append(": ").append(value).append("\n");
                });
        // @formatter:on
        return sb.toString();
    }
    
    public String getSimpleReport() {
        StringBuilder sb = new StringBuilder();
        
        sb.append(VALUES_HEADER);
        sb.append(getKeyedValues());
        sb.append("\n");
        
        return sb.toString().trim();
    }
    
    public String getFullReport() {
        StringBuilder sb = new StringBuilder();
        
        sb.append(PLACEHOLDERS_HEADER);
        // @formatter:off
        propertyPlaceholderByKey.keySet().stream()
                .sorted()
                .forEach(key -> sb.append(key).append(": ").append(propertyPlaceholderByKey.get(key)).append("\n"));
        // @formatter:on
        sb.append("\n");
        
        sb.append(VALUES_HEADER);
        sb.append(getKeyedValues());
        sb.append("\n");
        
        sb.append(REFS_HEADER);
        // @formatter:off
        propertyRefByKey.keySet().stream()
                .sorted()
                .forEach(key -> sb.append(key).append(": ").append(propertyRefByKey.get(key)).append("\n"));
        // @formatter:on
        sb.append("\n");
        
        // Note: We could just add all of the properties to a single properties object,
        // but if we do that, they will be printed in a random order, so we add one at a time
        sb.append(PROPERTIES_HEADER);
        sb.append(createEffectiveProperties());
        sb.append("\n");
        
        sb.append(YML_HEADER);
        sb.append(createEffectiveYaml());
        sb.append("\n");
        
        return sb.toString().trim();
    }
    
    private String createEffectiveProperties() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Properties properties = new Properties();
        for (String key : propertyValueByKey.keySet()) {
            Object value = propertyValueByKey.get(key);
            key = propertyPlaceholderByKey.get(key);
            if (key != null) {
                properties.clear();
                key = key.substring(2, key.length() - 1);
                properties.setProperty(key, String.valueOf(value));
                try {
                    properties.store(baos, null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return baos.toString(StandardCharsets.UTF_8).replaceAll("#.*\n", "");
    }
    
    private String createEffectiveYaml() {
        Map<String,Object> ymlMap = new LinkedHashMap<>();
        Set<String> propKeys = propertyPlaceholderByKey.values().stream().map(x -> x.substring(2, x.length() - 1)).collect(Collectors.toSet());
        for (String key : propertyValueByKey.keySet()) {
            Object value = propertyValueByKey.get(key);
            key = propertyPlaceholderByKey.get(key);
            if (key != null) {
                key = key.substring(2, key.length() - 1);
                Map<String,Object> curMap = ymlMap;
                String[] keyParts = key.split("\\.");
                for (int i = 0; i < keyParts.length; i++) {
                    final String partialKey = createPartialKey(keyParts, 0, i + 1);
                    if (i == keyParts.length - 1) {
                        curMap.put(keyParts[i], value);
                    }
                    // if this partial key is set to a value in the properties, then we stop here and set the value using the remaining terms as the key
                    else if (propKeys.stream().anyMatch(x -> x.equals(partialKey))) {
                        String finalKey = "[" + createPartialKey(keyParts, i, keyParts.length) + "]";
                        curMap.put(finalKey, value);
                    } else {
                        curMap = (LinkedHashMap<String,Object>) curMap.computeIfAbsent(keyParts[i], (k) -> new LinkedHashMap<String,Object>());
                    }
                }
            }
        }
        
        String yml = null;
        try {
            YAMLFactory yamlFactory = new YAMLFactory();
            yamlFactory.configure(WRITE_DOC_START_MARKER, false);
            yamlFactory.configure(SPLIT_LINES, false);
            yml = new ObjectMapper(yamlFactory).writeValueAsString(ymlMap);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return yml;
    }
    
    private String createPartialKey(String[] keyComponents, int start, int stop) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < stop; i++) {
            if (i > start) {
                sb.append(".");
            }
            sb.append(keyComponents[i]);
        }
        return sb.toString();
    }
    
    public Map<String,String> getPropertyPlaceholderByKey() {
        return propertyPlaceholderByKey;
    }
    
    public Map<String,Object> getPropertyValueByKey() {
        return propertyValueByKey;
    }
    
    public Map<String,String> getPropertyRefByKey() {
        return propertyRefByKey;
    }
}
