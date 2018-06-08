package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.ageoff.FilterOptions;
import datawave.iterators.filter.ageoff.FilterRule;

/**
 * 
 */
public class FileRuleWatcher extends FileSystemWatcher<Collection<FilterRule>> {
    
    private static final Logger log = Logger.getLogger(FileRuleWatcher.class);
    
    /**
     * @param fs
     * @param filePath
     * @param configuredDiff
     * @throws IOException
     */
    public FileRuleWatcher(FileSystem fs, Path filePath, long configuredDiff) throws IOException {
        super(fs, filePath, configuredDiff);
    }
    
    /**
     * @param filePath
     * @param configuredDiff
     * @throws IOException
     */
    public FileRuleWatcher(Path filePath, long configuredDiff) throws IOException {
        super(filePath.getFileSystem(new Configuration()), filePath, configuredDiff);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.ingest.util.cache.watch.FileSystemWatcher#loadContents(java.io.InputStream)
     */
    @Override
    protected Collection<FilterRule> loadContents(InputStream in) throws IOException {
        
        try {
            Collection<RuleConfig> mergedRuleConfigs = loadRuleConfigs(in);
            Collection<FilterRule> filterRules = new ArrayList<>();
            /**
             * This has been changed to support extended options.
             */
            for (RuleConfig ruleConfig : mergedRuleConfigs) {
                
                try {
                    FilterRule filter = (FilterRule) Class.forName(ruleConfig.filterClassName).newInstance();
                    
                    FilterOptions option = new FilterOptions();
                    
                    if (ruleConfig.ttlValue != null) {
                        option.setTTL(Long.valueOf(ruleConfig.ttlValue));
                    }
                    if (ruleConfig.ttlUnits != null) {
                        option.setTTLUnits(ruleConfig.ttlUnits);
                    }
                    option.setOption(AgeOffConfigParams.MATCHPATTERN, ruleConfig.matchPattern);
                    
                    StringBuilder extOptions = new StringBuilder();
                    
                    for (Entry<String,String> myOption : ruleConfig.extendedOptions.entrySet()) {
                        option.setOption(myOption.getKey(), myOption.getValue());
                        extOptions.append(myOption.getKey()).append(",");
                    }
                    
                    int extOptionLen = extOptions.length();
                    
                    option.setOption(AgeOffConfigParams.EXTENDED_OPTIONS, extOptions.toString().substring(0, extOptionLen - 1));
                    
                    filter.init(option);
                    
                    filterRules.add(filter);
                    
                } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
                    log.error(e);
                    throw new IOException(e);
                }
            }
            return filterRules;
        } catch (Exception ex) {
            log.error("uh oh: " + ex);
            throw new IOException(ex);
        } finally {
            IOUtils.closeStream(in);
        }
        
    }
    
    protected Collection<RuleConfig> loadRuleConfigs(InputStream in) throws IOException {
        
        Collection<RuleConfig> ruleConfigs = new ArrayList<>();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder;
        
        Document doc;
        try {
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            docBuilder = factory.newDocumentBuilder();
            doc = docBuilder.parse(in);
            
            Element docElement = doc.getDocumentElement();
            NodeList parents = docElement.getElementsByTagName("parent");
            if (null != parents) {
                ruleConfigs.addAll(loadParentRuleConfigs(parents));
            }
            NodeList rules = docElement.getElementsByTagName("rule");
            // parse each node in rules and create a rule config
            ruleConfigs.addAll(IntStream.range(0, rules.getLength()).mapToObj(i -> getRuleConfigForNode(rules, i)).collect(Collectors.toList()));
            
            // if any rules are a merge rule, the merge them all?
            boolean shouldMerge = ruleConfigs.stream().anyMatch(c -> c.isMerge);
            if (shouldMerge) {
                // merge configs that map to the same filter class name
                return ruleConfigs.stream()
                                .collect(Collectors.toMap(RuleConfig::getFilterClassName, Function.identity(), (rule1, rule2) -> mergeRules(rule1, rule2)))
                                .values();
            } else {
                return ruleConfigs;
            }
        } catch (Exception ex) {
            log.error("uh oh: " + ex);
            throw new IOException(ex);
        } finally {
            IOUtils.closeStream(in);
        }
    }
    
    RuleConfig mergeRules(RuleConfig rule1, RuleConfig rule2) {
        // the rule that is a merge should have its values override the original rule
        if (rule1.isMerge) {
            rule2.ttlValue = rule1.ttlValue;
            rule2.ttlUnits = rule1.ttlUnits;
            rule2.extendedOptions.putAll(rule1.extendedOptions);
            rule2.matchPattern += rule1.matchPattern;
            return rule2;
        } else if (rule2.isMerge) {
            rule1.ttlValue = rule2.ttlValue;
            rule1.ttlUnits = rule2.ttlUnits;
            rule1.extendedOptions.putAll(rule2.extendedOptions);
            rule1.matchPattern += rule2.matchPattern;
            return rule1;
        }
        // neither is a merge rule, so just pick highest priority?
        if (rule1.priority > rule2.priority) {
            return rule1;
        } else {
            return rule2;
        }
    }
    
    private RuleConfig getRuleConfigForNode(NodeList rules, int index) {
        Map<String,String> extendedOptions = new HashMap<>();
        String ttlValue = null;
        String ttlUnits = null;
        String matchPattern = null;
        String filterClassName = null;
        extendedOptions.clear();
        
        Element ruleElem = (Element) rules.item(index);
        boolean isMerge = isMergeRule(ruleElem);
        
        NodeList ruleElementList = ruleElem.getChildNodes();
        for (int j = 0; j < ruleElementList.getLength(); j++) {
            Node nodeItem = ruleElementList.item(j);
            
            String nodeName = nodeItem.getNodeName();
            
            log.debug("getting " + nodeName);
            if ("filterClass".equals(nodeName)) {
                filterClassName = nodeItem.getTextContent().trim();
            } else if ("ttl".equals(nodeName)) {
                ttlValue = nodeItem.getTextContent().trim();
                ttlUnits = ((Element) nodeItem).getAttribute("units").trim();
            } else if ("ttlValue".equals(nodeName)) {
                ttlValue = nodeItem.getTextContent().trim();
            } else if ("ttlUnits".equals(nodeName)) {
                ttlUnits = nodeItem.getTextContent().trim();
            } else if ("matchPattern".equals(nodeName)) {
                matchPattern = nodeItem.getTextContent().trim();
            } else {
                /*
                 * gives us the ability to add arbitrary configuration items along with adding XML attributes as sub tags.
                 * 
                 * The sub tags are sent as name . attributename
                 */
                
                extendedOptions.put(nodeName, nodeItem.getTextContent().trim());
                if (nodeItem.hasAttributes()) {
                    NamedNodeMap attributeMap = nodeItem.getAttributes();
                    // if an xml tag exists with the attribute "name" then use the modified prefix
                    String prefix = extractNewPrefix(nodeName, attributeMap);
                    if (null == prefix) {
                        // to maintain backwards compatibility, continue to produce the legacy
                        // option name
                        prefix = nodeName;
                    }
                    for (int k = 0; k < attributeMap.getLength(); k++) {
                        Node attItem = attributeMap.item(k);
                        extendedOptions.put(prefix + "." + attItem.getNodeName(), attItem.getTextContent().trim());
                    }
                }
            }
            
        }
        extendedOptions.put(AgeOffConfigParams.IS_MERGE, Boolean.toString(isMerge));
        return new RuleConfig(filterClassName, index).ttlValue(ttlValue).ttlUnits(ttlUnits).matchPattern(matchPattern).setIsMerge(isMerge)
                        .extendedOptions(extendedOptions);
    }
    
    private Collection<? extends RuleConfig> loadParentRuleConfigs(NodeList parents) throws IOException {
        Collection<RuleConfig> rules = new ArrayList<>();
        for (int i = 0; i < parents.getLength(); i++) {
            Node parent = parents.item(i);
            String parentPathStr = parent.getTextContent();
            URL resource = this.getClass().getResource(parentPathStr);
            if (resource == null) {
                throw new IllegalArgumentException("Invalid parent config path specified, resource " + parentPathStr + " not found!");
            }
            Path parentPath = new Path(resource.toString());
            if (!fs.exists(parentPath)) {
                throw new IllegalArgumentException("Invalid parent config path specified, " + parentPathStr + " does not exist!");
            }
            rules.addAll(loadRuleConfigs(fs.open(parentPath)));
        }
        return rules;
    }
    
    /**
     * Xml tag names cannot start with a number, so it was not previously possible to include extended options for items that begin with a number. The new
     * prefix, provided by this method, provides a mechanism for doing this. Check if attribute "name" exists, e.g. &lt;field name='abc'&gt;value&lt;/field&gt;
     * 
     * @param nodeName
     *            xml tag name, e.g. field using the above example
     * @param attributeMap
     *            attributes for current node
     * @return null if the name attribute does not exist in the attribute map, or a prefix, e.g. field.abc using the above example
     */
    private String extractNewPrefix(String nodeName, NamedNodeMap attributeMap) {
        String newPrefix = null;
        Node nameAttribute = attributeMap.getNamedItem("name");
        if (null != nameAttribute) {
            newPrefix = nodeName + "." + nameAttribute.getNodeValue();
        }
        return newPrefix;
    }
    
    /**
     * does the rule specify mode="merge"
     * 
     * @param nodeItem
     *            to inspect
     * @return if mode attribute is set to merge
     */
    private boolean isMergeRule(Node nodeItem) {
        if (null != nodeItem.getAttributes()) {
            Node namedItem = nodeItem.getAttributes().getNamedItem("mode");
            if (null == namedItem) {
                return false;
            }
            return namedItem.getNodeValue().equalsIgnoreCase("merge");
        } else {
            return false;
        }
    }
    
    /**
     * Temporary holding class for rule configs to allow merges of rules;
     */
    private static class RuleConfig {
        String ttlValue = null;
        String ttlUnits = null;
        String matchPattern = null;
        String filterClassName = null;
        boolean isMerge = false;
        Map<String,String> extendedOptions = new HashMap<>();
        int priority = -1;
        
        public RuleConfig(String filterClassName, int priority) {
            this.filterClassName = filterClassName;
            this.priority = priority;
        }
        
        public String getFilterClassName() {
            return filterClassName;
        }
        
        public RuleConfig ttlValue(String ttlValue) {
            this.ttlValue = ttlValue;
            return this;
        }
        
        public RuleConfig ttlUnits(String ttlUnits) {
            this.ttlUnits = ttlUnits;
            return this;
        }
        
        public RuleConfig matchPattern(String matchPattern) {
            this.matchPattern = matchPattern;
            return this;
        }
        
        public RuleConfig setIsMerge(boolean isMerge) {
            this.isMerge = isMerge;
            return this;
        }
        
        public RuleConfig extendedOptions(Map<String,String> extendedOptions) {
            this.extendedOptions = extendedOptions;
            return this;
        }
    }
}
