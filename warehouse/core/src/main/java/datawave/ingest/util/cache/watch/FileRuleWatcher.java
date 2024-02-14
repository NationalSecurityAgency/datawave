package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.accumulo.core.iterators.IteratorEnvironment;
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
 * File Rule Watch
 */
public class FileRuleWatcher extends FileSystemWatcher<Collection<FilterRule>> {

    private static final Logger log = Logger.getLogger(FileRuleWatcher.class);

    private final IteratorEnvironment iterEnv;

    /**
     * @param fs
     *            file system
     * @param filePath
     *            path to the file
     * @param configuredDiff
     *            configured diff
     * @throws IOException
     *             if there is a problem reading the file
     */
    public FileRuleWatcher(FileSystem fs, Path filePath, long configuredDiff) throws IOException {
        this(fs, filePath, configuredDiff, null);
    }

    /**
     * @param fs
     *            file system
     * @param filePath
     *            path to the file
     * @param configuredDiff
     *            configured diff
     * @param iterEnv
     *            iterator environment
     * @throws IOException
     *             if there is a problem reading the file
     */
    public FileRuleWatcher(FileSystem fs, Path filePath, long configuredDiff, IteratorEnvironment iterEnv) throws IOException {
        super(fs, filePath, configuredDiff);
        this.iterEnv = iterEnv;
    }

    /**
     * @param filePath
     *            path to the file
     * @param configuredDiff
     *            configured diff
     * @throws IOException
     *             if there is an error reading the file
     */
    public FileRuleWatcher(Path filePath, long configuredDiff) throws IOException {
        this(filePath, configuredDiff, null);
    }

    /**
     * @param filePath
     *            the path to the file
     * @param configuredDiff
     *            configured diff
     * @param iterEnv
     *            iterator environment
     * @throws IOException
     *             if there is an error reading the file
     */
    public FileRuleWatcher(Path filePath, long configuredDiff, IteratorEnvironment iterEnv) throws IOException {
        super(filePath.getFileSystem(new Configuration()), filePath, configuredDiff);
        this.iterEnv = iterEnv;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.util.cache.watch.FileSystemWatcher#loadContents(java.io.InputStream)
     */
    @Override
    protected Collection<FilterRule> loadContents(InputStream in) throws IOException {

        try {
            List<RuleConfig> mergedRuleConfigs = loadRuleConfigs(in);
            List<FilterRule> filterRules = new ArrayList<>();
            /**
             * This has been changed to support extended options.
             */
            for (RuleConfig ruleConfig : mergedRuleConfigs) {

                try {
                    FilterRule filter = (FilterRule) Class.forName(ruleConfig.filterClassName).getDeclaredConstructor().newInstance();

                    FilterOptions option = new FilterOptions();

                    if (ruleConfig.ttlValue != null) {
                        option.setTTL(Long.parseLong(ruleConfig.ttlValue));
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

                    filter.init(option, iterEnv);

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

    protected List<RuleConfig> loadRuleConfigs(InputStream in) throws IOException {

        List<RuleConfig> ruleConfigs = new ArrayList<>();
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
            if (null != parents && parents.getLength() > 0) {
                if (parents.getLength() > 1) {
                    throw new IllegalArgumentException("Not allowed to have more than one parent config");
                }
                ruleConfigs.addAll(loadParentRuleConfigs(parents.item(0)));
            }
            NodeList rules = docElement.getElementsByTagName("rule");
            // parse each node in rules and create a rule config
            // @formatter:off
            List<RuleConfig> childRules = IntStream.range(0, rules.getLength())
                .mapToObj(i -> getRuleConfigForNode(rules, i))
                .collect(Collectors.toList());
            // first check if there are any parent rules to merge with
            // no use trying to merge if no parent rules exist
            if (ruleConfigs.isEmpty()) {
                return childRules;
            }
            // Example merge process:
            // | parent | child | result |
            // |--------|-------|--------|
            // | A - 1  | A - 1 | A - 1m | <= type A with label 1 merged with child
            // | B      |       | B      | <= type B from parent maintains order
            // | C - 2  | C - 2 | C - 2m | <= type C with label 2 merged with child
            // |        | D - 3 | D - 3  | <= type D with label 3 not merged, nothing to merge with
            // |        | E     | E      | <= type E with no label maintains order in child
            //
            // iterate through child rules and try to merge them into the parent if possible
            List<RuleConfig> mergedRules = childRules.stream()
                .filter(r -> !r.getLabel().isEmpty())
                .filter(r -> mergeIfPossible(r, ruleConfigs))
                .collect(Collectors.toList());

            childRules.removeAll(mergedRules);
            // what ever is left add to the end of the list
            ruleConfigs.addAll(childRules);

            // @formatter:on
        } catch (Exception ex) {
            log.error("uh oh: " + ex);
            throw new IOException(ex);
        } finally {
            IOUtils.closeStream(in);
        }
        return ruleConfigs;
    }

    boolean mergeIfPossible(RuleConfig child, List<RuleConfig> parents) {
        // @formatter:off
        // find parent with matching label
        List<RuleConfig> candidates = parents.stream()
            .filter(r -> r.getLabel().equals(child.label))
            .collect(Collectors.toList());
        // should we be able to have more than one matching parent?
        for (RuleConfig parent : candidates) {
            mergeChildIntoParent(child, parent);
        }
        // might be the case that there are no matching labels
        return !candidates.isEmpty();
        // @formatter:on
    }

    /**
     * combinedRule is expected to be a copy of the parent rule, which will be modified with overrides from additionalRule. The matchPatterns in combinedRule
     * will also be appended with the matchPatterns from additionalRule. additionalRule is expected to be the child rule.
     *
     * @param additionalRule
     *            contains the modifications that will be introduced into combinedRule
     * @param combinedRule
     *            contains the base rule, to be amended with additionalRule
     */
    void mergeChildIntoParent(RuleConfig additionalRule, RuleConfig combinedRule) {
        if (additionalRule.isMerge) {
            if (null != additionalRule.ttlValue) {
                combinedRule.ttlValue = additionalRule.ttlValue;
            }
            if (null != additionalRule.ttlUnits) {
                combinedRule.ttlUnits = additionalRule.ttlUnits;
            }
            combinedRule.extendedOptions.putAll(additionalRule.extendedOptions);
            if (null != additionalRule.matchPattern && !additionalRule.matchPattern.trim().isEmpty()) {
                combinedRule.matchPattern += "\n" + additionalRule.matchPattern;
            }
            // Override the filterClassName
            combinedRule.filterClassName = additionalRule.filterClassName;
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
        String label = getLabelIfAny(ruleElem);

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

        // @formatter:off
        return new RuleConfig(filterClassName, index)
            .ttlValue(ttlValue)
            .ttlUnits(ttlUnits)
            .matchPattern(matchPattern)
            .label(label)
            .setIsMerge(isMerge)
            .extendedOptions(extendedOptions);
        // @formatter:on
    }

    // Return the RuleConfigs found within the configuration file referenced in the provided Node's text
    private Collection<? extends RuleConfig> loadParentRuleConfigs(Node parent) throws IOException {
        Collection<RuleConfig> rules = new ArrayList<>();
        String parentPathStr = parent.getTextContent();

        if (null == parentPathStr || parentPathStr.isEmpty()) {
            throw new IllegalArgumentException("Invalid parent config path, none specified!");
        }
        // loading parent relative to dir that child is in.
        Path parentPath = new Path(this.filePath.getParent(), parentPathStr);
        if (!fs.exists(parentPath)) {
            throw new IllegalArgumentException("Invalid parent config path specified, " + parentPathStr + " does not exist!");
        }
        rules.addAll(loadRuleConfigs(fs.open(parentPath)));
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

    private String getLabelIfAny(Element nodeItem) {
        if (null != nodeItem.getAttributes()) {
            Node namedItem = nodeItem.getAttributes().getNamedItem("label");
            if (null == namedItem) {
                return "";
            }
            return namedItem.getNodeValue();
        } else {
            return "";
        }
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
        String matchPattern = "";
        String filterClassName = null;
        String label;
        boolean isMerge = false;
        Map<String,String> extendedOptions = new HashMap<>();
        int priority = -1;

        public RuleConfig(String filterClassName, int priority) {
            this.filterClassName = filterClassName;
            this.priority = priority;
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

        public RuleConfig label(String label) {
            this.label = label;
            return this;
        }

        public String getLabel() {
            return label;
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
