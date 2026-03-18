package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import datawave.iterators.filter.AgeOffConfigParams;

public class XmlRuleProcessor {
    private static final Logger log = Logger.getLogger(XmlRuleProcessor.class);

    private final AgeOffFileLoaderDependencyProvider loaderConfig;

    public XmlRuleProcessor(AgeOffFileLoaderDependencyProvider loaderConfig) {
        this.loaderConfig = loaderConfig;
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
            // add what is left to the end of the list
            ruleConfigs.addAll(childRules);

            // @formatter:on
        } catch (ParserConfigurationException | SAXException ex) {
            log.trace("An error occurred while loading age-off rules, the exception will be rethrown", ex);
            throw new IOException(ex);
        } finally {
            IOUtils.closeStream(in);
        }
        return ruleConfigs;
    }

    // Return the RuleConfigs found within the configuration file referenced in the provided Node's text
    private Collection<? extends RuleConfig> loadParentRuleConfigs(Node parent) throws IOException {
        Collection<RuleConfig> rules = new ArrayList<>();
        InputStream parentRuleInputStream = loaderConfig.getParentStream(parent);

        rules.addAll(loadRuleConfigs(parentRuleInputStream));
        return rules;
    }

    boolean mergeIfPossible(RuleConfig child, List<RuleConfig> parents) {
        // @formatter:off
        // find parent with matching label
        List<RuleConfig> candidates = parents.stream()
                .filter(r -> r.getLabel().equals(child.getLabel()))
                .collect(Collectors.toList());

        // should we be able to have more than one matching parent?
        for (RuleConfig parent : candidates) {
            RuleConfig mergedRule = mergeChildIntoParent(child, parent);
            if (mergedRule != null) {
                int index = parents.indexOf(parent);
                if (index != -1) {
                    parents.set(index, mergedRule);
                }
            }
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
     * @return the new merged RuleConfig or null if no merging was done
     */
    RuleConfig mergeChildIntoParent(RuleConfig additionalRule, RuleConfig combinedRule) {
        if (additionalRule.isMerge()) {
            // Merge parent and child extended options
            Map<String,String> mergedExtendedOptions = new HashMap<>(combinedRule.getExtendedOptions());
            mergedExtendedOptions.putAll(additionalRule.getExtendedOptions());

            // @formatter:off
            RuleConfig.Builder builder = new RuleConfig.Builder(additionalRule.getFilterClassName(), combinedRule.getPriority())
                    .setTtlValue(combinedRule.getTtlValue())
                    .setTtlUnits(combinedRule.getTtlUnits())
                    .setMatchPattern(combinedRule.getMatchPattern())
                    .setLabel(combinedRule.getLabel())
                    .setMerge(combinedRule.isMerge())
                    .setExtendedOptions(mergedExtendedOptions);
            // @formatter:on

            if (additionalRule.getTtlValue() != null) {
                builder.setTtlValue(additionalRule.getTtlValue());
            }
            if (additionalRule.getTtlUnits() != null) {
                builder.setTtlUnits(additionalRule.getTtlUnits());
            }
            if (additionalRule.getMatchPattern() != null && !additionalRule.getMatchPattern().trim().isEmpty()) {
                builder.setMatchPattern(combinedRule.getMatchPattern() + "\n" + additionalRule.getMatchPattern());
            }

            // Create a new RuleConfig with the merged values
            return builder.build();
        }
        return null;
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

            switch (nodeName) {
                case "filterClass":
                    filterClassName = nodeItem.getTextContent().trim();
                    break;
                case "ttl":
                    ttlValue = nodeItem.getTextContent().trim();
                    ttlUnits = ((Element) nodeItem).getAttribute("units").trim();
                    break;
                case "ttlValue":
                    ttlValue = nodeItem.getTextContent().trim();
                    break;
                case "ttlUnits":
                    ttlUnits = nodeItem.getTextContent().trim();
                    break;
                case "matchPattern":
                    matchPattern = nodeItem.getTextContent().trim();
                    break;
                default:
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
                    break;
            }
        }
        extendedOptions.put(AgeOffConfigParams.IS_MERGE, Boolean.toString(isMerge));

        // @formatter:off
        return new RuleConfig.Builder(filterClassName, index)
                .setTtlValue(ttlValue)
                .setTtlUnits(ttlUnits)
                .setMatchPattern(matchPattern)
                .setLabel(label)
                .setMerge(isMerge)
                .setExtendedOptions(extendedOptions)
                .build();
        // @formatter:on
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
}
