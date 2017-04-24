package nsa.datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import nsa.datawave.iterators.filter.AgeOffConfigParams;
import nsa.datawave.iterators.filter.ageoff.FilterOptions;
import nsa.datawave.iterators.filter.ageoff.FilterRule;

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
     * @see nsa.datawave.ingest.util.cache.watch.FileSystemWatcher#loadContents(java.io.InputStream)
     */
    @Override
    protected Collection<FilterRule> loadContents(InputStream in) throws IOException {
        
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder;
        
        Document doc;
        try {
            docBuilder = factory.newDocumentBuilder();
            doc = docBuilder.parse(in);
            
            Collection<FilterRule> filterRules = new ArrayList<>();
            Element docElement = doc.getDocumentElement();
            NodeList rules = docElement.getElementsByTagName("rule");
            
            Map<String,String> extendedOptions = new HashMap<>();
            /**
             * This has been changed to support extended options.
             */
            for (int i = 0; i < rules.getLength(); i++) {
                String ttlValue = null;
                String ttlUnits = null;
                String matchPattern = null;
                String filterClassName = null;
                extendedOptions.clear();
                
                Element ruleElem = (Element) rules.item(i);
                
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
                                // to maintain backwards compatibility, continue to produce the legacy option name
                                prefix = nodeName;
                            }
                            for (int k = 0; k < attributeMap.getLength(); k++) {
                                Node attItem = attributeMap.item(k);
                                extendedOptions.put(prefix + "." + attItem.getNodeName(), attItem.getTextContent().trim());
                            }
                        }
                    }
                    
                }
                
                try {
                    FilterRule filter = (FilterRule) Class.forName(filterClassName).newInstance();
                    
                    FilterOptions option = new FilterOptions();
                    
                    if (ttlValue != null) {
                        option.setTTL(Long.valueOf(ttlValue));
                    }
                    if (ttlUnits != null) {
                        option.setTTLUnits(ttlUnits);
                    }
                    option.setOption(AgeOffConfigParams.MATCHPATTERN, matchPattern);
                    
                    StringBuilder extOptions = new StringBuilder();
                    
                    for (Entry<String,String> myOption : extendedOptions.entrySet()) {
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
}
