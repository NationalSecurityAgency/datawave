package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import datawave.query.language.functions.QueryFunction;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.jexl.JexlBooleanNode;
import datawave.query.language.parser.jexl.JexlNode;
import datawave.query.language.parser.jexl.JexlSelectorNode;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.webservice.query.QueryImpl.Parameter;

import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.springframework.util.PropertyPlaceholderHelper;

public class Chain extends JexlQueryFunction {
    
    private Logger log = Logger.getLogger(Chain.class);
    private static final String PARAMETER_SEPARATOR = ";";
    private static final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
    private Map<String,Object> fieldMap = null;
    private String queryLogic = null;
    private String query = null;
    private String begin = null;
    private String end = null;
    private String params = null;
    private Map<String,Parameter> parameterMap = new HashMap<>();
    
    public Chain() {
        super("chain", new ArrayList<>());
    }
    
    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        super.initialize(parameterList, depth, parent);
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        
        if (this.parameterList.size() < 5 || this.parameterList.size() > 6) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        String fieldMapStr = parameterList.get(0);
        fieldMap = parseFields(fieldMapStr);
        
        queryLogic = parameterList.get(1);
        query = parameterList.get(2);
        begin = parameterList.get(3);
        end = parameterList.get(4);
        if (parameterList.size() == 6) {
            params = parameterList.get(5);
        }
        
        DateFormatUtil dateFormatter = new DateFormatUtil();
        try {
            dateFormatter.fromString(begin);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid value for begin argument to function: " + this.name + " (" + begin + ")", e);
        }
        try {
            dateFormatter.fromString(end);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid value for end argument to function: " + this.name + " (" + end + ")", e);
        }
        if (params != null) {
            try {
                parameterMap = parseParameters(params);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("invalid value for params argument to function: " + this.name + " (" + params + ")", e);
            }
        }
    }
    
    public String getBegin() {
        return begin;
    }
    
    public String getEnd() {
        return end;
    }
    
    public String getQueryLogic() {
        return queryLogic;
    }
    
    public String getQuery() {
        return query;
    }
    
    public String getParams() {
        return params;
    }
    
    private Map<String,Parameter> parseParameters(String params) {
        
        Map<String,Parameter> parameterMap = new HashMap<>();
        if (null != params) {
            String[] param = params.split(PARAMETER_SEPARATOR);
            for (String yyy : param) {
                String[] parts = yyy.split(PARAMETER_NAME_VALUE_SEPARATOR);
                if (parts.length == 2) {
                    parameterMap.put(parts[0], new Parameter(parts[0], parts[1]));
                }
            }
        }
        return parameterMap;
    }
    
    private Map<String,Object> parseFields(String fieldMapStr) {
        
        Map<String,Object> fieldMap = new HashMap<>();
        
        String[] split1 = fieldMapStr.split(",");
        for (String s : split1) {
            String[] split2 = s.trim().split("=");
            String variableName = split2[0];
            Object variableValue = null;
            
            if (split2.length == 1) {
                variableValue = variableName;
            } else {
                String variableValueString = (String) split2[1];
                if (variableValueString.startsWith("[") && variableValueString.endsWith("]")) {
                    List<String> choiceList = new ArrayList<>();
                    String[] choices = variableValueString.split("|");
                    Collections.addAll(choiceList, choices);
                    variableValue = choiceList;
                } else {
                    variableValue = variableValueString;
                }
            }
            fieldMap.put(variableName, variableValue);
        }
        return fieldMap;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("#");
        sb.append(this.getName().toUpperCase());
        sb.append("(");
        boolean first = true;
        for (String p : parameterList) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(escapeString(p));
        }
        sb.append(")");
        return sb.toString();
    }
    
    public Set<String> getProcessedQueries(Document d) {
        
        Set<String> querySet = new HashSet<>();
        Properties singleProperties = new Properties();
        Properties undefinedProperties = new Properties();
        Map<String,List<String>> multiValueMap = new LinkedHashMap<>();
        
        // cycle through variable to field mapping
        for (Map.Entry<String,Object> entry : fieldMap.entrySet()) {
            String k = entry.getKey();
            Object v = entry.getValue();
            
            String fieldName = null;
            if (v instanceof String) {
                // simple case
                fieldName = (String) v;
            } else {
                // list of field choices, take the first that we find
                List<String> choiceList = (List<String>) v;
                for (String s : choiceList) {
                    if (d.containsKey(s)) {
                        fieldName = s;
                        break;
                    }
                }
            }
            
            if (fieldName != null) {
                List<String> fieldValues = getValueFromDocument(d, fieldName);
                if (fieldValues.size() == 0) {
                    undefinedProperties.setProperty(k, "__UNDEFINED__");
                } else if (fieldValues.size() == 1) {
                    singleProperties.setProperty(k, fieldValues.get(0));
                } else if (fieldValues.size() > 1) {
                    multiValueMap.put(k, fieldValues);
                }
            }
        }
        
        // initialize indexes
        Map<String,Integer> index = new LinkedHashMap<>();
        Map<String,Integer> sizes = new LinkedHashMap<>();
        for (Map.Entry<String,List<String>> entry : multiValueMap.entrySet()) {
            index.put(entry.getKey(), 0);
            sizes.put(entry.getKey(), entry.getValue().size());
        }
        
        PropertyPlaceholderHelper pph = new PropertyPlaceholderHelper("${", "}");
        Properties p = new Properties();
        do {
            p.clear();
            p.putAll(singleProperties);
            for (Map.Entry<String,Integer> entry : index.entrySet()) {
                p.setProperty(entry.getKey(), multiValueMap.get(entry.getKey()).get(entry.getValue()));
            }
            String finalQuery = null;
            if (undefinedProperties.isEmpty()) {
                finalQuery = pph.replacePlaceholders(query, p);
            } else {
                p.putAll(undefinedProperties);
                finalQuery = pph.replacePlaceholders(query, p);
                finalQuery = fixQuery(finalQuery);
            }
            if (finalQuery != null) {
                querySet.add(finalQuery);
            }
        } while (incrementIndexes(index, sizes));
        
        return querySet;
    }
    
    static private String fixQuery(String query) {
        
        JexlNode node = null;
        try {
            LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
            node = parser.convertToJexlNode(query);
            fixQuery(node);
        } catch (ParseException e) {
            return null;
        }
        
        if (node.toString().contains("__UNDEFINED__")) {
            return null;
        } else {
            return node.toString();
        }
    }
    
    static private void fixQuery(JexlNode node) throws ParseException {
        
        System.out.println(node.toString() + " -- " + node.getClass().getName() + " -- " + node.getChildren().size());
        for (JexlNode n : node.getChildren()) {
            fixQuery(n);
        }
        
        if (node instanceof JexlBooleanNode) {
            JexlBooleanNode jbn = (JexlBooleanNode) node;
            if (jbn.getType().equals(JexlBooleanNode.Type.OR)) {
                Iterator<JexlNode> itr = jbn.getChildren().iterator();
                while (itr.hasNext()) {
                    JexlNode n = itr.next();
                    if (n instanceof JexlSelectorNode) {
                        JexlSelectorNode jsn = (JexlSelectorNode) n;
                        if (jsn.getSelector().contains("__UNDEFINED__") || jsn.getField().contains("__UNDEFINED__")) {
                            // see if the OR clause can survive without this node
                            itr.remove();
                        }
                    }
                }
                if (jbn.getChildren().size() == 0) {
                    throw new ParseException("Too many undefined terms");
                }
            }
        }
    }
    
    static private boolean incrementIndexes(Map<String,Integer> index, Map<String,Integer> sizes) {
        
        int position = 0;
        for (String k : index.keySet()) {
            
            Integer v = index.get(k);
            Integer s = sizes.get(k);
            if (v + 1 == s) {
                if (position < index.size() - 1) {
                    index.put(k, 0);
                } else {
                    return false;
                }
            } else {
                index.put(k, v + 1);
                return true;
            }
            position++;
        }
        return false;
    }
    
    private List<String> getValueFromDocument(Document document, String desiredField) {
        List<String> terms = new ArrayList<>();
        
        try {
            // Fetch all 'selector' fields from the tasking event
            Attribute<?> desiredFieldAttr = document.get(desiredField);
            
            if (null == desiredFieldAttr || desiredFieldAttr.size() == 0) {
                return terms;
            }
            
            Attributes desiredFieldAttributes = null;
            if (desiredFieldAttr instanceof Attributes == false) {
                desiredFieldAttributes = new Attributes(false);
                desiredFieldAttributes.add(desiredFieldAttr);
            } else {
                desiredFieldAttributes = (Attributes) desiredFieldAttr;
            }
            
            // Join each value for this 'selector' field, grouped together
            for (Attribute<?> desiredAttr : desiredFieldAttributes.getAttributes()) {
                String val = new String(desiredAttr.getData().toString());
                
                val = val.replaceAll("'", "\\\\'").trim();
                
                if (val.isEmpty()) {
                    continue;
                }
                
                // Add selectorVal
                terms.add(val);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return terms;
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Chain();
    }
    
    public Map<String,Parameter> getParameterMap() {
        return parameterMap;
    }
}
