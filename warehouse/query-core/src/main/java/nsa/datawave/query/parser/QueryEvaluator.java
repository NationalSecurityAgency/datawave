package nsa.datawave.query.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import nsa.datawave.data.type.NoOpType;
import nsa.datawave.data.type.Type;
import nsa.datawave.query.jexl.DatawaveArithmetic;
import nsa.datawave.query.jexl.DatawaveJexlEngine;
import nsa.datawave.query.parser.EventFields.FieldValue;
import nsa.datawave.query.util.Metadata;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * This class evaluates events against a query. The query is passed to the constructor and then parsed. It is evaluated against an event in the evaluate method.
 */
@Deprecated
public class QueryEvaluator {
    private static Logger log = Logger.getLogger(QueryEvaluator.class);
    private static JexlEngine engine = new DatawaveJexlEngine(null, new DatawaveArithmetic(false), null, null);
    private static final String MULTI_VALUED_BOOL_PREFIX = "$";
    private static final String SPECIAL_HANDLING_FIELDNAME_PREFIX = "$";
    
    // According to the JEXL 2.0 docs, the engine is thread-safe. Let's create 1 engine per VM and
    // cache 128 expressions
    static {
        engine.setSilent(false);
        engine.setCache(128);
    }
    private String query = null;
    private Set<String> fieldNames = null;
    private Multimap<String,DatawaveTreeNode> fields = null;
    private Multimap<String,DatawaveTreeNode> specialHandlingNodes = null;
    private List<DatawaveTreeNode> functionNodesUsingLiterals = null;
    private String modifiedQuery = null;
    private JexlContext ctx = new MapContext();
    private boolean caseInsensitive = true;
    private DatawaveTreeNode root;
    private DatawaveQueryAnalyzer analyzer;
    
    private Metadata metadata = new Metadata();
    
    // FieldName -> Collection<Type<?>>
    private Multimap<String,Class<? extends Type<?>>> dataTypeMap = HashMultimap.create();
    private Map<Class<? extends Type<?>>,Type<?>> dataTypeCacheMap = new HashMap<>();
    
    public Multimap<String,Class<? extends Type<?>>> getDataTypeMap() {
        return dataTypeMap;
    }
    
    public Map<Class<? extends Type<?>>,Type<?>> getDataTypeCacheMap() {
        return dataTypeCacheMap;
    }
    
    public QueryEvaluator() {
        setCaseInsensitive(true);
    }
    
    public QueryEvaluator(String query) throws ParseException {
        this();
        setQuery(query);
    }
    
    public QueryEvaluator(String query, boolean insensitive) throws ParseException {
        setCaseInsensitive(insensitive);
        setQuery(query);
    }
    
    public boolean isCaseInsensitive() {
        return caseInsensitive;
    }
    
    public void setCaseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
    }
    
    public void setQuery(String query) throws ParseException {
        this.query = query;
        if (log.isDebugEnabled()) {
            log.debug("Constructor, query: " + query);
        }
        
        // Adding in analyzer and root node for multi-valued fields patch
        // TODO: remove DatawaveQueryParser
        analyzer = new DatawaveQueryAnalyzer();
        root = analyzer.parseJexlQuery(query);
        if (this.caseInsensitive) {
            try {
                root = analyzer.applyCaseSensitivity(root, true, false);
            } catch (Exception e) {
                throw new ParseException(e.getMessage());
            }
            this.query = analyzer.rebuildQueryFromTree(root);
        }
        
        try {
            this.specialHandlingNodes = analyzer.getSpecialHandlingNodes(root);
        } catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
        
        try {
            this.functionNodesUsingLiterals = analyzer.getFunctionNodesUsingLiterals(root);
        } catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
        
        try {
            this.fields = analyzer.getFieldNameToNodeMapWithFunctionsAndNullLiterals(root, metadata);
        } catch (Exception e) {
            throw new ParseException(e.getMessage());
        }
        
        if (log.isDebugEnabled()) {
            log.debug("terms map: " + this.fields);
        }
        
        if (caseInsensitive) {
            fieldNames = new HashSet<>();
            for (String lit : fields.keySet()) {
                fieldNames.add(lit.toUpperCase());
            }
        } else {
            this.fieldNames = fields.keySet();
        }
    }
    
    public String getQuery() {
        return this.query;
    }
    
    public void printLiterals() {
        for (String s : fieldNames) {
            System.out.println("literal: " + s);
        }
    }
    
    public Set<String> getFieldNames() {
        return Collections.unmodifiableSet(this.fieldNames);
    }
    
    /**
     * This method is called when a field has more than one value associated with it. In the case where a field has multiple values, a Object[] is placed into
     * the MapContext with the values. The query is rewritten to test all of the values in the object array.
     *
     * @param query
     * @param fieldName
     * @param fieldValues
     * @return
     */
    public DatawaveTreeNode rewriteQuery(DatawaveTreeNode root, String fieldName, Collection<String> fieldValues, String specialFieldName,
                    Collection<String> validUnevaluatedFieldNames) {
        if (log.isDebugEnabled()) {
            log.debug("rewriteQuery, root: " + root.getContents());
        }
        // Here we have a field that has multiple values. In this case we need to put
        // all values into the jexl context as an array and rewrite the query to account for all
        // of the fields.
        if (caseInsensitive) {
            fieldName = fieldName.toUpperCase();
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Modifying original query: " + query);
        }
        
        // Pull the values out of the FieldValue object, and add all normalized versions to the discrete values from the Event
        List<String> values = new ArrayList<>(fieldValues.size());
        for (String fval : fieldValues) {
            if (this.dataTypeMap.isEmpty() || validUnevaluatedFieldNames.contains(fieldName)) {
                values.add(caseInsensitive ? fval.toLowerCase() : fval);
            } else {
                List<String> normVals = getNormalizedFieldValue(fieldName, fval);
                if (normVals.isEmpty()) { // No normalization took place, probably should never happen
                    values.add(caseInsensitive ? fval.toLowerCase() : fval);
                } else {
                    // we have at least one normalized value
                    for (String normVal : normVals) {
                        values.add(caseInsensitive ? normVal.toLowerCase() : normVal);
                    }
                }
            }
        }
        
        boolean specialHandling = this.specialHandlingNodes.containsKey(fieldName);
        
        if (log.isTraceEnabled() && specialHandling) {
            log.trace(fieldName + " needs special handling.");
        }
        
        if (specialHandling) {
            // Add the array to the context
            ctx.set(specialFieldName, values);
        } else {
            ctx.set(fieldName, values);
        }
        
        if (log.isDebugEnabled()) {
            log.debug("rewriteQuery, added to context: " + (specialHandling ? specialFieldName : fieldName) + " , " + values);
        }
        
        List<DatawaveTreeNode> nodeListForField = new ArrayList<>(fields.get(fieldName));
        Map<String,Integer> nodeToSuffix = new HashMap<>(nodeListForField.size());
        
        if (log.isTraceEnabled()) {
            log.trace("Number of query nodes for the field: " + nodeListForField.size());
        }
        
        String scriptFieldName = specialHandling ? specialFieldName : fieldName;
        
        // Add a script to the beginning of the query for this multi-valued field
        StringBuilder script = new StringBuilder();
        
        // For multi-valued fields, we may have multiple values in the query we are evaluating
        // We need to make sure that multi-valued fields use different variables to keep state
        for (int i = 0; i < nodeListForField.size(); i++) {
            // $FIELD_NAME_X = false;
            script.append(MULTI_VALUED_BOOL_PREFIX).append(scriptFieldName).append("_").append(i).append(" = false;\n");
            
            // Attempt to construct a unique key for a node in the query tree
            // We need some way to uniquely reference a DatawaveTreeNode since it doesn't implement
            // Comparable or anything similar. Makes the assumption that a NTN with the same
            // fieldname, fieldvalue, operator(s), and function name (where appropriate given the type of
            // NTN) uniquely identify a node in the query tree
            DatawaveTreeNode n = nodeListForField.get(i);
            String nodeKey;
            if (n.isFunctionNode()) {
                nodeKey = ParserTreeConstants.JJTFUNCTIONNODE + "_" + n.getOriginalQueryString();
            } else if (n.isRangeNode()) {
                nodeKey = n.getFieldName() + "_" + n.getRangeLowerOp() + "_" + n.getLowerBound() + "_" + n.getRangeUpperOp() + "_" + n.getUpperBound();
            } else {
                nodeKey = n.getFieldName() + "_" + n.getOperator() + "_" + n.getPredicate();
            }
            
            nodeToSuffix.put(nodeKey, i);
            
            if (log.isDebugEnabled()) {
                log.debug("Added " + nodeKey + " to suffixMap with value " + i);
            }
        }
        
        // for (field : VALUES_FROM_EVENT) {
        script.append("for (field : ").append(scriptFieldName).append(") {\n");
        
        for (int i = 0; i < nodeListForField.size(); i++) {
            DatawaveTreeNode t = nodeListForField.get(i);
            log.debug("Node t: " + t);
            
            // if ($FIELD_NAME_X == false && ...
            script.append("\tif ($").append(scriptFieldName).append("_").append(i).append(" == false && ");
            if (t.getType() == ParserTreeConstants.JJTFUNCTIONNODE) {
                log.debug("rewriteQuery, functionNode: " + t.toString());
                
                // ... function(field, ...)) {
                if (specialHandling) {
                    // The Reference in the function's argument will be quoted as a string
                    script.append(t.getOriginalQueryString().replace("'" + fieldName + "'", "field")).append(") { \n");
                } else {
                    script.append(t.getOriginalQueryString().replace(scriptFieldName, "field")).append(") { \n");
                }
            } else if (t.isRangeNode()) {
                log.debug("Multi-valued field on a RangeNode: " + t.toString());
                
                // .. field < val && field > val) {
                script.append("field ").append(t.getRangeLowerOp()).append(" '").append(t.getLowerBound()).append("' && ");
                script.append("field ").append(t.getRangeUpperOp()).append(" '").append(t.getUpperBound()).append("') { \n");
            } else {
                log.debug("fieldName:" + scriptFieldName + "  ,  operator: " + t.getOperator() + "  ,  predicate: " + t.getPredicate());
                
                // .. field == val) {
                script.append("field ").append(t.getOperator()).append(" ").append(t.getPredicate()).append(") { \n");
            }
            
            // $FIELD_NAME_X = true;
            script.append("\t\t$").append(scriptFieldName).append("_").append(i).append(" = true;\n\t}\n");
        }
        
        script.append("}\n");
        
        // Convert the final query string into a form with the proper variable names from above (e.g. FIELD_NAME -> $FIELD_NAME_X)
        root = this.modifyNodeInQueryTree(root, fieldName, nodeToSuffix);
        
        script.append(root.getScript());
        
        // Add the final evaluation to the script and set it on the DatawaveTreeNode)
        root.setScript(script.toString());
        
        if (log.isDebugEnabled()) {
            log.debug("leaving rewriteQuery with: " + root.getContents() + " and script: " + root.getScript());
        }
        return root;
    }
    
    /**
     * Given the query tree as a DatawaveTreeNode, convert the fieldnames in the tree nodes that had to be modified to account for multiple values in the Event.
     *
     * @param root
     * @param fName
     * @param nodeToSuffix
     * @return
     */
    private DatawaveTreeNode modifyNodeInQueryTree(DatawaveTreeNode root, String fName, Map<String,Integer> nodeToSuffix) {
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        while (bfe.hasMoreElements()) {
            DatawaveTreeNode node = bfe.nextElement();
            if (!node.isLeaf()) {
                continue;
            }
            
            // Unravel the mapping of DatawaveTreeNode to the suffix ("_X"). This ensures
            // that a node in the query correctly maps to the special boolean var created in rewriteQuery().
            String nodeKey;
            if (node.isFunctionNode()) {
                nodeKey = ParserTreeConstants.JJTFUNCTIONNODE + "_" + node.getOriginalQueryString();
            } else if (node.isRangeNode()) {
                nodeKey = node.getFieldName() + "_" + node.getRangeLowerOp() + "_" + node.getLowerBound() + "_" + node.getRangeUpperOp() + "_"
                                + node.getUpperBound();
            } else {
                nodeKey = node.getFieldName() + "_" + node.getOperator() + "_" + node.getPredicate();
            }
            
            String suffix = "";
            if (nodeToSuffix.containsKey(nodeKey)) {
                suffix = "_" + nodeToSuffix.get(nodeKey).toString();
            }
            
            log.debug("modifyNodeInQuery: fName:" + fName + "  node: " + node);
            if (node.isFunctionNode()
                            && (node.getStringFunctionArgs().get(0).equalsIgnoreCase(fName) || node.getStringFunctionArgs().get(0)
                                            .equalsIgnoreCase("'" + fName + "'"))) {
                log.trace("modifyNodeInQuery, function node, fName: " + fName + suffix);
                log.debug("Using suffix '" + suffix + "' for fname '" + fName + "' with nodeKey '" + nodeKey + "'");
                node.setFunctionNode(false);
                node.setOperator("==");
                node.setType(ParserTreeConstants.JJTEQNODE);
                node.setFieldName(MULTI_VALUED_BOOL_PREFIX + fName + suffix);
                node.setFieldValue("true");
                node.setFieldValueLiteralType(ASTTrueNode.class);
            } else if (node.isRangeNode() && node.getFieldName().equalsIgnoreCase(fName)) {
                node.setFieldName(MULTI_VALUED_BOOL_PREFIX + fName + suffix);
                node.setRangeLowerOp("==");
                node.setRangeUpperOp("==");
                node.setLowerBound("true");
                node.setUpperBound("true");
                node.setFieldValueLiteralType(ASTStringLiteral.class);
                log.debug("modifyNodeInQueryTree, rangenode: " + node.printRangeNode());
                log.debug("Using suffix '" + suffix + "' for fname '" + fName + "' with nodeKey '" + nodeKey + "'");
            } else if (fName.equalsIgnoreCase(node.getFieldName())) {
                log.debug("Using suffix '" + suffix + "' for fname '" + fName + "' with nodeKey '" + nodeKey + "'");
                
                // field might be null in node if we have a function node
                if (this.specialHandlingNodes.containsKey(node.getFieldName())) {
                    log.trace("modifyNodeInQuery, node had special handling");
                    node.setFieldName(MULTI_VALUED_BOOL_PREFIX + MULTI_VALUED_BOOL_PREFIX + fName + suffix);
                } else {
                    log.trace("modifyNodeInQuery, no special handling");
                    node.setFieldName(MULTI_VALUED_BOOL_PREFIX + fName + suffix);
                }
                
                node.setFieldValue("true");
                node.setFieldValueLiteralType(ASTTrueNode.class);
                node.setType(ParserTreeConstants.JJTEQNODE);
                node.setOperator("==");
            }
        }
        
        return root;
    }
    
    /**
     * Evaluates the query against an event.
     *
     * @param eventFields
     * @return
     */
    public boolean evaluate(EventFields eventFields) throws ParseException {
        return evaluate(eventFields, null);
    }
    
    /**
     * Evaluates the query against an event.
     *
     * @param eventFields
     * @return
     */
    public boolean evaluate(EventFields eventFields, Map<String,Object> extraParameters) throws ParseException {
        this.modifiedQuery = null;
        boolean rewritten = false;
        DatawaveTreeNode tmpRoot = root;
        boolean firstTime = true;
        
        // Copy the query
        StringBuilder q = new StringBuilder(query);
        
        // Copy the fieldNames, we are going to remove elements from this set
        // when they are added to the JEXL context. This will allow us to
        // determine which items in the query where *NOT* in the data.
        HashSet<String> literalsCopy = new HashSet<>(fieldNames);
        if (log.isDebugEnabled()) {
            log.debug("literalsCopy: " + literalsCopy);
            for (String s : this.specialHandlingNodes.keySet()) {
                log.trace("special handling fieldName: " + s);
            }
        }
        
        // Convert the eventFields to a Multimap<String,String>
        Multimap<String,String> eventFieldValues = HashMultimap.create();
        for (Map.Entry<String,FieldValue> entry : eventFields.entries()) {
            eventFieldValues.put(entry.getKey(), new String(entry.getValue().getValue()));
        }
        
        // Inject valid unevaluated field names/value pairs into the event
        // these should not be modified, so add the field names to a set of fields to skip when normalizing.
        HashSet<String> validUnevaluatedFieldNames = new HashSet<>();
        if (extraParameters != null) {
            Object o = extraParameters.get("validUnevaluatedFields");
            if (o != null) {
                Multimap<String,String> validUnevaluatedFields = (Multimap<String,String>) o;
                extraParameters.remove(o);
                
                for (Map.Entry<String,Collection<String>> field : validUnevaluatedFields.asMap().entrySet()) {
                    validUnevaluatedFieldNames.add(field.getKey());
                    if (log.isDebugEnabled()) {
                        log.debug("Adding unevaluatedFields to event field values: " + field.getKey() + ", " + field.getValue());
                    }
                    eventFieldValues.putAll(field.getKey(), field.getValue());
                }
            }
        }
        
        // Loop through the event fields and add them to the JexlContext.
        for (Entry<String,Collection<String>> field : eventFieldValues.asMap().entrySet()) {
            String eventFieldName = field.getKey();
            if (caseInsensitive) {
                eventFieldName = eventFieldName.toUpperCase();
            }
            // If this field is not part of the expression, then skip it.
            if (!fieldNames.contains(eventFieldName)) {
                log.trace("did not find fieldName : " + eventFieldName + " in the query fieldNames map");
                continue;
            } else {
                literalsCopy.remove(eventFieldName);
            }
            
            // This field may have multiple values.
            if (field.getValue().isEmpty()) {
                continue;
            }
            
            if (this.specialHandlingNodes.containsKey(eventFieldName)) {
                eventFieldName = SPECIAL_HANDLING_FIELDNAME_PREFIX + eventFieldName;
                log.trace("SpecialHandled field: " + eventFieldName);
            }
            if (field.getValue().size() == 1) {
                String fval = field.getValue().iterator().next();
                
                if (log.isDebugEnabled()) {
                    log.debug("evaluate, Event has only 1 value for this field name: " + eventFieldName);
                }
                
                // do not normalize if there are no dataTypes, or this is a validUnevaluatedFieldName
                if (this.dataTypeMap.isEmpty() || validUnevaluatedFieldNames.contains(eventFieldName)) {
                    // We are explicitly converting these bytes to a String.
                    if (caseInsensitive) {
                        ctx.set(eventFieldName, fval.toLowerCase());
                    } else {
                        ctx.set(eventFieldName, fval);
                    }
                } else {
                    List<String> normVals = getNormalizedFieldValue(field.getKey(), fval);
                    if (normVals.isEmpty()) {
                        if (caseInsensitive) {
                            ctx.set(eventFieldName, fval.toLowerCase());
                        } else {
                            ctx.set(eventFieldName, fval);
                        }
                    } else if (normVals.size() == 1) {
                        if (caseInsensitive) {
                            ctx.set(eventFieldName, normVals.get(0).toLowerCase());
                        } else {
                            ctx.set(eventFieldName, normVals.get(0));
                        }
                    } else {
                        // need to force to rewriter - expansion of normVals will happen there.
                        if (firstTime) {
                            tmpRoot = analyzer.copyTree(root);
                            firstTime = false;
                        }
                        tmpRoot = rewriteQuery(tmpRoot, field.getKey(), field.getValue(), eventFieldName, validUnevaluatedFieldNames);
                        rewritten = true;
                    }
                }
                if (log.isTraceEnabled()) {
                    log.trace("ctx has " + eventFieldName + "  :  " + ctx.get(eventFieldName));
                }
                
            } else { // multivalued fields need to go through the query rewrite
                if (firstTime) {
                    tmpRoot = analyzer.copyTree(root);
                    firstTime = false;
                    log.trace("tmpRoot: " + tmpRoot.getContents());
                }
                tmpRoot = rewriteQuery(tmpRoot, field.getKey(), field.getValue(), eventFieldName, validUnevaluatedFieldNames);
                rewritten = true;
            }// End of if
            
        }// End of loop
        
        if (!this.specialHandlingNodes.isEmpty()) {
            // need to overwrite the special handling nodes
            if (!rewritten) {
                tmpRoot = analyzer.copyTree(root);
                rewritten = true;
            }
            
            try {
                analyzer.fixSpecialHandlingNodes(tmpRoot, SPECIAL_HANDLING_FIELDNAME_PREFIX);
            } catch (Exception e) {
                throw new ParseException(e.getMessage());
            }
        }
        
        // ensure the function arguments are appropriately marked as identifiers
        if (!this.functionNodesUsingLiterals.isEmpty()) {
            // need to overwrite the special handling nodes
            if (!rewritten) {
                tmpRoot = analyzer.copyTree(root);
                rewritten = true;
            }
            
            try {
                analyzer.fixLiteralFunctionArgs(tmpRoot);
            } catch (Exception e) {
                throw new ParseException(e.getMessage());
            }
        }
        
        // If we went through the rewriteQuery method due to multi-valued field, overwrite q.
        if (rewritten) {
            q.delete(0, q.length());
            q.append(analyzer.rebuildQueryFromTree(tmpRoot));
            log.debug("rewritten query: " + q.toString());
        }
        
        // For any fieldNames in the query that were not found in the data, add them to the context
        // with a null value.
        for (String lit : literalsCopy) {
            if (this.specialHandlingNodes.containsKey(lit)) {
                lit = SPECIAL_HANDLING_FIELDNAME_PREFIX + lit;
            }
            if (log.isDebugEnabled()) {
                log.debug("adding: " + lit + " to the context as null");
            }
            ctx.set(lit, null);
        }
        
        // If we were given extra parameters, add them into the JexlContext
        if (extraParameters != null && extraParameters.size() > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Adding additional parameters to the JexlContext");
            }
            
            for (Entry<String,Object> entry : extraParameters.entrySet()) {
                String key = entry.getKey();
                if (log.isDebugEnabled()) {
                    log.debug("Adding to the JexlContext: " + key + " => " + entry.getValue());
                }
                ctx.set(key, entry.getValue());
            }
        }
        this.modifiedQuery = q.toString();
        if (log.isDebugEnabled()) {
            log.debug("Evaluating query: " + modifiedQuery);
        }
        
        Boolean result = null;
        if (rewritten) {
            Script script = engine.createScript(this.modifiedQuery);
            try {
                result = (Boolean) script.execute(ctx);
            } catch (Exception e) {
                log.error("Error evaluating script: " + this.modifiedQuery + " against event\n" + eventFields.toString(), e);
            }
        } else {
            Expression expr = engine.createExpression(this.modifiedQuery);
            try {
                result = (Boolean) expr.evaluate(ctx);
            } catch (Exception e) {
                log.error("Error evaluating expression: " + this.modifiedQuery + " against event\n" + eventFields.toString(), e);
            }
        }
        return (null != result && result);
    } // End of method
    
    /**
     *
     * @return rewritten query that was evaluated against the most recent event
     */
    public String getModifiedQuery() {
        return this.modifiedQuery;
    }
    
    /**
     * The normalizer classes retrieved client side were flattened into a String delimited by a ";" and passed to the iterators via the options map. Here we are
     * going to unpack that list, instantiate each class so that we have an instance of the normalizer to use for the entire query run.
     *
     * TODO Consider moving this "unpacking" up into the evaluating iterator and passing the map to the QueryEvaluator.
     *
     * @param normalizerList
     */
    public void configureNormalizers(String normalizerList) {
        if (log.isDebugEnabled()) {
            log.debug("configureNormalizers: " + normalizerList);
        }
        String[] parts = normalizerList.split(";");
        for (String part : parts) {
            String[] norms = part.split(":");
            if (norms.length > 1) {
                // norms[0] is the fieldname
                for (int i = 1; i < norms.length; i++) {
                    if (log.isDebugEnabled()) {
                        log.debug("configureNormalizers, attempting to get normalizer: " + norms[i]);
                    }
                    try {
                        @SuppressWarnings("unchecked")
                        Type<?> datawaveType = Type.Factory.createType(norms[i]);
                        Class<? extends Type<?>> clazz = (Class<? extends Type<?>>) datawaveType.getClass();
                        if (log.isDebugEnabled()) {
                            log.debug("configureNormalizers, clazz: " + clazz);
                        }
                        if (!dataTypeCacheMap.containsKey(clazz)) {
                            dataTypeCacheMap.put(clazz, datawaveType);
                        }
                        String fName = norms[0];
                        if (this.caseInsensitive) {
                            fName = fName.toUpperCase(); // fieldnames are always uppercase
                        }
                        dataTypeMap.put(fName, clazz);
                    } catch (Exception ex) {
                        log.error("Could not instantiate datawaveType for: " + norms[i]);
                    }
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("configureNormalizers, normalizer map: " + dataTypeMap);
        }
    }
    
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }
    
    public Metadata getMetadata() {
        return this.metadata;
    }
    
    /**
     * Given a field name and value, apply all dataType normalizers for that field name to the value and return a list of the normalized values.
     *
     * @param fName
     *            The Field Name
     * @param fieldValue
     *            The Field Value
     * @return List<String> containing all normalized forms of the fieldValue. NOTE: Currently we are returning a list of strings, because Accumulo only uses
     *         strings at present.
     */
    public List<String> getNormalizedFieldValue(String fName, String fValue) {
        List<String> normStrings = new ArrayList<>();
        fName = fName.toUpperCase();
        if (log.isDebugEnabled()) {
            log.debug("normalizeMeList, fieldName: " + fName);
        }
        if (this.dataTypeMap.containsKey(fName)) {
            for (Class<? extends Type<?>> clazz : this.dataTypeMap.get(fName)) {
                Type<?> normalizer;
                if (this.dataTypeCacheMap.containsKey(clazz)) {
                    normalizer = this.dataTypeCacheMap.get(clazz);
                } else {
                    try {
                        normalizer = clazz.newInstance();
                    } catch (InstantiationException ex) {
                        log.error("Could not instantiate normalizer in QueryEvaluator, using NoOp, ex: " + ex);
                        normalizer = new NoOpType();
                    } catch (IllegalAccessException ex) {
                        log.error("Could not instantiate normalizer in QueryEvaluator, using NoOp, ex: " + ex);
                        normalizer = new NoOpType();
                    }
                }
                
                if (log.isDebugEnabled()) {
                    log.debug("normalizeMeList, normazlier: " + normalizer.getClass());
                }
                
                try {
                    normStrings.add(normalizer.normalize(fValue));
                } catch (Exception e) {
                    log.error("Unable to normalize " + fName + " = " + fValue + " using " + normalizer.getClass() + ", skipping form");
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("normalizeMeList: " + normStrings);
        }
        return normStrings;
    }
    
}
