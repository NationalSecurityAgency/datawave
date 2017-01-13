package nsa.datawave.query.parser;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import nsa.datawave.core.iterators.EvaluatingIterator;
import nsa.datawave.data.type.Type;
import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.query.config.GenericShardQueryConfiguration;
import nsa.datawave.query.functions.JexlFunctionArgumentDescriptorFactory;
import nsa.datawave.query.functions.arguments.JexlArgument;
import nsa.datawave.query.functions.arguments.JexlArgument.JexlArgumentType;
import nsa.datawave.query.functions.arguments.JexlArgumentDescriptor;
import nsa.datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.exceptions.InvalidQueryException;
import nsa.datawave.query.util.Metadata;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.webservice.common.logging.ThreadConfigurableLogger;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.QueryException;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * The server-side field index queries can only support operations on indexed fields. Additionally, queries that have differing ranges (i.e. one range at the
 * fieldname level and another at the fieldValue level) are not currently supported. This class removes these conflicts from the query as well as sets proper
 * capitalization configurations etc.
 *
 * Once the query has been modified, you can pass it to the BooleanLogicIterator on the server-side via the options map.
 *
 */

/*
 * This used to be more of an API with order sensitive methods walled off as private. It has been opened up as we need more functionality, so it's kind of a
 * mess right now!
 */
@Deprecated
public class DatawaveQueryAnalyzer {
    
    protected static final Logger log = ThreadConfigurableLogger.getLogger(DatawaveQueryAnalyzer.class);
    public static final String INDEXED_TERMS_LIST = "INDEXED_TERMS_LIST"; // comma
                                                                          // separated
                                                                          // list
                                                                          // of
                                                                          // indexed
                                                                          // terms.
    public static Set<Integer> rangeNodeSet;
    
    static {
        rangeNodeSet = new HashSet<>();
        rangeNodeSet.add(ParserTreeConstants.JJTLENODE);
        rangeNodeSet.add(ParserTreeConstants.JJTLTNODE);
        rangeNodeSet.add(ParserTreeConstants.JJTGENODE);
        rangeNodeSet.add(ParserTreeConstants.JJTGTNODE);
        rangeNodeSet = Collections.unmodifiableSet(rangeNodeSet);
    }
    
    /**
     * Given a query parse tree, upper or lower case the FieldNames and FieldValues. Currently DATAWAVE requires UPPER case FIELDNAMES and lower case field
     * values, but we want to allow people to input the query however they wish.
     *
     * @param root
     *            root DatawaveTreeNode of the query parse tree.
     * @param fieldNameUpperCase
     *            true->UPPER false->lower
     * @param fieldValueUpperCase
     *            true->UPPER false->lower
     * @return root DatawaveTeeNode of the modified query parse tree.
     * @throws JavaRegexParseException
     */
    public DatawaveTreeNode applyCaseSensitivity(DatawaveTreeNode root, boolean fieldNameUpperCase, boolean fieldValueUpperCase) throws JavaRegexParseException {
        // for each leaf, apply case sensitivity
        Enumeration<?> bfe = root.breadthFirstEnumeration();
        while (bfe.hasMoreElements()) {
            DatawaveTreeNode node = (DatawaveTreeNode) bfe.nextElement();
            if (node.isLeaf()) {
                if (node.isRangeNode()) {
                    node.setFieldName(fieldNameUpperCase ? node.getFieldName().toUpperCase() : node.getFieldName().toLowerCase());
                    node.setLowerBound(fieldValueUpperCase ? node.getLowerBound().toUpperCase() : node.getLowerBound().toLowerCase());
                    node.setUpperBound(fieldValueUpperCase ? node.getUpperBound().toUpperCase() : node.getUpperBound().toLowerCase());
                } else if (node.isFunctionNode()) {
                    JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
                    if (desc != null) {
                        JexlArgument[] args = desc.getArguments();
                        for (int i = 0; i < args.length; i++) {
                            JexlArgument arg = args[i];
                            if (arg.getArgumentType() == JexlArgumentType.FIELD_NAME) {
                                arg.getJexlArgumentNode().image = fieldNameUpperCase ? arg.getJexlArgumentNode().image.toUpperCase() : arg
                                                .getJexlArgumentNode().image.toLowerCase();
                            } else if (arg.getArgumentType() == JexlArgumentType.VALUE) {
                                arg.getJexlArgumentNode().image = fieldValueUpperCase ? arg.getJexlArgumentNode().image.toUpperCase() : arg
                                                .getJexlArgumentNode().image.toLowerCase();
                            } else if (arg.getArgumentType() == JexlArgumentType.REGEX) {
                                JavaRegexAnalyzer regex = new JavaRegexAnalyzer(arg.getJexlArgumentNode().image);
                                regex.applyRegexCaseSensitivity(fieldValueUpperCase);
                                arg.getJexlArgumentNode().image = regex.getRegex();
                            }
                        }
                    }
                } else {
                    if (node.getFieldName() != null) {
                        String fName = fieldNameUpperCase ? node.getFieldName().toUpperCase() : node.getFieldName().toLowerCase();
                        node.setFieldName(fName);
                    }
                    
                    if (node.getFieldValue() != null) {
                        if (node.getType() == ParserTreeConstants.JJTERNODE || node.getType() == ParserTreeConstants.JJTNRNODE) {
                            JavaRegexAnalyzer regex = new JavaRegexAnalyzer(node.getFieldValue());
                            regex.applyRegexCaseSensitivity(fieldValueUpperCase);
                            node.setFieldValue(regex.getRegex());
                        } else {
                            String fValue = fieldValueUpperCase ? node.getFieldValue().toUpperCase() : node.getFieldValue().toLowerCase();
                            node.setFieldValue(fValue);
                        }
                    }
                }
            }
        }
        return root;
    }
    
    /**
     * Given a query parse tree, count the number of terms.
     * 
     * @param root
     *            root DatawaveTreeNode of the query parse tree.
     * @return count the count of terms
     */
    public int countTerms(DatawaveTreeNode root) {
        int count = 0;
        Enumeration<?> bfe = root.breadthFirstEnumeration();
        while (bfe.hasMoreElements()) {
            DatawaveTreeNode node = (DatawaveTreeNode) bfe.nextElement();
            if (node.isLeaf()) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Given the root of the query parse tree and a set of Indexed Field Names, determine if we can run an optimized query or not.
     * 
     * @param root
     *            the root node of the query parse tree.
     * @param indexedFields
     *            Set of FieldNames which are indexed.
     * @return true/false on whether we can run an optimized query or not.
     * @throws JavaRegexParseException
     */
    public boolean isOptimizedQuery(DatawaveTreeNode root, Set<String> indexedFields) throws JavaRegexParseException {
        return isOptimizedQuery(root, indexedFields, Collections.<String> emptySet());
    }
    
    /**
     * Given the root of the query parse tree and a set of Indexed Field Names, determine if we can run an optimized query or not.
     *
     * @param root
     *            the root node of the query parse tree.
     * @param indexedFields
     *            Set of FieldNames which are indexed.
     * @param indexOnlyFields
     *            Set of FieldNames which are indexed but not included in the event.
     * @return true/false on whether we can run an optimized query or not.
     * @throws JavaRegexParseException
     */
    public boolean isOptimizedQuery(DatawaveTreeNode root, Set<String> indexedFields, Set<String> indexOnlyFields) throws JavaRegexParseException {
        if (log.isDebugEnabled()) {
            log.debug("isOptimizedQuery(root,indexedTerms): " + root.getContents());
        }
        
        if (null == indexedFields) {
            indexedFields = Collections.emptySet();
        }
        
        if (null == indexOnlyFields) {
            indexedFields = Collections.emptySet();
        }
        
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> nodes = Collections.list(root.breadthFirstEnumeration());
        
        // walk backwards
        for (int i = nodes.size() - 1; i >= 0; i--) {
            DatawaveTreeNode node = nodes.get(i);
            if (node.isLeaf() && !node.isFunctionNode()) {
                if (node.isNegated()) {
                    node.setOptimized(false);
                    continue;
                }
                
                if (node.isFunctionNode() || !indexedFields.contains(node.getFieldName())) {
                    node.setOptimized(false);
                    continue;
                }
                
                // treat unbounded range as filter unless it is index only.
                if (node.isUnboundedRange() && !indexOnlyFields.contains(node.getFieldName())) {
                    node.setOptimized(false);
                    continue;
                }
                
                // we cannot handle null values, negated or not
                if (node.getFieldValueLiteralType() == ASTNullLiteral.class) {
                    node.setOptimized(false);
                    continue;
                }
                
                // if it's a positive regex, test for leading and trailing
                // wildcards
                if (node.getType() == ParserTreeConstants.JJTERNODE) {
                    node.setOptimized(!(new JavaRegexAnalyzer(node.getFieldValue()).isNgram()));
                }
            }
            
            if (node.getType() == ParserTreeConstants.JJTANDNODE) {
                node.setOptimized(false); // easy to prove AND node true;
                
                if (node.isChildrenAllNegated()) {
                    node.setOptimized(false);
                    continue;
                }
                
                // check children, only need 1 optimized, positive child.
                @SuppressWarnings("unchecked")
                Enumeration<DatawaveTreeNode> children = node.children();
                while (children.hasMoreElements()) {
                    DatawaveTreeNode n = children.nextElement();
                    if (n.isOptimized()) { // negations are already not
                                           // optimized
                        // only need one
                        node.setOptimized(true);
                        break;
                    }
                }
                
            } else if (node.getType() == ParserTreeConstants.JJTORNODE) {
                // need all children to be positive & optimized.
                node.setOptimized(true); // easier to prove false
                if (node.isChildrenAllNegated()) {
                    node.setOptimized(false);
                    continue;
                }
                
                @SuppressWarnings("unchecked")
                Enumeration<DatawaveTreeNode> children = node.children();
                while (children.hasMoreElements()) {
                    DatawaveTreeNode n = children.nextElement();
                    if (!n.isOptimized() || n.isNegated() || n.isChildrenAllNegated()) {
                        node.setOptimized(false);
                        break;
                    }
                    
                } // end while
            } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) { // HEAD
                                                                              // node
                root.setOptimized(((DatawaveTreeNode) node.getFirstChild()).isOptimized());
                break;
            } else {
                // what do we do in the case of ranges ?
            }
        }
        return root.isOptimized();
    }
    
    /**
     * Given a map of FieldValue -> Set<FieldNames>, replace the _ANYFIELD_ value with an OR node containing the fName-fValue pairs
     *
     * @param root
     *            Root node of the query parse tree
     * @param valueMap
     *            Mapping of FieldValue to a Set of FieldNames
     * @return root of the modified query parse tree
     */
    public DatawaveTreeNode fixANYFIELDValues(DatawaveTreeNode root, Map<String,Set<String>> valueMap) {
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> nodeList = Collections.list(root.breadthFirstEnumeration());
        
        boolean canProcess = true;
        
        // don't process the parent/root, note the i>=1 criteria
        for (int i = nodeList.size() - 1; canProcess && i >= 1; i--) {
            DatawaveTreeNode node = nodeList.get(i);
            
            // basically not a leaf node and not a function.
            // NOTE: AND/OR nodes can become leaves if all of their children get
            // removed,
            // so you need to be explicit. node.isLeaf() doesn't work here.
            // The HEAD node is skipped in the for loop.
            if ((node.getType() != ParserTreeConstants.JJTANDNODE && node.getType() != ParserTreeConstants.JJTORNODE) && !node.isFunctionNode()
                            && node.getFieldName().equals(Constants.ANY_FIELD)) {
                Set<String> fieldNames = valueMap.get(node.getFieldValue());
                
                if (log.isDebugEnabled()) {
                    log.debug("fixANYFIELDValues, fieldValue: " + node.getFieldValue() + "   fieldNames: " + fieldNames);
                }
                
                if (fieldNames != null && !fieldNames.isEmpty()) {
                    // create an OR node full of equals nodes, or an AND node if negated
                    DatawaveTreeNode replacementNode = null;
                    if (node.isNegated()) {
                        replacementNode = new DatawaveTreeNode(ParserTreeConstants.JJTANDNODE);
                    } else {
                        replacementNode = new DatawaveTreeNode(ParserTreeConstants.JJTORNODE);
                    }
                    for (String fName : fieldNames) {
                        replacementNode.add(new DatawaveTreeNode(node.getType(), fName, node.getFieldValue(), node.isNegated()));
                    }
                    DatawaveTreeNode parent = (DatawaveTreeNode) node.getParent();
                    parent.remove(node);
                    parent.add(replacementNode);
                    
                } else { // this entry is null
                    if (log.isDebugEnabled()) {
                        log.debug("fixANYFIELDValues, fieldNames was either null or empty for value: " + node.getFieldValue());
                    }
                    boolean value = false;
                    // reverse the value if a NE or NR node
                    if (node.getType() == ParserTreeConstants.JJTNENODE || node.getType() == ParserTreeConstants.JJTNRNODE) {
                        value = !value;
                    }
                    // if this node is actually comparing to null, then reverse the value
                    if (node.getFieldValueLiteralType() == ASTNullLiteral.class) {
                        value = !value;
                    }
                    DatawaveTreeNode newRoot = dropNode(root, node, value);
                    // if a new root was returned, then we have a query that always evaluates to true or false
                    if (newRoot != null && newRoot != root) {
                        canProcess = false;
                    }
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("fixANYFIELDValues, can process? " + canProcess);
        }
        if (!canProcess) {
            root.removeAllChildren();
        }
        return root;
    }
    
    /**
     * Parse a query string using a JEXL parser and transform it into a parse tree of our DatawaveTreeNodes. This also sets all convenience maps that the
     * analyzer provides.
     *
     * @param query
     *            The query string in JEXL syntax to parse
     * @return Root node of the query parse tree.
     * @throws ParseException
     */
    public DatawaveTreeNode parseJexlQuery(String query) throws ParseException {
        DatawaveQueryParser too = new DatawaveQueryParser();
        try {
            DatawaveTreeNode root = too.parseQuery(query);
            root = this.refactorTree(root);
            return collapseBranches(root);
        } catch (ParseException e) {
            log.error("Could not parse the given query: " + query);
            throw e;
        } catch (InvalidQueryException iqe) {
            throw new ParseException("Empty query");
        }
    }
    
    /**
     * Given a set of FieldNames which are indexed, we can remove parts of the query where the FieldName is not indexed. Needed if we go down the optimization
     * path using Field Index keys i.e. BooleanLogicIterator
     *
     * Note, this tests the FieldName only, not the FieldName,FieldValue pair known as a QueryTerm. This means you should be able to call this after checking
     * the Meta Data table.
     *
     * This also removes function nodes.
     *
     * @param root
     *            root node of the query parse tree.
     * @param indexedFields
     *            Set of field names which are indexed
     * @return root node of the modified query parse tree with nodes containing non-indexed Fields removed.
     */
    public DatawaveTreeNode removeNonIndexedFields(DatawaveTreeNode root, Set<String> indexedFields) {
        
        @SuppressWarnings(value = "unchecked")
        List<DatawaveTreeNode> nodes = Collections.list(root.breadthFirstEnumeration());
        
        // walk backwards
        boolean canProcess = true;
        for (int i = nodes.size() - 1; canProcess && i >= 0; i--) {
            DatawaveTreeNode node = nodes.get(i);
            
            boolean dropNode = false;
            
            if (log.isDebugEnabled()) {
                log.debug("removeNonIndexedFields, analyzing node: " + node.toString() + "  " + node.printNode() + " , is unbounded range?"
                                + node.isUnboundedRange());
                if (node.isRangeNode()) {
                    log.debug("\trangeNode: " + node.printRangeNode());
                }
            }
            // assume we cannot apply the function to the index as we would have already expanded function nodes that can be expanded
            if (node.isFunctionNode()) {
                if (log.isDebugEnabled()) {
                    log.debug("removeNonIndexedFields, removing function: " + node.getFunctionName());
                }
                dropNode = true;
            } else if (node.getFieldName() != null) {
                if (log.isDebugEnabled()) {
                    log.debug("removeNonIndexedFields, Testing: " + node.getFieldName() + ":" + node.getFieldValue());
                }
                
                if (!indexedFields.contains(node.getFieldName())) {
                    if (log.isDebugEnabled()) {
                        log.debug(node.getFieldName() + ":" + node.getFieldValue() + " is NOT indexed");
                    }
                    dropNode = true;
                } else if (node.isUnboundedRange() && !node.isIndexOnlyField()) {
                    if (log.isDebugEnabled()) {
                        log.debug(node.getFieldName() + " " + node.getOperator() + " " + node.getFieldValue()
                                        + " is unbounded range and not an indexOnly/Unevaluated field.");
                    }
                    dropNode = true;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(node.getFieldName() + ":" + node.getFieldValue() + " is indexed");
                    }
                }
            }
            
            if (dropNode) {
                DatawaveTreeNode newRoot = dropNode(root, node);
                // if a new root was returned, then we have a query that always evaluates to true or false
                if (newRoot != null && newRoot != root) {
                    canProcess = false;
                }
            }
        }
        
        if (!canProcess) {
            // remove all of the children denoting we have nothing left to query the index with
            root.removeAllChildren();
        }
        return root;
    }
    
    public DatawaveTreeNode removeNegationViolations(DatawaveTreeNode root) throws InvalidQueryException {
        // Double check the top level node for negation violations
        // if AND, one child must be positive, if OR, no negatives allowed.
        DatawaveTreeNode one = (DatawaveTreeNode) root.getFirstChild(); // Head node
        // has only
        // 1 child.
        if (one.getType() == JexlOperatorConstants.JJTORNODE) {
            @SuppressWarnings("unchecked")
            ArrayList<DatawaveTreeNode> childrenList = Collections.list(one.children());
            for (DatawaveTreeNode child : childrenList) {
                if (child.isNegated()) {
                    DatawaveTreeNode newRoot = dropNode(root, child);
                    if (newRoot != null && newRoot != root) {
                        QueryException qe = new QueryException(DatawaveErrorCode.EMPTY_QUERY_AFTER_NEGATION_REMOVAL);
                        throw new InvalidQueryException(qe);
                    }
                }
            }
        } else if (one.getType() == JexlOperatorConstants.JJTANDNODE) {
            @SuppressWarnings("unchecked")
            ArrayList<DatawaveTreeNode> childrenList = Collections.list(one.children());
            boolean ok = false;
            for (DatawaveTreeNode child : childrenList) {
                if (!child.isNegated()) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                QueryException qe = new QueryException(DatawaveErrorCode.AND_EXPRESSION_WIHOUT_NON_NEGATED_CHILDREN);
                throw new InvalidQueryException(qe);
            }
        }
        
        return root;
    }
    
    // After tree conflicts have been resolve, we can collapse branches where
    // leaves have been pruned.
    public DatawaveTreeNode collapseBranches(DatawaveTreeNode myroot) throws InvalidQueryException {
        if (log.isDebugEnabled()) {
            log.debug("collapseBranches, root: " + myroot.getContents());
        }
        // NOTE: doing a depth first enumeration didn't work when I started
        // removing nodes halfway through. The following method does work,
        // it's essentially a reverse breadth first traversal.
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> nodes = Collections.list(myroot.breadthFirstEnumeration());
        
        // walk backwards
        for (int i = nodes.size() - 1; i >= 0; i--) {
            DatawaveTreeNode node = nodes.get(i);
            if (log.isDebugEnabled()) {
                log.debug("collapseBranches, inspecting node: " + node.toString() + "  " + node.printNode());
            }
            
            if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
                if (node.getChildCount() == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("AND/OR has no children removing from parent");
                        
                    }
                    node.removeFromParent();
                } else if (node.getChildCount() == 1) {
                    DatawaveTreeNode p = (DatawaveTreeNode) node.getParent();
                    DatawaveTreeNode c = (DatawaveTreeNode) node.getFirstChild();
                    node.removeFromParent();
                    if (node.isNegated()) {
                        c.setNegated(!c.isNegated());
                    }
                    p.add(c);
                    
                }
            } else if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) {
                if (node.getChildCount() == 0) {
                    QueryException qe = new QueryException(DatawaveErrorCode.NO_TERMS_REMAIN_AFTER_OPTIMIZE_REMOVAL);
                    throw new InvalidQueryException(qe);
                }
            }
        }
        return myroot;
    }
    
    /**
     *
     * @param root
     * @return
     */
    private DatawaveTreeNode refactorTree(DatawaveTreeNode root) {
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> backwards = Collections.list(root.breadthFirstEnumeration());
        
        for (int i = backwards.size() - 1; i >= 0; i--) {
            DatawaveTreeNode n = backwards.get(i);
            if (n.getType() == ParserTreeConstants.JJTNOTNODE) {
                DatawaveTreeNode child = (DatawaveTreeNode) n.getChildAt(0);
                child.setNegated(!child.isNegated());
                DatawaveTreeNode parent = (DatawaveTreeNode) n.getParent();
                parent.remove(n);
                parent.add(child);
            }
        }
        
        // collapse range nodes
        root = collapseRanges(root);
        
        // cycle through again and distribute nots
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode child;
        
        while (bfe.hasMoreElements()) {
            child = bfe.nextElement();
            
            if (child.isNegated()) {
                if (child.getChildCount() > 0) {
                    demorganSubTree(child);
                }
            }
        }
        
        // Need to test nodes to set all Children negated piece, easiest to do while walking backwards.
        for (int i = backwards.size() - 1; i >= 0; i--) {
            DatawaveTreeNode node = backwards.get(i);
            if (node.isLeaf()) {
                continue;
            }
            
            @SuppressWarnings("unchecked")
            Enumeration<DatawaveTreeNode> children = node.children();
            boolean allNegative = true;
            while (children.hasMoreElements()) {
                DatawaveTreeNode c = children.nextElement();
                if (!c.isNegated()) {
                    allNegative = false;
                    break;
                }
            }
            node.setChildrenAllNegated(allNegative);
            
            // While we're walking the tree, if you have an ORnode and it has
            // at least 1 negative child, it's query range will be infinite
            if (node.getType() == ParserTreeConstants.JJTORNODE) {
                @SuppressWarnings("unchecked")
                Enumeration<DatawaveTreeNode> childrenTmp = node.children();
                children = childrenTmp;
                while (children.hasMoreElements()) {
                    DatawaveTreeNode c = children.nextElement();
                    if (c.isNegated()) {
                        node.setInfiniteRange(true);
                        break;
                    }
                }
            }
        }
        
        return root;
        
    }
    
    /**
     * Apply DeMorgan rules to a given sub-tree. This distributes a NOT downwards in the parse tree flipping AND/OR as necessary.
     *
     * @param root
     *            of the tree/sub-tree
     */
    private void demorganSubTree(DatawaveTreeNode root) {
        
        // assumptions: root is negated, and root is not a leaf
        
        root.setNegated(false);
        
        if (root.getType() == ParserTreeConstants.JJTANDNODE) {
            root.setType(ParserTreeConstants.JJTORNODE);
        } else if (root.getType() == ParserTreeConstants.JJTORNODE) {
            root.setType(ParserTreeConstants.JJTANDNODE);
        } else {
            log.error("refactorSubTree, node type not supported");
        }
        
        Enumeration<?> children = root.children();
        DatawaveTreeNode child = null;
        // now distribute the negative
        
        while (children.hasMoreElements()) {
            child = (DatawaveTreeNode) children.nextElement();
            if (child.getType() == ParserTreeConstants.JJTNENODE) {
                child.setType(ParserTreeConstants.JJTEQNODE);
                child.setNegated(!child.isNegated());
                child.setOperator(JexlOperatorConstants.getOperator(child.getType()));
            }
            if (child.getType() == ParserTreeConstants.JJTNRNODE) {
                child.setType(ParserTreeConstants.JJTERNODE);
                child.setNegated(!child.isNegated());
                child.setOperator(JexlOperatorConstants.getOperator(child.getType()));
            }
            child.setNegated(!child.isNegated());
        }
    }
    
    /**
     * Given a query parse tree, traverse it and collapse ranges into a single node which contains all pieces. Mark unbounded range nodes as unbounded.
     *
     * Warning, Voodoo. Currently we'll let JEXL ParserTreeConstants.JJTLENODE represent collapsed range nodes. To be clean we should really extend and add a
     * JJTRANGENODE to the ParserTreeConstants. Leave that as a TODO.
     *
     * @param root
     *            root of the query parse tree
     * @return The root of the updated query parse tree with ranges collapsed.
     */
    private DatawaveTreeNode collapseRanges(DatawaveTreeNode root) {
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> backwards = Collections.list(root.breadthFirstEnumeration());
        
        // walk backwards
        for (int i = backwards.size() - 1; i >= 0; i--) {
            DatawaveTreeNode node = backwards.get(i);
            if (node.isLeaf()) {
                if (rangeNodeSet.contains(node.getType())) {
                    ((DatawaveTreeNode) node.getParent()).setContainsRangeNodes(true);
                    node.setUnboundedRange(true); // assume all range nodes are unbounded until proven false
                }
                continue;
            }
            
            if ((node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) && node.containsRangeNodes()) {
                // look at children and collapse the bounded range into a single
                // node.
                
                Map<Text,RangeBounds> boundedRangeMap = getBoundedRangeMap(node);
                
                if (boundedRangeMap != null && !boundedRangeMap.isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("collapseRanges, node: " + node.getContents());
                    }
                    Map<String,DatawaveTreeNode> rangeNodeMap = new HashMap<>();
                    
                    for (Entry<Text,RangeBounds> entry : boundedRangeMap.entrySet()) {
                        DatawaveTreeNode nu = new DatawaveTreeNode(ParserTreeConstants.JJTAMBIGUOUS);
                        nu.setFieldName(entry.getKey().toString());
                        nu.setLowerBound(entry.getValue().getLower().toString());
                        nu.setUpperBound(entry.getValue().getUpper().toString());
                        nu.setRangeNode(true);
                        nu.setUnboundedRange(false); // we have an upper & lower bound
                        rangeNodeMap.put(nu.getFieldName(), nu);
                    }
                    
                    // Warning, Voodoo. We're going to use JJTLENODE to represent the entire range node.
                    // To be clean we really should extend ParserTreeConstants and add a true range node.
                    // Put that down as a future TODO.
                    @SuppressWarnings("unchecked")
                    List<DatawaveTreeNode> children = Collections.list(node.children());
                    for (int j = children.size() - 1; j >= 0; j--) {
                        DatawaveTreeNode child = children.get(j);
                        if (child.getType() == ParserTreeConstants.JJTLENODE || child.getType() == ParserTreeConstants.JJTLTNODE) {
                            if (rangeNodeMap.containsKey(child.getFieldName())) {
                                DatawaveTreeNode nu = rangeNodeMap.get(child.getFieldName());
                                nu.setRangeUpperOp(JexlOperatorConstants.getOperator(child.getType()));
                                nu.setType(ParserTreeConstants.JJTLENODE);
                                nu.setOperator(JexlOperatorConstants.getOperator(nu.getType()));
                            }
                            child.removeFromParent();
                            
                        } else if (child.getType() == ParserTreeConstants.JJTGENODE || child.getType() == ParserTreeConstants.JJTGTNODE) {
                            if (rangeNodeMap.containsKey(child.getFieldName())) {
                                DatawaveTreeNode nu = rangeNodeMap.get(child.getFieldName());
                                nu.setRangeLowerOp(JexlOperatorConstants.getOperator(child.getType()));
                                nu.setType(ParserTreeConstants.JJTLENODE); // see
                                                                           // the
                                                                           // voodoo.
                                nu.setOperator(JexlOperatorConstants.getOperator(nu.getType()));
                            }
                            child.removeFromParent();
                            
                        }
                    }
                    
                    for (Entry<String,DatawaveTreeNode> entry : rangeNodeMap.entrySet()) {
                        if (entry.getValue().getType() == ParserTreeConstants.JJTLENODE) {
                            // add it to the parent node
                            node.add(entry.getValue());
                        }
                    }
                } // else do nothing.
            }
        }
        
        return root;
    }
    
    private Map<Text,RangeBounds> getBoundedRangeMap(DatawaveTreeNode node) {
        if (log.isDebugEnabled()) {
            log.debug("getBoundedRangeMap called");
        }
        
        if (node.getType() == ParserTreeConstants.JJTANDNODE || node.getType() == ParserTreeConstants.JJTORNODE) {
            @SuppressWarnings("unchecked")
            Enumeration<DatawaveTreeNode> children = node.children();
            Map<Text,RangeBounds> rangeMap = new HashMap<>();
            while (children.hasMoreElements()) {
                DatawaveTreeNode child = children.nextElement();
                
                if (!rangeNodeSet.contains(child.getType())) {
                    continue;
                }
                
                // first flip the comparison if negated
                if (child.isNegated()) {
                    child.setNegated(false);
                    if (child.getType() == ParserTreeConstants.JJTLENODE) {
                        child.setType(ParserTreeConstants.JJTGTNODE);
                    } else if (child.getType() == ParserTreeConstants.JJTLTNODE) {
                        child.setType(ParserTreeConstants.JJTGENODE);
                    } else if (child.getType() == ParserTreeConstants.JJTGENODE) {
                        child.setType(ParserTreeConstants.JJTLTNODE);
                    } else if (child.getType() == ParserTreeConstants.JJTGTNODE) {
                        child.setType(ParserTreeConstants.JJTLENODE);
                    }
                }
                
                // upper bound
                if (child.getType() == ParserTreeConstants.JJTLENODE || child.getType() == ParserTreeConstants.JJTLTNODE) {
                    Text fName = new Text(child.getFieldName());
                    if (rangeMap.containsKey(fName)) {
                        RangeBounds rb = rangeMap.get(fName);
                        if (rb.getUpper() != null) {
                            log.warn("testBoundedRangeExistence, two Upper bounds exist for bounded range.");
                            if (node.getType() == ParserTreeConstants.JJTANDNODE) {
                                // set upper to the minimum
                                if (child.getFieldValue().compareTo(rb.getUpper().toString()) < 0) {
                                    rb.setUpper(new Text(child.getFieldValue()));
                                }
                            } else {
                                // set upper to the maximum
                                if (child.getFieldValue().compareTo(rb.getUpper().toString()) > 0) {
                                    rb.setUpper(new Text(child.getFieldValue()));
                                }
                            }
                        } else {
                            rb.setUpper(new Text(child.getFieldValue()));
                        }
                    } else {
                        RangeBounds rb = new RangeBounds();
                        rb.setUpper(new Text(child.getFieldValue()));
                        rangeMap.put(new Text(child.getFieldName()), rb);
                    }
                    
                    // lower bound
                } else if (child.getType() == ParserTreeConstants.JJTGENODE || child.getType() == ParserTreeConstants.JJTGTNODE) {
                    Text fName = new Text(child.getFieldName());
                    if (rangeMap.containsKey(fName)) {
                        RangeBounds rb = rangeMap.get(fName);
                        if (rb.getLower() != null) {
                            log.warn("testBoundedRangeExistence, two Lower bounds exist for bounded range.");
                            if (node.getType() == ParserTreeConstants.JJTANDNODE) {
                                // set lower to the maximum
                                if (child.getFieldValue().compareTo(rb.getLower().toString()) > 0) {
                                    rb.setLower(new Text(child.getFieldValue()));
                                }
                            } else {
                                // set lower to the minimum
                                if (child.getFieldValue().compareTo(rb.getLower().toString()) < 0) {
                                    rb.setLower(new Text(child.getFieldValue()));
                                }
                            }
                        } else {
                            rb.setLower(new Text(child.getFieldValue()));
                        }
                    } else {
                        RangeBounds rb = new RangeBounds();
                        rb.setLower(new Text(child.getFieldValue()));
                        rangeMap.put(new Text(child.getFieldName()), rb);
                    }
                }
            }
            
            Iterator<Entry<Text,RangeBounds>> entryIterator = rangeMap.entrySet().iterator();
            while (entryIterator.hasNext()) {
                Entry<Text,RangeBounds> entry = entryIterator.next();
                RangeBounds rb = entry.getValue();
                if (rb.getLower() == null || rb.getUpper() == null) {
                    // unbounded range, remove
                    if (log.isDebugEnabled()) {
                        log.debug("testBoundedRangeExistence: Unbounded Range detected, removing entry from rangeMap");
                    }
                    entryIterator.remove(); // Avoid concurrent modification exception
                }
            }
            if (!rangeMap.isEmpty()) {
                return rangeMap;
            }
        }
        
        return null;
    }
    
    /**
     * Recursive function. Given a DatawaveTreeNode containing the parse tree of the query, rebuild it and return it as a string. The returned query should be
     * parsable by the DatawaveQueryParser.
     *
     * To handle the query, there are a number of things which need to be removed.
     *
     * 1. Non-Indexed terms need to be removed. 2. Ranges need to be turned into an discrete set or removed (although technically boolean logic could create the
     * set if necessary) 3. Regex should be turned into a discrete set or removed. 4. Function nodes should be removed. 5. Terms should be normalized 6.
     * Normalized terms should be OR'd with each other.
     *
     * @param node
     *            Root of the query parse tree
     * @return rebuild and return the query string from the parse tree.
     * 
     */
    public String rebuildQueryFromTree(DatawaveTreeNode node) {
        String query = "";
        if (node.isLeaf()) {
            
            String operator = node.getOperator();
            
            if (node.isNegated()) {
                if (node.getType() == JexlOperatorConstants.JJTEQNODE) {
                    operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTNENODE);
                } else if (node.getType() == JexlOperatorConstants.JJTERNODE) {
                    operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTNRNODE);
                } else if (node.getType() == JexlOperatorConstants.JJTLTNODE) {
                    operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTGENODE);
                } else if (node.getType() == JexlOperatorConstants.JJTLENODE) {
                    operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTGTNODE);
                } else if (node.getType() == JexlOperatorConstants.JJTGTNODE) {
                    operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTLENODE);
                } else if (node.getType() == JexlOperatorConstants.JJTGENODE) {
                    operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTLTNODE);
                } else if (node.getType() == JexlOperatorConstants.JJTAMBIGUOUS) {
                    operator = "!";
                }
            }
            
            if (node.getType() == ParserTreeConstants.JJTFUNCTIONNODE) {
                StringBuilder sb = new StringBuilder(query);
                if (node.isNegated()) {
                    log.debug("function node, negated");
                    sb.append("! ");
                }
                sb.append(node.getFunctionNamespace()).append(":").append(node.getFunctionName()).append("(");
                String quote;
                for (JexlNode jex : node.getFunctionArgs()) {
                    log.debug("rebuildQueryFromTree, jex type: " + jex.getClass());
                    if (jex.getClass() == ASTStringLiteral.class) {
                        quote = "'";
                    } else {
                        quote = "";
                    }
                    sb.append(quote).append(jex.image).append(quote).append(",");
                }
                sb.setCharAt(sb.length() - 1, ')'); // swap the trailing comma with a closing parenthesis
                query = sb.toString();
            } else {
                if (node.getFieldName() == null) {
                    node.setFieldName("");
                }
                
                if (operator == null) {
                    operator = "";
                } else {
                    operator = " " + operator + " ";
                }
                
                // Collapsed range node type.
                if (node.getType() == ParserTreeConstants.JJTLENODE && node.getLowerBound() != null && !node.getLowerBound().isEmpty()) {
                    // rebuild entire range
                    StringBuilder sb = new StringBuilder();
                    if (node.isNegated()) {
                        sb.append("!");
                    }
                    
                    sb.append("(").append(node.getFieldNameSmartQuote()).append(" ").append(node.getRangeUpperOp()).append(" '").append(node.getUpperBound())
                                    .append("'");
                    sb.append(" and ");
                    sb.append(node.getFieldNameSmartQuote()).append(" ").append(node.getRangeLowerOp()).append(" '").append(node.getLowerBound()).append("' )");
                    query = sb.toString();
                    
                } else if (node.getFieldValueLiteralType() == ASTStringLiteral.class || node.getFieldValueLiteralType() == null) {
                    if (node.getType() == ParserTreeConstants.JJTERNODE || node.getType() == ParserTreeConstants.JJTNRNODE) {
                        // this process will eat the extra escaping on escaped literal backslashes, we should preserve it for when we rebuild the query
                        node.setFieldValue(node.getFieldValue().replaceAll("\\\\\\\\", "\\\\\\\\\\\\\\\\"));
                    }
                    String quote = "'";
                    if (node.getFieldNameSmartQuote().equals("") && node.isNegated()) {
                        query = operator + quote + escapeQuote(node.getFieldValue()) + quote;
                    } else {
                        query = node.getFieldNameSmartQuote() + operator + quote + escapeQuote(node.getFieldValue()) + quote;
                    }
                } else {
                    query = node.getFieldNameSmartQuote() + operator + node.getFieldValue();
                }
            }
            
        } else {
            List<String> parts = new ArrayList<>();
            @SuppressWarnings("unchecked")
            Enumeration<DatawaveTreeNode> children = node.children();
            while (children.hasMoreElements()) {
                DatawaveTreeNode child = children.nextElement();
                parts.add(rebuildQueryFromTree(child));
            }
            if (node.getType() == ParserTreeConstants.JJTJEXLSCRIPT) {
                StringBuilder sb = new StringBuilder();
                if (!node.getScript().isEmpty()) {
                    sb.append(node.getScript());
                }
                sb.append(org.apache.commons.lang.StringUtils.join(parts, ""));
                return sb.toString();
            }
            String op = " " + JexlOperatorConstants.getOperator(node.getType()) + " ";
            if (log.isDebugEnabled()) {
                log.debug("Operator: " + op);
            }
            query = org.apache.commons.lang.StringUtils.join(parts, op);
            if (node.isNegated()) {
                query = "! (" + query + ")";
            } else {
                query = "(" + query + ")";
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("rebuildQueryFromTree, returning query:  " + query);
        }
        return query;
    }
    
    /**
     * Get the mapping of FieldNames to TreeNodes. i.e. COLOR -> maps to all nodes with COLOR as the FieldName. This method does not return any nodes which have
     * a null term/literal
     *
     * @return Multimap of FieldName -> [DatawaveTreeNode,DatawaveTreeNode,...]
     */
    public Multimap<String,DatawaveTreeNode> getFieldNameToNodeMap(DatawaveTreeNode root) {
        Multimap<String,DatawaveTreeNode> fieldNameToNodeMap = HashMultimap.create();
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode node;
        while (bfe.hasMoreElements()) {
            node = bfe.nextElement();
            
            if (!node.isLeaf()) {
                continue;
            }
            
            if (node.isFunctionNode()) {
                continue;
            }
            
            if (node.getFieldValueLiteralType() != ASTNullLiteral.class) {
                fieldNameToNodeMap.put(node.getFieldName(), node);
            }
        }
        return fieldNameToNodeMap;
    }
    
    /**
     * Get the mapping of FieldNames to TreeNodes. i.e. COLOR -> maps to all nodes with COLOR as the FieldName. This method does not return any nodes which have
     * a null term/literal
     *
     * @return Multimap of FieldName -> [DatawaveTreeNode,DatawaveTreeNode,...]
     */
    public Multimap<String,DatawaveTreeNode> getFieldNameToNodeMapWithFunctionsAndNullLiterals(DatawaveTreeNode root, Metadata metadata) {
        Multimap<String,DatawaveTreeNode> fieldNameToNodeMap = HashMultimap.create();
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode node;
        while (bfe.hasMoreElements()) {
            node = bfe.nextElement();
            
            if (!node.isLeaf()) {
                continue;
            }
            
            if (node.isFunctionNode()) {
                JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
                if (desc != null) {
                    JexlArgument[] args = desc.getArgumentsWithFieldNames(metadata);
                    for (JexlArgument arg : args) {
                        if (arg.getFieldNames() != null) {
                            for (String fieldname : arg.getFieldNames()) {
                                fieldNameToNodeMap.put(fieldname, node);
                            }
                        }
                    }
                }
            } else {
                fieldNameToNodeMap.put(node.getFieldName(), node);
            }
        }
        return fieldNameToNodeMap;
    }
    
    /**
     * Get the TreeNodes for a specified field name
     *
     * @return Set of DatawaveTreeNode
     */
    public Set<DatawaveTreeNode> getNodesForField(DatawaveTreeNode root, String fieldName) {
        Set<DatawaveTreeNode> nodes = new HashSet<>();
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode node;
        while (bfe.hasMoreElements()) {
            node = bfe.nextElement();
            
            if (!node.isLeaf()) {
                continue;
            }
            
            if (node.isFunctionNode()) {
                continue;
            }
            
            if (node.getFieldValueLiteralType() != ASTNullLiteral.class && node.getFieldName().equals(fieldName)) {
                nodes.add(node);
            }
        }
        return nodes;
    }
    
    /**
     * Get the mapping of FieldName to FieldValues. i.e. given the FieldName COLOR, you get all of the colors that maps to, red, blue, white, ...
     *
     * @return Multimap<String,String> of FieldName to FieldValue
     */
    public Multimap<String,String> getFieldNameToValueMap(DatawaveTreeNode root) {
        Multimap<String,String> fieldNameToValueMap = HashMultimap.create();
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode node;
        while (bfe.hasMoreElements()) {
            node = bfe.nextElement();
            
            if (!node.isLeaf()) {
                continue;
            }
            
            if (node.isFunctionNode()) {
                continue;
            }
            
            if (node.getFieldValueLiteralType() != ASTNullLiteral.class) {
                fieldNameToValueMap.put(node.getFieldName(), node.getFieldValue());
            }
            
        }
        return fieldNameToValueMap;
    }
    
    /**
     * Get a Set of DatawaveTreeNodes which contain JEXL functions i.e. f:function(X,'b','c')
     *
     * @return Set of nodes which contain functions
     */
    public Set<DatawaveTreeNode> getFunctionSet(DatawaveTreeNode root) {
        Set<DatawaveTreeNode> functionSet = new HashSet<>();
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode node;
        while (bfe.hasMoreElements()) {
            node = bfe.nextElement();
            
            if (!node.isLeaf()) {
                continue;
            }
            
            if (node.isFunctionNode()) {
                functionSet.add(node);
            }
        }
        return functionSet;
    }
    
    /**
     * Get a Set of DatawaveTreeNodes which had an ASTNullLiteral: null as their FieldValue in the query. i.e. COLOR == null
     *
     * @return Set of nodes which have null FieldValues
     * 
     */
    public Set<DatawaveTreeNode> getNullValueSet(DatawaveTreeNode root) {
        Set<DatawaveTreeNode> nullValueSet = new HashSet<>();
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode node;
        while (bfe.hasMoreElements()) {
            node = bfe.nextElement();
            
            if (!node.isLeaf()) {
                continue;
            }
            
            if (node.isFunctionNode()) {
                continue;
            }
            
            if (node.getFieldValueLiteralType() == ASTNullLiteral.class) {
                nullValueSet.add(node);
            }
            
        }
        return nullValueSet;
    }
    
    /**
     * Pass in the root of your query parse tree, and a list of FieldNames to skip during JEXL context evaluation. This will modify the query and return the
     * root of the parse tree for the query you should pass to the evaluator.
     *
     * Internally, it replaces the FieldValue to a Null type.
     *
     * @param root
     *            The Root of the query parse tree
     * @param unevalFieldNames
     *            List of FieldNames which should not be evaluated in JEXL context
     * @return root of the modified query parse tree.
     * 
     */
    public DatawaveTreeNode replaceUnevaluatedExpressions(DatawaveTreeNode root, Set<String> unevalFieldNames) {
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode node;
        while (bfe.hasMoreElements()) {
            node = bfe.nextElement();
            if (unevalFieldNames.contains(node.getFieldName())) {
                if (log.isTraceEnabled()) {
                    log.trace("uneval contains: " + node.getFieldName());
                }
                node.setFieldValueLiteralType(ASTNullLiteral.class);
                node.setFieldValue(null);
                node.setOperator(JexlOperatorConstants.getOperator(ParserTreeConstants.JJTEQNODE)); // set it
                                                                                                    // to ==
                                                                                                    // regardless
                node.setNegated(false);
            }
        }
        return root;
    }
    
    /**
     * Pass in the root node of the query parse tree and a Multimap of FieldName-> [Normalizers]. The tree will be traversed and a new mapping of
     * FieldName:FieldValue -> normalized fieldValue will be created. This is passed to an ORing function which will replace each node with the normalized value
     * or an or of multiple normalized values.
     *
     * The internal query maps will be updated and the root of the modified parse tree will be returned.
     *
     * @param root
     *            The root DatawaveTreeNode of the query parse tree
     * @param indexedFields
     *            Multimap of FieldName -> [Normalizers]
     * @return returns the root DatawaveTreeNode of the query parse tree after the query has been modified.
     * @throws Exception
     * 
     */
    public DatawaveTreeNode normalizeFieldsInQuery(DatawaveTreeNode root, Multimap<String,Type<?>> indexedFields, Metadata metadata) {
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode node;
        while (bfe.hasMoreElements()) {
            node = bfe.nextElement();
            if (node.isLeaf()) {
                if (node.isFunctionNode()) {
                    JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
                    if (desc != null) {
                        JexlArgument[] args = desc.getArgumentsWithFieldNames(metadata);
                        Multimap<Integer,String> variants = HashMultimap.create();
                        for (int i = 0; i < args.length; i++) {
                            JexlArgument arg = args[i];
                            // only normalize string literals that is a value or a regex
                            if ((arg.getArgumentType() == JexlArgumentType.VALUE) || (arg.getArgumentType() == JexlArgumentType.REGEX)) {
                                if (!(arg.getJexlArgumentNode() instanceof ASTStringLiteral)) {
                                    if (arg.getJexlArgumentNode() instanceof ASTNumberLiteral) {
                                        // not normalizing numeric arguments
                                    } else {
                                        throw new RuntimeException("Unexpected argument type for a value or regex: " + arg.getJexlArgumentNode().getClass());
                                    }
                                } else {
                                    for (String fieldName : arg.getFieldNames()) {
                                        for (Type<?> tn : indexedFields.get(fieldName)) {
                                            String value = node.getFunctionArgs().get(i).image;
                                            if (value != null) {
                                                try {
                                                    if (arg.getArgumentType() == JexlArgumentType.REGEX) {
                                                        value = tn.normalizeRegex(value);
                                                    } else {
                                                        value = tn.normalize(value);
                                                    }
                                                    if (null == value || value.isEmpty()) {
                                                        if (log.isTraceEnabled()) {
                                                            log.trace("normalizer returned empty or null, do not push into tree");
                                                        }
                                                    } else {
                                                        variants.put(i, value);
                                                    }
                                                } catch (Exception ne) {
                                                    if (log.isDebugEnabled()) {
                                                        log.debug("normalizer " + tn.getClass() + " failed to normalize " + node.getFieldValue() + ": ignoring",
                                                                        ne);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        List<DatawaveTreeNode> nodes = new ArrayList<>();
                        nodes.add(node);
                        for (Integer argIndex : variants.keySet()) {
                            Set<String> values = new HashSet<>(variants.get(argIndex));
                            if (values.size() == 1) {
                                for (DatawaveTreeNode n : nodes) {
                                    n.getFunctionArgs().get(argIndex).image = values.iterator().next();
                                }
                            } else {
                                // make an appropriate set of node copies
                                List<DatawaveTreeNode> orgNodes = nodes;
                                nodes = new ArrayList<>();
                                for (DatawaveTreeNode orgNode : orgNodes) {
                                    for (String value : values) {
                                        DatawaveTreeNode copy = this.copyNode(orgNode);
                                        ASTStringLiteral argCopy = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
                                        argCopy.image = value;
                                        copy.getFunctionArgs().set(argIndex, argCopy);
                                        nodes.add(copy);
                                    }
                                }
                            }
                        }
                        if (nodes.size() == 1) {
                            // in this case the variant has already been made in the root node
                            node.setFieldValueLiteralType(ASTStringLiteral.class);
                        } else if (nodes.size() > 1) {
                            node.setType(ParserTreeConstants.JJTORNODE);
                            node.setFieldName(null);
                            node.setFieldValue(null);
                            for (DatawaveTreeNode n : nodes) {
                                n.setFieldValueLiteralType(ASTStringLiteral.class);
                                node.add(n);
                            }
                        }
                    }
                } else if ((node.getFieldName() != null) && (indexedFields.containsKey(node.getFieldName()))) {
                    if (log.isDebugEnabled()) {
                        log.debug("attempting to normalize node: " + node.getContents());
                        log.debug("node.isRangeNode(): " + node.isRangeNode());
                        log.debug("node.type-> " + node.getType());
                    }
                    
                    // get the normalizer name
                    if (node.isRangeNode()) {
                        Set<Pair<String,String>> variants = new HashSet<>();
                        for (Type<?> tn : indexedFields.get(node.getFieldName())) {
                            try {
                                Pair<String,String> bounds = new Pair<>(tn.normalize(node.getLowerBound()), tn.normalize(node.getUpperBound()));
                                variants.add(bounds);
                                if (log.isDebugEnabled()) {
                                    log.debug("Normalized range for " + node.getFieldName() + " using " + tn.getClass() + ": " + bounds);
                                }
                            } catch (Exception ne) {
                                if (log.isDebugEnabled()) {
                                    log.debug("normalizer " + tn.getClass() + " failed to normalize " + node.getLowerBound() + " and/or "
                                                    + node.getUpperBound() + ": ignoring", ne);
                                }
                            }
                        }
                        if (variants.size() == 1) {
                            // normalizer, update upper and lower bounds of the range node.
                            Pair<String,String> bounds = variants.iterator().next();
                            node.setLowerBound(bounds.getFirst());
                            node.setUpperBound(bounds.getSecond());
                            node.setFieldValueLiteralType(ASTStringLiteral.class);
                        } else if (variants.size() > 1) {
                            node.setType(ParserTreeConstants.JJTORNODE);
                            for (Pair<String,String> bounds : variants) {
                                DatawaveTreeNode nu = this.copyNode(node); // copy the previous node, then modify it
                                nu.setType(ParserTreeConstants.JJTLENODE);
                                nu.setRangeNode(true);
                                nu.setLowerBound(bounds.getFirst());
                                nu.setUpperBound(bounds.getSecond());
                                nu.setFieldValueLiteralType(ASTStringLiteral.class);
                                node.add(nu);
                            }
                            node.setRangeNode(false);
                            node.setFieldName(null);
                            node.setFieldValue(null);
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Normalized node: " + node.getContents());
                        }
                        
                    } else if (node.getFieldValueLiteralType() != ASTNullLiteral.class) {
                        Set<String> variants = new HashSet<>();
                        for (Type<?> tn : indexedFields.get(node.getFieldName())) {
                            String value = null;
                            try {
                                if ((node.getType() == ParserTreeConstants.JJTERNODE) || (node.getType() == ParserTreeConstants.JJTNRNODE)) {
                                    value = tn.normalizeRegex(node.getFieldValue());
                                } else {
                                    value = tn.normalize(node.getFieldValue());
                                }
                                if (log.isTraceEnabled()) {
                                    log.trace("normalier: " + tn + "  normalized value: " + value);
                                }
                                if (null == value || value.isEmpty()) {
                                    if (log.isTraceEnabled()) {
                                        log.trace("normalizer returned empty or null, do not push into tree");
                                    }
                                } else {
                                    variants.add(value);
                                }
                            } catch (Exception ne) {
                                if (log.isDebugEnabled()) {
                                    log.debug("normalizer " + tn.getClass() + " failed to normalize " + node.getFieldValue() + ": ignoring", ne);
                                }
                            }
                        }
                        if (variants.size() == 1) {
                            String value = variants.iterator().next();
                            node.setFieldValue(value);
                            node.setFieldValueLiteralType(ASTStringLiteral.class);
                        } else if (variants.size() > 1) {
                            for (String value : variants) {
                                DatawaveTreeNode n = copyNode(node);
                                n.setFieldValue(value);
                                n.setFieldValueLiteralType(ASTStringLiteral.class);
                                node.add(n);
                            }
                            node.setType(ParserTreeConstants.JJTORNODE);
                            node.setFieldName(null);
                            node.setFieldValue(null);
                        }
                    }
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Normalized Tree: " + root.getContents());
        }
        return root;
    }
    
    /**
     * Analyze the query and remove expressions that will always evaluate to true (or false) because the field does not exist.
     *
     * @param root
     *            The root DatawaveTreeNode of the query parse tree
     * @param metadataHelper
     * @return returns the root DatawaveTreeNode of the query parse tree after the query has been modified.
     * @throws InvalidQueryException
     *             thrown if this query becomes empty after pruning the missing fields
     * 
     */
    public DatawaveTreeNode pruneMissingFields(DatawaveTreeNode root, GenericShardQueryConfiguration config, MetadataHelper metadataHelper)
                    throws InvalidQueryException {
        Set<String> allFields;
        try {
            allFields = Sets.newHashSet(metadataHelper.getAllFields(config.getDatatypeFilter()));
            if (config.getIncludeDataTypeAsField()) {
                allFields.add(EvaluatingIterator.EVENT_DATATYPE);
            }
            @SuppressWarnings("unchecked")
            List<DatawaveTreeNode> nodes = Collections.list(root.breadthFirstEnumeration());
            Set<String> removedFields = new HashSet<>();
            
            for (DatawaveTreeNode node : nodes) {
                boolean dropNode = false;
                boolean value = false;
                if (node.isFunctionNode()) {
                    JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
                    if (desc != null) {
                        // get the index query equivalent
                        DatawaveTreeNode indexQuery = desc.getIndexQuery(metadataHelper.getMetadata());
                        // if we got an index query other than the function node itself
                        if (indexQuery != node) {
                            // try to prune the index query itself
                            try {
                                indexQuery = pruneMissingFields(indexQuery, config, metadataHelper);
                            } catch (InvalidQueryException e) {
                                dropNode = true;
                            }
                        }
                    }
                } else if (node.getFieldName() != null) {
                    
                    if (!allFields.contains(node.getFieldName())) {
                        
                        dropNode = true;
                        removedFields.add(node.getFieldName());
                        // reverse the value if a NE or NR node
                        if (node.getType() == ParserTreeConstants.JJTNENODE || node.getType() == ParserTreeConstants.JJTNRNODE) {
                            value = !value;
                        }
                        // if this node is actually comparing to null, then reverse the value
                        if (node.getFieldValueLiteralType() == ASTNullLiteral.class) {
                            value = !value;
                        }
                    }
                }
                
                if (dropNode) {
                    DatawaveTreeNode newRoot = dropNode(root, node, value);
                    // if a new root was returned, then we have a query that always evaluates to true or false
                    if (newRoot != null && newRoot != root) {
                        QueryException qe = new QueryException(DatawaveErrorCode.NO_TERMS_REMAIN_AFTER_NONEXISTENT_REMOVAL, MessageFormat.format(
                                        "Removed: {0}", removedFields));
                        throw new InvalidQueryException(qe);
                    }
                }
            }
        } catch (TableNotFoundException e) {
            throw new IllegalStateException("Cannot query non-existent metadata table", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Error fetching metadata table information", e);
        } catch (MarkingFunctions.Exception e) {
            throw new IllegalStateException("Error fetching metadata table information", e);
        }
        return root;
    }
    
    /**
     * Drop a node from a query, pruning the tree as needed. If the value of the dropped node makes the parent's value known, then the parent is pruned as
     * appropriate.
     *
     * @param root
     *            The root node of the query
     * @param node
     *            The node being dropped
     * @param value
     *            The value of the node being dropped PRIOR to applying the negation flag (i.e. true if the node would always evaluate to with isNegated() ==
     *            false)
     * @return root: if the node was dropped in a sub-expression null: if the entire tree was dropped as a result, but the top level parent was not the same as
     *         root JJTTRUENODE: if the entire tree was dropped up to root, and the tree would always evaluate to true JJTFALSENODE: if the entire tree was
     *         dropped up to root, and the tree would always evaluate to false
     */
    public DatawaveTreeNode dropNode(DatawaveTreeNode root, DatawaveTreeNode node, boolean value) {
        DatawaveTreeNode parent = (DatawaveTreeNode) node.getParent();
        if (parent != null) {
            // reverse the value if negated
            if (node.isNegated()) {
                value = !value;
            }
            // if the value is true
            if (value) {
                // if contained in an OR, then drop the OR clause
                if (parent.getType() == ParserTreeConstants.JJTORNODE) {
                    return dropNode(root, parent, value);
                }
                // if contained in anything else (and or script), then simply drop this node
                else {
                    if (parent.getChildCount() == 1) {
                        return dropNode(root, parent, value);
                    } else {
                        parent.remove(node);
                        if (parent.getChildCount() == 1) {
                            DatawaveTreeNode parentParent = (DatawaveTreeNode) parent.getParent();
                            if (parentParent != null) {
                                DatawaveTreeNode child = (DatawaveTreeNode) parent.getChildAt(0);
                                if (parent.isNegated()) {
                                    child.setNegated(!child.isNegated());
                                }
                                parentParent.remove(parent);
                                parentParent.add(child);
                            }
                        }
                        return root;
                    }
                }
            } else {
                // if contained in an OR, then drop the node of the OR clause
                if (parent.getType() == ParserTreeConstants.JJTORNODE) {
                    // however if only one child, then drop the OR
                    if (parent.getChildCount() == 1) {
                        return dropNode(root, parent, value);
                    } else {
                        parent.remove(node);
                        if (parent.getChildCount() == 1) {
                            DatawaveTreeNode parentParent = (DatawaveTreeNode) parent.getParent();
                            if (parentParent != null) {
                                DatawaveTreeNode child = (DatawaveTreeNode) parent.getChildAt(0);
                                if (parent.isNegated()) {
                                    child.setNegated(!child.isNegated());
                                }
                                parentParent.remove(parent);
                                parentParent.add(child);
                            }
                        }
                        return root;
                    }
                }
                // if contained in anything else (and or script, then simply drop the parent
                else {
                    return dropNode(root, parent, value);
                }
            }
        } else {
            if (node == root) {
                return new DatawaveTreeNode(value ? ParserTreeConstants.JJTTRUENODE : ParserTreeConstants.JJTFALSENODE);
            } else {
                return null;
            }
        }
    }
    
    /**
     * Drop a node from a query when the value of the node being dropped is unknown.
     *
     * @param root
     *            The root node of the query
     * @param node
     *            The node being dropped
     * @return root: if the node was dropped in a sub-expression null: if the entire tree was dropped as a result, but the top level parent was not the same as
     *         root JJTTRUENODE: if the entire tree was dropped up to root
     */
    public DatawaveTreeNode dropNode(DatawaveTreeNode root, DatawaveTreeNode node) {
        DatawaveTreeNode parent = (DatawaveTreeNode) node.getParent();
        // determine the value of the node that will NOT drop the parent as a result
        boolean value = true;
        if (parent != null) {
            if (((DatawaveTreeNode) node.getParent()).getType() == ParserTreeConstants.JJTORNODE) {
                value = false;
            }
        }
        if (node.isNegated()) {
            value = !value;
        }
        return dropNode(root, node, value);
    }
    
    /**
     * This method will create a deep copy of the query parse tree that you pass it.
     *
     * @param originalRoot
     *            the root of the parse tree of your query.
     * @return Receive the root of a new copy of the tree you passed in.
     * 
     */
    public DatawaveTreeNode copyTree(DatawaveTreeNode originalRoot) {
        
        if (originalRoot.getChildCount() == 0) {
            return copyNode(originalRoot);
        }
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> children = originalRoot.children();
        DatawaveTreeNode child;
        DatawaveTreeNode copy = copyNode(originalRoot);
        while (children.hasMoreElements()) {
            child = children.nextElement();
            copy.add(copyTree((child)));
        }
        return copy;
    }
    
    private DatawaveTreeNode copyNode(DatawaveTreeNode node) {
        DatawaveTreeNode copy = new DatawaveTreeNode(node.getType());
        copy.setFieldName(node.getFieldName());
        copy.setFieldValue(node.getFieldValue());
        copy.setFieldValueLiteralType(node.getFieldValueLiteralType());
        copy.setOperator(node.getOperator());
        copy.setCardinality(node.getCardinality());
        copy.setNegated(node.isNegated());
        copy.setChildrenAllNegated(node.isChildrenAllNegated());
        copy.setInfiniteRange(node.isInfiniteRange());
        copy.setOptimized(node.isOptimized());
        copy.setRemoval(node.isRemoval());
        copy.setUnboundedRange(node.isUnboundedRange());
        copy.setIndexOnlyField(node.isIndexOnlyField());
        
        if (node.getRanges() != null) {
            copy.setRanges(new HashSet<>(node.getRanges()));
        }
        
        if (node.isFunctionNode()) {
            copy.setFunctionNode(node.isFunctionNode());
            copy.setFunctionNamespace(node.getFunctionNamespace());
            copy.setFunctionName(node.getFunctionName());
            copy.setFunctionClass(node.getFunctionClass());
            copy.setSpecialHandling(node.isSpecialHandling());
            copy.setFunctionArgs(new ArrayList<>(node.getFunctionArgs()));
        }
        
        if (node.isRangeNode()) {
            copy.setRangeNode(true);
            copy.setUpperBound(node.getUpperBound());
            copy.setLowerBound(node.getLowerBound());
            copy.setRangeUpperOp(node.getRangeUpperOp());
            copy.setRangeLowerOp(node.getRangeLowerOp());
        }
        
        return copy;
    }
    
    /**
     * Boolean Logic iterator cannot handle query terms with a null value. Its job is to find something, not the absence of something. I'm not sure it is a good
     * idea to remove these pieces, it may cause unintended consequences in the query.
     *
     * @param root
     *            Root node of the query parse tree
     * @return root of the modified parse tree.
     * @throws InvalidQueryException
     */
    public DatawaveTreeNode removeNullValueNodes(DatawaveTreeNode root) throws InvalidQueryException {
        
        // NOTE: doing a depth first enumeration didn't work when I started
        // removing nodes halfway through. The following method does work,
        // it's essentially a reverse breadth first traversal.
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> nodes = Collections.list(root.breadthFirstEnumeration());
        
        // walk backwards
        for (int i = nodes.size() - 1; i >= 0; i--) {
            DatawaveTreeNode node = nodes.get(i);
            if (log.isDebugEnabled()) {
                log.debug("removeNullValueNodes, Testing: " + node);
            }
            
            if (node.isLeaf() && node.getFieldValueLiteralType() == ASTNullLiteral.class) {
                log.debug("Removing null literal node: " + node);
                DatawaveTreeNode newRoot = dropNode(root, node);
                if (newRoot != null && newRoot != root) {
                    QueryException qe = new QueryException(DatawaveErrorCode.EMPTY_QUERY_AFTER_NULL_VALUE_NODE_REMOVAL);
                    throw new InvalidQueryException(qe);
                }
            }
        }
        
        return root;
    }
    
    /**
     * This function removes JEXL function nodes from a parsed query.
     *
     * @param root
     *            Root node of the query parser tree
     * @return root of the modified parse tree.
     * @throws InvalidQueryException
     */
    public DatawaveTreeNode removeFunctionNodes(DatawaveTreeNode root) throws InvalidQueryException {
        
        // NOTE: doing a depth first enumeration didn't work when I started
        // removing nodes halfway through. The following method does work,
        // it's essentially a reverse breadth first traversal.
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> nodes = Collections.list(root.breadthFirstEnumeration());
        
        // walk backwards
        for (int i = nodes.size() - 1; i >= 0; i--) {
            DatawaveTreeNode node = nodes.get(i);
            if (log.isDebugEnabled()) {
                log.debug("removeFunctionNodes, Testing: " + node.getFieldName() + ":" + node.getFieldValue());
            }
            
            if (node.isFunctionNode() || node.getType() == ParserTreeConstants.JJTFUNCTIONNODE) {
                DatawaveTreeNode newRoot = dropNode(root, node);
                if (newRoot != null && newRoot != root) {
                    QueryException qe = new QueryException(DatawaveErrorCode.EMPTY_QUERY_AFTER_FUNCTION_NODE_REMOVAL);
                    throw new InvalidQueryException(qe);
                }
            }
        }
        return root;
    }
    
    /**
     * This function replaces JEXL function nodes from a parsed query with a suitable index query
     *
     * @param root
     *            Root node of the query parser tree
     * @param config
     *            Configuration
     * @return root of the modified parse tree.
     */
    public DatawaveTreeNode replaceFunctionNodes(DatawaveTreeNode root, MetadataHelper metadataHelper) {
        
        // NOTE: doing a depth first enumeration didn't work when I started
        // removing nodes halfway through. The following method does work,
        // it's essentially a reverse breadth first traversal.
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> nodes = Collections.list(root.breadthFirstEnumeration());
        
        // walk backwards
        for (int i = nodes.size() - 1; i >= 0; i--) {
            DatawaveTreeNode node = nodes.get(i);
            if (log.isDebugEnabled()) {
                log.debug("removeFunctionNodes, Testing: " + node.getFieldName() + ":" + node.getFieldValue());
            }
            
            if (node.isFunctionNode() || node.getType() == ParserTreeConstants.JJTFUNCTIONNODE) {
                // Replace the function node with an equivalent
                // index query node
                
                // remember the parent and remove this node
                DatawaveTreeNode p = (DatawaveTreeNode) node.getParent();
                
                // get the descriptor
                try {
                    JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
                    if (desc != null) {
                        
                        // get the index query corresponding to this function
                        DatawaveTreeNode indexNode = desc.getIndexQuery(metadataHelper.getMetadata());
                        if (indexNode != null) {
                            // replace the function node with the index query
                            node.removeFromParent();
                            p.add(indexNode);
                        }
                    }
                } catch (Exception e) {
                    log.error("Cannot get or invoke JexlFunctionArgumentDescriptorFactory for " + node.getFunctionClass(), e);
                    throw new IllegalStateException("Cannot get or invoke JexlFunctionArgumentDescriptorFactory for " + node.getFunctionClass(), e);
                }
            }
        }
        return root;
    }
    
    /**
     * This function removes JEXL function nodes from a parsed query.
     *
     * @param root
     *            Root node of the query parser tree
     * @return root of the modified parse tree.
     * @throws InvalidQueryException
     *             if nothing left after removing "Ngram" regex expressions
     * @throws JavaRegexParseException
     */
    public DatawaveTreeNode removeUnhandledRegexNodes(DatawaveTreeNode root) throws InvalidQueryException, JavaRegexParseException {
        @SuppressWarnings("unchecked")
        List<DatawaveTreeNode> nodes = Collections.list(root.breadthFirstEnumeration());
        Set<String> removedExpressions = new HashSet<>();
        
        // walk backwards
        for (int i = nodes.size() - 1; i >= 0; i--) {
            DatawaveTreeNode node = nodes.get(i);
            if (log.isDebugEnabled()) {
                log.debug("removeUnhandledRegexNodes, Testing: " + node.getFieldName() + ":" + node.getFieldValue());
            }
            
            if (node.getType() == ParserTreeConstants.JJTERNODE) {
                if (new JavaRegexAnalyzer(node.getFieldValue()).isNgram()) {
                    removedExpressions.add(node.getFieldValue());
                    DatawaveTreeNode newRoot = dropNode(root, node);
                    if (newRoot != null && newRoot != root) {
                        QueryException qe = new QueryException(DatawaveErrorCode.EMPTY_QUERY_AFTER_POST_INDEX_REGEX_REMOVAL, MessageFormat.format(
                                        "Removed: {0}", removedExpressions));
                        throw new InvalidQueryException(qe);
                    }
                }
            }
        }
        
        return root;
    }
    
    /**
     * Given a mapping of FieldName -> [multiple FieldNames] update them in the query. For single mappings, change the field name, for multiple insert an or.
     * NOTE: It is expected that negations are pushed down to the bottom as with the demorganSubTree routine
     *
     * @param root
     * @param fieldMap
     * @return
     */
    public DatawaveTreeNode applyFieldMapping(DatawaveTreeNode root, Multimap<String,String> fieldMap, MetadataHelper metadataHelper) {
        if (log.isTraceEnabled()) {
            log.trace("applyFieldMapping: " + fieldMap);
        }
        if (!fieldMap.isEmpty()) {
            @SuppressWarnings("unchecked")
            List<DatawaveTreeNode> nodes = Collections.list(root.breadthFirstEnumeration());
            
            DatawaveTreeNode node;
            // walk the tree backwards and update.
            for (int i = nodes.size() - 1; i >= 0; i--) {
                node = nodes.get(i);
                
                if (!node.isLeaf()) {
                    continue;
                }
                
                if (node.isFunctionNode()) {
                    // get the descriptor
                    try {
                        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
                        if (desc != null) {
                            JexlArgument[] args = desc.getArguments();
                            List<DatawaveTreeNode> functionNodes = applyFieldMappingToFunction(node, fieldMap, args, 0);
                            if (functionNodes.size() == 1) {
                                node.setFunctionArgs(functionNodes.get(0).getFunctionArgs());
                            } else {
                                boolean negated = node.isNegated();
                                for (DatawaveTreeNode functionNode : functionNodes) {
                                    functionNode.setNegated(negated);
                                    node.add(functionNode);
                                }
                                if (negated) {
                                    node.setType(ParserTreeConstants.JJTANDNODE);
                                } else {
                                    node.setType(ParserTreeConstants.JJTORNODE);
                                }
                                node.setChildrenAllNegated(negated);
                                node.setNegated(false);
                                node.setFieldName(null);
                                node.setFieldValue(null);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Cannot get or invoke JexlFunctionArgumentDescriptorFactory for " + node.getFunctionClass(), e);
                        throw new IllegalArgumentException("Cannot get or invoke JexlFunctionArgumentDescriptorFactory for " + node.getFunctionClass(), e);
                    }
                } else if (node.getFieldName() != null && fieldMap.containsKey(node.getFieldName())) {
                    Collection<String> fNames = fieldMap.get(node.getFieldName());
                    if (fNames.size() == 1) {
                        node.setFieldName(fNames.iterator().next());
                    } else if (fNames.size() > 1) {
                        boolean negated = node.isNegated();
                        if (node.getType() == ParserTreeConstants.JJTNENODE) {
                            negated = !negated;
                            node.setType(ParserTreeConstants.JJTEQNODE);
                        } else if (node.getType() == ParserTreeConstants.JJTNRNODE) {
                            negated = !negated;
                            node.setType(ParserTreeConstants.JJTERNODE);
                        }
                        node.setNegated(negated);
                        for (String fName : fNames) {
                            DatawaveTreeNode n = copyNode(node);
                            n.setFieldName(fName);
                            node.add(n);
                        }
                        if (negated) {
                            node.setType(ParserTreeConstants.JJTANDNODE);
                        } else {
                            node.setType(ParserTreeConstants.JJTORNODE);
                        }
                        node.setChildrenAllNegated(negated);
                        node.setNegated(false);
                        node.setFieldName(null);
                        node.setFieldValue(null);
                    }
                }
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("root after mapping: " + root.getContents());
        }
        return root;
    }
    
    /**
     * Given a mapping of FieldName -> [multiple FieldNames] update them in a function node.
     *
     * @param root
     * @param fieldMap
     * @return
     */
    public List<DatawaveTreeNode> applyFieldMappingToFunction(DatawaveTreeNode functionNode, Multimap<String,String> fieldMap, JexlArgument[] args, int index) {
        if (index == args.length) {
            return Collections.singletonList(functionNode);
        }
        List<DatawaveTreeNode> nodes = new ArrayList<>();
        if (args[index].getArgumentType().equals(JexlArgumentType.FIELD_NAME) && fieldMap.containsKey(args[index].getJexlArgumentNode().image)) {
            Collection<String> fNames = fieldMap.get(args[index].getJexlArgumentNode().image);
            for (String fname : fNames) {
                DatawaveTreeNode newNode = copyNode(functionNode);
                List<JexlNode> newArgs = new ArrayList<>(newNode.getFunctionArgs());
                JexlNode newArg = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
                newArg.image = fname;
                newArgs.set(index, newArg);
                newNode.setFunctionArgs(newArgs);
                nodes.addAll(applyFieldMappingToFunction(newNode, fieldMap, args, index + 1));
            }
        } else {
            nodes.addAll(applyFieldMappingToFunction(functionNode, fieldMap, args, index + 1));
        }
        return nodes;
    }
    
    /**
     * Container used to hold the lower and upper bound of a range
     */
    public static class RangeBounds {
        private String originalLower = null;
        private Text lower = null;
        private String originalUpper = null;
        private Text upper = null;
        
        public Text getLower() {
            return lower;
        }
        
        public Text getUpper() {
            return upper;
        }
        
        public void setLower(Text lower) {
            this.lower = lower;
        }
        
        public void setUpper(Text upper) {
            this.upper = upper;
        }
        
        public String getOriginalLower() {
            return originalLower;
        }
        
        public String getOriginalUpper() {
            return originalUpper;
        }
        
        public void setOriginalLower(String originalLower) {
            this.originalLower = originalLower;
        }
        
        public void setOriginalUpper(String originalUpper) {
            this.originalUpper = originalUpper;
        }
    }
    
    /**
     * Escape single quotes in the field value. You don't need to check for the escape \ as Jexl should have already removed it.
     *
     * @param fValue
     * @return
     */
    public String escapeQuote(String fValue) {
        if (null != fValue && !fValue.trim().isEmpty()) {
            if (fValue.contains("'")) {
                int location = fValue.indexOf("'");
                while (location > -1) {
                    fValue = (new StringBuilder(fValue)).insert(location, "\\").toString();
                    location = fValue.indexOf("'", location + 2);
                }
            }
            
            if (fValue.charAt(fValue.length() - 1) == '\\') {
                log.debug("escapeQuote: the last character is a backslash, need to convert to unicode due to JEXL bug");
                fValue = (new StringBuilder(fValue)).substring(0, fValue.length() - 1) + "\\u005C";
            }
        }
        
        return fValue;
    }
    
    /**
     * Added to address cases where field names start with a numeric character.
     *
     * @param root
     */
    public Multimap<String,DatawaveTreeNode> getSpecialHandlingNodes(DatawaveTreeNode root) {
        Multimap<String,DatawaveTreeNode> specialHandlingNodes = HashMultimap.create();
        
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        while (bfe.hasMoreElements()) {
            DatawaveTreeNode node = bfe.nextElement();
            if (node.isFunctionNode()) {
                JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
                if (desc != null) {
                    JexlArgument[] args = desc.getArguments();
                    for (JexlArgument arg : args) {
                        // if we have a field name that is not going to be replaced by a value in the context, then leave it alone
                        if (arg.getArgumentType() == JexlArgumentType.FIELD_NAME && arg.isContextReference()) {
                            String fieldname = arg.getJexlArgumentNode().image;
                            if (DatawaveTreeNode.jexlInvalidStartingChars.matcher(fieldname).matches()) {
                                node.setSpecialHandling(true); // if it was quoted from the
                                                               // start we may not have
                                                               // marked it as needing
                                                               // special handling
                                specialHandlingNodes.put(fieldname, node);
                            }
                        }
                    }
                }
                
            } else if (node.getFieldName() != null) {
                if (node.isSpecialHandling() || DatawaveTreeNode.jexlInvalidStartingChars.matcher(node.getFieldName()).matches()) {
                    node.setSpecialHandling(true); // if it was quoted from the
                                                   // start we may not have
                                                   // marked it as needing
                                                   // special handling
                    specialHandlingNodes.put(node.getFieldName(), node);
                }
            }
        }
        return specialHandlingNodes;
    }
    
    /**
     * This appends the given prefix to the fieldnames of all nodes which have been marked as needing special handling.
     *
     * @param prefix
     */
    public void fixSpecialHandlingNodes(DatawaveTreeNode root, String prefix) {
        StringBuilder sb = new StringBuilder();
        Multimap<String,DatawaveTreeNode> nodeMap = getSpecialHandlingNodes(root);
        for (String fieldName : nodeMap.keySet()) {
            sb.delete(0, sb.length());
            sb.append(prefix).append(fieldName);
            String prefixedFieldName = sb.toString();
            for (DatawaveTreeNode node : nodeMap.get(fieldName)) {
                if (node.isFunctionNode()) {
                    JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
                    if (desc != null) {
                        JexlArgument[] args = desc.getArguments();
                        for (int i = 0; i < args.length; i++) {
                            JexlArgument arg = args[i];
                            // if we have a field name that is not going to be replaced by a value in the context, then leave it alone
                            if (arg.getArgumentType() == JexlArgumentType.FIELD_NAME && arg.isContextReference()) {
                                if (arg.getJexlArgumentNode().image.equals(fieldName)) {
                                    JexlNode newArg = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
                                    newArg.image = prefixedFieldName;
                                    node.getFunctionArgs().set(i, newArg);
                                }
                            }
                        }
                    }
                } else {
                    node.setFieldName(prefixedFieldName);
                }
            }
        }
    }
    
    /**
     * This will func the nodes that have literal arguments that should be identifiers. They may have been quoted due to illegal jexl identifier values (e.g.
     * numeric fields).
     */
    public List<DatawaveTreeNode> getFunctionNodesUsingLiterals(DatawaveTreeNode root) {
        List<DatawaveTreeNode> nodes = new ArrayList<>();
        @SuppressWarnings("unchecked")
        Enumeration<DatawaveTreeNode> bfe = root.breadthFirstEnumeration();
        DatawaveTreeNode node;
        while (bfe.hasMoreElements()) {
            node = bfe.nextElement();
            if (node.isFunctionNode()) {
                JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
                if (desc != null) {
                    JexlArgument[] args = desc.getArguments();
                    boolean literalArg = false;
                    for (int i = 0; !literalArg && i < args.length; i++) {
                        JexlArgument arg = args[i];
                        JexlNode argNode = arg.getJexlArgumentNode();
                        if (arg.isContextReference()) {
                            if (!(argNode instanceof ASTIdentifier)) {
                                literalArg = true;
                                break;
                            }
                        } else if (argNode instanceof ASTIdentifier) {
                            literalArg = true;
                            break;
                        }
                    }
                    if (literalArg) {
                        nodes.add(node);
                    }
                }
            }
        }
        return nodes;
    }
    
    /**
     * This will ensure that the fieldname references in function arguments are all identifiers and subsequently will get replaced with the field values
     * appropriately upon evaluation. This will also ensure that all literals are actually literals.
     *
     * @param prefix
     */
    public void fixLiteralFunctionArgs(DatawaveTreeNode root) {
        for (DatawaveTreeNode node : getFunctionNodesUsingLiterals(root)) {
            JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
            JexlArgument[] args = desc.getArguments();
            for (int i = 0; i < args.length; i++) {
                JexlArgument arg = args[i];
                JexlNode argNode = arg.getJexlArgumentNode();
                if (arg.isContextReference()) {
                    JexlNode newArgNode = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
                    newArgNode.image = argNode.image;
                    node.getFunctionArgs().set(i, newArgNode);
                } else if (argNode instanceof ASTIdentifier) {
                    JexlNode newArgNode = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
                    newArgNode.image = argNode.image;
                    node.getFunctionArgs().set(i, newArgNode);
                }
            }
        }
    }
    
    /**
     * Given set of index only fields, iterate through provided query tree and mark them accordingly.
     * 
     * @param root
     * @param indexOnlyFields
     */
    public static void markIndexOnlyFields(DatawaveTreeNode root, Set<String> indexOnlyFields) {
        if (null == indexOnlyFields) {
            indexOnlyFields = Collections.emptySet();
        }
        Enumeration<DatawaveTreeNode> nodes = root.breadthFirstEnumeration();
        while (nodes.hasMoreElements()) {
            DatawaveTreeNode node = nodes.nextElement();
            if (node.isLeaf() && null != node.getFieldName() && indexOnlyFields.contains(node.getFieldName())) {
                node.setIndexOnlyField(true);
            }
        }
    }
    
}
