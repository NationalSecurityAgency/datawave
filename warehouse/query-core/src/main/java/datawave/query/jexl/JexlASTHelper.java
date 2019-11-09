package datawave.query.jexl;

import com.google.common.base.Ascii;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.normalizer.NormalizationException;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.index.lookup.RangeStream;
import datawave.query.index.stats.IndexStatsClient;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.postprocessing.tf.Function;
import datawave.query.postprocessing.tf.FunctionReferenceVisitor;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNode.Literal;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.jexl2.parser.TokenMgrError;
import org.apache.log4j.Logger;

import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.commons.jexl2.parser.JexlNodes.children;

/**
 *
 */
public class JexlASTHelper {
    
    protected static final Logger log = Logger.getLogger(JexlASTHelper.class);
    
    public static final Character GROUPING_CHARACTER_SEPARATOR = '.';
    public static final Character IDENTIFIER_PREFIX = '$';
    
    public static final Set<Class<?>> RANGE_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTGTNode.class, ASTGENode.class, ASTLTNode.class, ASTLENode.class);
    
    public static final Set<Class<?>> INCLUSIVE_RANGE_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTGENode.class, ASTLENode.class);
    public static final Set<Class<?>> EXCLUSIVE_RANGE_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTGTNode.class, ASTLTNode.class);
    
    public static final Set<Class<?>> LESS_THAN_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTLTNode.class, ASTLENode.class);
    
    public static final Set<Class<?>> GREATER_THAN_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTGTNode.class, ASTLENode.class);
    
    public static final Set<Class<?>> NON_RANGE_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTEQNode.class, ASTNENode.class, ASTERNode.class, ASTNRNode.class);
    
    public static final Map<Class<?>,Class<?>> NEGATED_NON_RANGE_NODE_CLASSES = ImmutableMap.<Class<?>,Class<?>> of(ASTEQNode.class, ASTNENode.class,
                    ASTNENode.class, ASTEQNode.class, ASTERNode.class, ASTNRNode.class, ASTNRNode.class, ASTERNode.class);
    
    /**
     * Parse a query string using a JEXL parser and transform it into a parse tree of our RefactoredDatawaveTreeNodes. This also sets all convenience maps that
     * the analyzer provides.
     * 
     * @param query
     *            The query string in JEXL syntax to parse
     * @return Root node of the query parse tree.
     * @throws ParseException
     */
    public static ASTJexlScript parseJexlQuery(String query) throws ParseException {
        // Instantiate a parser and visitor
        Parser parser = new Parser(new StringReader(";"));
        
        String caseFixQuery = query.replaceAll("\\s+[Aa][Nn][Dd]\\s+", " and ");
        caseFixQuery = caseFixQuery.replaceAll("\\s+[Oo][Rr]\\s+", " or ");
        caseFixQuery = caseFixQuery.replaceAll("\\s+[Nn][Oo][Tt]\\s+", " not ");
        
        // Parse the query
        try {
            return parser.parse(new StringReader(caseFixQuery), null);
        } catch (TokenMgrError e) {
            throw new ParseException(e.getMessage());
        }
    }
    
    /**
     * Fetch the literal off of the grandchild. Returns null if there is no literal
     * 
     * @param node
     * @return
     * @throws NoSuchElementException
     */
    public static JexlNode getLiteral(JexlNode node) throws NoSuchElementException {
        node = dereference(node);
        // check for the case where this is the literal node
        if (isLiteral(node)) {
            return node;
        }
        
        // TODO With commons-jexl-2.1.1, ASTTrueNode and ASTFalseNode are not JexlNode.Literal(s).
        // It would likely be best to make this return a Literal<?> instead of Object
        if (null != node && 2 == node.jjtGetNumChildren()) {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode child = node.jjtGetChild(i);
                
                if (null != child) {
                    if (child instanceof ASTReference) {
                        for (int j = 0; j < child.jjtGetNumChildren(); j++) {
                            JexlNode grandChild = child.jjtGetChild(j);
                            
                            // If the grandchild and its image is non-null and equal to the any-field identifier
                            //
                            if (null != grandChild && isLiteral(grandChild)) {
                                return grandChild;
                            }
                        }
                    } else if (isLiteral(child)) {
                        return child;
                    }
                }
            }
        }
        return null;
    }
    
    /**
     * Helper method to determine if the child is a literal
     * 
     * @param child
     * @return
     */
    public static boolean isLiteral(final JexlNode child) {
        if (child instanceof ASTNumberLiteral) {
            return true;
        } else if (child instanceof ASTTrueNode) {
            return true;
        } else if (child instanceof ASTFalseNode) {
            return true;
        } else if (child instanceof ASTNullLiteral) {
            return true;
        } else if (child instanceof JexlNode.Literal) {
            return true;
        }
        return false;
    }
    
    /**
     * Fetch the literal off of the grandchild. Throws an exception if there is no literal
     * 
     * @param node
     * @return
     * @throws NoSuchElementException
     */
    @SuppressWarnings("rawtypes")
    public static Object getLiteralValue(JexlNode node) throws NoSuchElementException {
        Object literal = getLiteral(node);
        // If the grandchild and its image is non-null and equal to the any-field identifier
        if (literal instanceof JexlNode.Literal) {
            return ((JexlNode.Literal) literal).getLiteral();
        } else if (literal instanceof ASTTrueNode) {
            return true;
        } else if (literal instanceof ASTFalseNode) {
            return false;
        } else if (literal instanceof ASTNullLiteral) {
            return null;
        }
        
        NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.LITERAL_MISSING);
        throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
    }
    
    /**
     * Fetch the literal off of the grandchild, removing a leading {@link #IDENTIFIER_PREFIX} if present. Throws an exception if there is no literal
     * 
     * @param node
     * @return
     * @throws NoSuchElementException
     */
    public static String getIdentifier(JexlNode node) throws NoSuchElementException {
        if (null != node && 2 == node.jjtGetNumChildren()) {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode child = node.jjtGetChild(i);
                
                if (null != child && child instanceof ASTReference) {
                    for (int j = 0; j < child.jjtGetNumChildren(); j++) {
                        JexlNode grandChild = child.jjtGetChild(j);
                        
                        // If the grandchild and its image is non-null and equal to the any-field identifier
                        if (null != grandChild && grandChild instanceof ASTIdentifier) {
                            return deconstructIdentifier(grandChild.image);
                        } else if (null != grandChild && grandChild instanceof ASTFunctionNode) {
                            return null;
                        }
                    }
                    return null;
                } else {
                    return null;
                }
            }
        } else if (node instanceof ASTIdentifier && node.jjtGetNumChildren() == 0) {
            return deconstructIdentifier(node.image);
        }
        
        NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.IDENTIFIER_MISSING);
        throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
    }
    
    /**
     * Finds all the functions and returns a map indexed by function context name to the function.
     * 
     * @param query
     * @return
     */
    public static Multimap<String,Function> getFunctions(JexlNode query) {
        FunctionReferenceVisitor visitor = new FunctionReferenceVisitor();
        query.jjtAccept(visitor, null);
        return visitor.functions();
    }
    
    public static List<ASTIdentifier> getFunctionIdentifiers(ASTFunctionNode node) {
        Preconditions.checkNotNull(node);
        
        List<ASTIdentifier> identifiers = Lists.newArrayList();
        
        int numChildren = node.jjtGetNumChildren();
        for (int i = 2; i < numChildren; i++) {
            identifiers.addAll(getIdentifiers(node.jjtGetChild(i)));
        }
        
        return identifiers;
    }
    
    public static List<ASTFunctionNode> getFunctionNodes(JexlNode node) {
        List<ASTFunctionNode> functions = Lists.newArrayList();
        
        getFunctionNodes(node, functions);
        
        return functions;
    }
    
    private static void getFunctionNodes(JexlNode node, List<ASTFunctionNode> functions) {
        if (null == node) {
            return;
        }
        
        if (node instanceof ASTFunctionNode) {
            functions.add((ASTFunctionNode) node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                getFunctionNodes(node.jjtGetChild(i), functions);
            }
        }
    }
    
    public static List<ASTIdentifier> getIdentifiers(JexlNode node) {
        List<ASTIdentifier> identifiers = Lists.newArrayList();
        
        getIdentifiers(node, identifiers);
        
        return identifiers;
    }
    
    public static Set<String> getIdentifierNames(JexlNode node) {
        List<ASTIdentifier> identifiers = Lists.newArrayList();
        getIdentifiers(node, identifiers);
        Set<String> names = new HashSet<>();
        for (ASTIdentifier identifier : identifiers) {
            names.add(identifier.image);
        }
        
        return names;
    }
    
    private static void getIdentifiers(JexlNode node, List<ASTIdentifier> identifiers) {
        if (null == node) {
            return;
        }
        
        if (node instanceof ASTFunctionNode) {
            identifiers.addAll(getFunctionIdentifiers((ASTFunctionNode) node));
        } else if (node instanceof ASTMethodNode) {
            // Don't get identifiers under a method node, they are method names
            return;
        } else if (node instanceof ASTIdentifier) {
            identifiers.add((ASTIdentifier) node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                getIdentifiers(node.jjtGetChild(i), identifiers);
            }
        }
    }
    
    public static JexlNode dereference(JexlNode node) {
        while (node.jjtGetNumChildren() == 1 && (node instanceof ASTReference || node instanceof ASTReferenceExpression)) {
            node = node.jjtGetChild(0);
        }
        return node;
    }
    
    /**
     * This is the opposite of dereference in that this will climb back up reference and reference expression nodes that only contain one child.
     * 
     * @param node
     * @return the parent reference/referenceexpression or this node
     */
    public static JexlNode rereference(JexlNode node) {
        while (node.jjtGetParent() != null && node.jjtGetParent().jjtGetNumChildren() == 1
                        && (node.jjtGetParent() instanceof ASTReference || node.jjtGetParent() instanceof ASTReferenceExpression)) {
            node = node.jjtGetParent();
        }
        return node;
    }
    
    public static IdentifierOpLiteral getIdentifierOpLiteral(JexlNode node) {
        // ensure we have the pattern we expect here
        if (node.jjtGetNumChildren() == 2) {
            JexlNode child1 = JexlASTHelper.dereference(node.jjtGetChild(0));
            JexlNode child2 = JexlASTHelper.dereference(node.jjtGetChild(1));
            if (child1 instanceof ASTIdentifier && isLiteral(child2)) {
                return new IdentifierOpLiteral((ASTIdentifier) child1, node, child2);
            }
            if (child2 instanceof ASTIdentifier && isLiteral(child1)) {
                // this should no longer happen after the fix to groom the query by reordering binary expressions that
                // have the literal on the left side
                // if this is a range op, i must reverse the logic:
                node = (JexlNode) node.jjtAccept(new InvertNodeVisitor(), null);
                return new IdentifierOpLiteral((ASTIdentifier) child2, node, child1);
            }
        }
        return null;
    }
    
    public static class IdentifierOpLiteral {
        ASTIdentifier identifier;
        JexlNode op;
        JexlNode literal;
        
        public IdentifierOpLiteral(ASTIdentifier identifier, JexlNode op, JexlNode literal) {
            this.identifier = identifier;
            this.op = op;
            this.literal = literal;
        }
        
        public ASTIdentifier getIdentifier() {
            return identifier;
        }
        
        public String deconstructIdentifier() {
            return JexlASTHelper.deconstructIdentifier(identifier);
        }
        
        public JexlNode getOp() {
            return op;
        }
        
        public JexlNode getLiteral() {
            return literal;
        }
        
        public Object getLiteralValue() {
            return JexlASTHelper.getLiteralValue(literal);
        }
        
    }
    
    public static String deconstructIdentifier(ASTIdentifier identifier) {
        return deconstructIdentifier(identifier.image);
    }
    
    /**
     * Remove the {@link #IDENTIFIER_PREFIX} from the beginning of a fieldName if it exists
     * 
     * @param fieldName
     * @return
     */
    public static String deconstructIdentifier(String fieldName) {
        return deconstructIdentifier(fieldName, false);
    }
    
    /**
     * Remove the {@link #IDENTIFIER_PREFIX} from the beginning of a fieldName if it exists
     * 
     * @param fieldName
     * @param includeGroupingContext
     * @return
     */
    public static String deconstructIdentifier(String fieldName, Boolean includeGroupingContext) {
        if (fieldName != null && fieldName.length() > 1) {
            if (!includeGroupingContext) {
                fieldName = removeGroupingContext(fieldName);
            }
            
            if (fieldName.charAt(0) == IDENTIFIER_PREFIX) {
                return fieldName.substring(1);
            }
        }
        
        return fieldName;
    }
    
    /**
     * Rebuild the identifier with the {@link #IDENTIFIER_PREFIX} if the identifier starts with an invalid character per the Jexl IDENTIFIER definition
     * 
     * @param fieldName
     * @return
     */
    public static String rebuildIdentifier(String fieldName) {
        return rebuildIdentifier(fieldName, false);
    }
    
    /**
     * Rebuild the identifier with the {@link #IDENTIFIER_PREFIX} if the identifier starts with an invalid character per the Jexl IDENTIFIER definition
     * 
     * @param fieldName
     * @param includeGroupingContext
     * @return
     */
    public static String rebuildIdentifier(String fieldName, Boolean includeGroupingContext) {
        // fieldName may be null if it is from a Function node
        if (fieldName != null && fieldName.length() > 1) {
            if (!includeGroupingContext) {
                fieldName = removeGroupingContext(fieldName);
            }
            
            Character firstChar = fieldName.charAt(0);
            
            // Accepted first character in an identifier given the Commons-Jexl-2.1.1 IDENTIFIER definition
            if (!Ascii.isLowerCase(firstChar) && !Ascii.isUpperCase(firstChar) && firstChar != '_' && firstChar != '$' && firstChar != '@') {
                return IDENTIFIER_PREFIX + fieldName;
            }
        }
        
        return fieldName;
    }
    
    public static String getGroupingContext(String fieldName) {
        int offset = fieldName.indexOf(GROUPING_CHARACTER_SEPARATOR) + 1;
        if (0 != offset) {
            return new String(fieldName.getBytes(), offset, fieldName.length() - offset);
        }
        return "";
    }
    
    public static String removeGroupingContext(String fieldName) {
        int offset = fieldName.indexOf(GROUPING_CHARACTER_SEPARATOR);
        
        if (-1 != offset) {
            // same as substring
            return new String(fieldName.getBytes(), 0, offset);
        }
        
        return fieldName;
    }
    
    public static boolean hasGroupingContext(String fieldName) {
        return fieldName.indexOf(GROUPING_CHARACTER_SEPARATOR) != -1;
    }
    
    public static Set<String> getFieldNames(ASTFunctionNode function, MetadataHelper metadata, Set<String> datatypeFilter) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(function);
        
        return desc.fields(metadata, datatypeFilter);
    }
    
    public static Set<Set<String>> getFieldNameSets(ASTFunctionNode function, MetadataHelper metadata, Set<String> datatypeFilter) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(function);
        
        return desc.fieldSets(metadata, datatypeFilter);
    }
    
    public static List<JexlNode> getFunctionArguments(ASTFunctionNode function) {
        List<JexlNode> args = Lists.newArrayList();
        
        for (int i = 0; i < function.jjtGetNumChildren(); i++) {
            JexlNode child = function.jjtGetChild(i);
            
            // Arguments for the function are inside of an ASTReference
            if (child.getClass().equals(ASTReference.class) && child.jjtGetNumChildren() == 1) {
                JexlNode grandchild = child.jjtGetChild(0);
                
                args.add(grandchild);
            }
        }
        
        return args;
    }
    
    public static List<ASTEQNode> getPositiveEQNodes(JexlNode node) {
        List<ASTEQNode> eqNodes = Lists.newArrayList();
        
        getEQNodes(node, eqNodes);
        Iterator<ASTEQNode> eqNodeItr = eqNodes.iterator();
        while (eqNodeItr.hasNext()) {
            ASTEQNode n = eqNodeItr.next();
            if (isNodeNegated(n)) {
                eqNodeItr.remove();
            }
        }
        return eqNodes;
    }
    
    public static List<ASTEQNode> getNegativeEQNodes(JexlNode node) {
        List<ASTEQNode> eqNodes = Lists.newArrayList();
        
        getEQNodes(node, eqNodes);
        Iterator<ASTEQNode> eqNodeItr = eqNodes.iterator();
        while (eqNodeItr.hasNext()) {
            ASTEQNode n = eqNodeItr.next();
            if (isNodeNegated(n) == false) {
                eqNodeItr.remove();
            }
        }
        return eqNodes;
    }
    
    private static boolean isNodeNegated(JexlNode node) {
        JexlNode parent = node.jjtGetParent();
        
        if (parent == null) {
            return false;
        } else {
            int numNegations = numNegations(parent);
            if (numNegations % 2 == 0) {
                return false;
            } else {
                return true;
            }
        }
    }
    
    private static int numNegations(JexlNode node) {
        JexlNode parent = node.jjtGetParent();
        
        if (parent == null) {
            return 0;
        } else if (parent instanceof ASTNotNode) {
            return 1 + numNegations(parent);
        } else {
            return numNegations(parent);
        }
    }
    
    public static List<ASTEQNode> getEQNodes(JexlNode node) {
        List<ASTEQNode> eqNodes = Lists.newArrayList();
        
        getEQNodes(node, eqNodes);
        
        return eqNodes;
    }
    
    private static void getEQNodes(JexlNode node, List<ASTEQNode> eqNodes) {
        if (node instanceof ASTEQNode) {
            eqNodes.add((ASTEQNode) node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                getEQNodes(node.jjtGetChild(i), eqNodes);
            }
        }
    }
    
    public static List<ASTERNode> getERNodes(JexlNode node) {
        List<ASTERNode> erNodes = Lists.newArrayList();
        
        getERNodes(node, erNodes);
        
        return erNodes;
    }
    
    private static void getERNodes(JexlNode node, List<ASTERNode> erNodes) {
        if (node instanceof ASTERNode) {
            erNodes.add((ASTERNode) node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                getERNodes(node.jjtGetChild(i), erNodes);
            }
        }
    }
    
    public static List<JexlNode> getLiterals(JexlNode node) {
        return getLiterals(node, Lists.<JexlNode> newLinkedList());
    }
    
    private static List<JexlNode> getLiterals(JexlNode node, List<JexlNode> literals) {
        if (isLiteral(node)) {
            literals.add(node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                literals = getLiterals(node.jjtGetChild(i), literals);
            }
        }
        
        return literals;
    }
    
    public static Map<String,Object> getAssignments(JexlNode node) {
        return getAssignments(node, Maps.<String,Object> newHashMap());
    }
    
    private static Map<String,Object> getAssignments(JexlNode node, Map<String,Object> assignments) {
        if (node instanceof ASTAssignment) {
            assignments.put(getIdentifier(node), getLiteralValue(node));
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                assignments = getAssignments(node.jjtGetChild(i), assignments);
            }
        }
        
        return assignments;
    }
    
    /**
     * Get the bounded ranges (index only terms).
     *
     * @param root
     *            The root and node
     * @param helper
     *            The metadata helper
     * @param otherNodes
     *            If not null, then this is filled with all nodes not used to make the ranges (minimal node list, minimal tree depth)
     * @param maxDepth
     *            The maximum depth to traverse the tree. -1 represents unlimited depth.
     * @return The ranges, all bounded.
     */
    @SuppressWarnings("rawtypes")
    public static Map<LiteralRange<?>,List<JexlNode>> getBoundedRanges(JexlNode root, Set<String> datatypeFilterSet, MetadataHelper helper,
                    List<JexlNode> otherNodes, boolean includeDelayed, int maxDepth) {
        List<JexlNode> nonIndexedRangeNodes = new ArrayList<>();
        List<JexlNode> rangeNodes = getIndexRangeOperatorNodes(root, datatypeFilterSet, helper, nonIndexedRangeNodes, otherNodes, includeDelayed, maxDepth);
        return getBoundedRanges(rangeNodes, nonIndexedRangeNodes, otherNodes);
    }
    
    /**
     * Get the bounded ranges (index only terms).
     * 
     * @param root
     *            The root and node
     * @param helper
     *            The metadata helper
     * @param otherNodes
     *            If not null, then this is filled with all nodes not used to make the ranges (minimal node list, minimal tree depth)
     * @return The ranges, all bounded.
     */
    @SuppressWarnings("rawtypes")
    public static Map<LiteralRange<?>,List<JexlNode>> getBoundedRanges(JexlNode root, Set<String> datatypeFilterSet, MetadataHelper helper,
                    List<JexlNode> otherNodes, boolean includeDelayed) {
        List<JexlNode> nonIndexedRangeNodes = new ArrayList<>();
        List<JexlNode> rangeNodes = getIndexRangeOperatorNodes(root, datatypeFilterSet, helper, nonIndexedRangeNodes, otherNodes, includeDelayed, -1);
        return getBoundedRanges(rangeNodes, nonIndexedRangeNodes, otherNodes);
    }
    
    /**
     * Get the bounded ranges.
     *
     * @param root
     *            The root node
     * @param otherNodes
     *            If not null, then this is filled with all nodes not used to make the ranges (minimal node list, minimal tree depth)
     * @param maxDepth
     *            The maximum depth to traverse the tree. -1 represents unlimited depth.
     * @return The ranges, all bounded.
     */
    @SuppressWarnings("rawtypes")
    public static Map<LiteralRange<?>,List<JexlNode>> getBoundedRangesIndexAgnostic(JexlNode root, List<JexlNode> otherNodes, boolean includeDelayed,
                    int maxDepth) {
        List<JexlNode> rangeNodes = getRangeOperatorNodes(root, otherNodes, includeDelayed, maxDepth);
        return JexlASTHelper.getBoundedRanges(rangeNodes, null, otherNodes);
    }
    
    /**
     * Get the bounded ranges.
     * 
     * @param root
     *            The root node
     * @param otherNodes
     *            If not null, then this is filled with all nodes not used to make the ranges (minimal node list, minimal tree depth)
     * @return The ranges, all bounded.
     */
    @SuppressWarnings("rawtypes")
    public static Map<LiteralRange<?>,List<JexlNode>> getBoundedRangesIndexAgnostic(JexlNode root, List<JexlNode> otherNodes, boolean includeDelayed) {
        return getBoundedRangesIndexAgnostic(root, otherNodes, includeDelayed, -1);
    }
    
    protected static Map<LiteralRange<?>,List<JexlNode>> getBoundedRanges(List<JexlNode> rangeNodes, List<JexlNode> nonIndexedRangeNodes,
                    List<JexlNode> otherNodes) {
        
        // if the non-indexed range nodes were split out, then lets group them back into their AND expressions and put them in the
        // other node list (see getBoundedRanges vs. getBoundedRangesIndexAgnostic)
        if (nonIndexedRangeNodes != null && otherNodes != null) {
            Map<LiteralRange<?>,List<JexlNode>> ranges = getBoundedRanges(nonIndexedRangeNodes, otherNodes);
            for (List<JexlNode> range : ranges.values()) {
                // create a ref -> ref_exp -> and -> <range nodes>
                ASTAndNode andNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
                andNode = JexlNodes.children(andNode, range.get(0), range.get(1));
                ASTReferenceExpression refExpNode = JexlNodes.wrap(andNode);
                ASTReference refNode = JexlNodes.makeRef(refExpNode);
                otherNodes.add(refNode);
            }
        }
        return getBoundedRanges(rangeNodes, otherNodes);
    }
    
    protected static Map<LiteralRange<?>,List<JexlNode>> getBoundedRanges(List<JexlNode> rangeNodes, List<JexlNode> otherNodes) {
        Map<LiteralRange<?>,List<JexlNode>> ranges = new HashMap<>();
        
        while (!rangeNodes.isEmpty()) {
            JexlNode firstNode = rangeNodes.get(0);
            String fieldName = JexlASTHelper.getIdentifier(firstNode);
            Object literal = JexlASTHelper.getLiteralValue(firstNode);
            
            LiteralRange<?> range = null;
            List<JexlNode> thisRangesNodes = new ArrayList<>();
            if (literal instanceof String) {
                range = getStringBoundedRange(rangeNodes, thisRangesNodes, new LiteralRange<>(fieldName, LiteralRange.NodeOperand.AND));
            } else if (literal instanceof Integer) {
                range = getIntegerBoundedRange(rangeNodes, thisRangesNodes, new LiteralRange<>(fieldName, LiteralRange.NodeOperand.AND));
            } else if (literal instanceof Long) {
                range = getLongBoundedRange(rangeNodes, thisRangesNodes, new LiteralRange<>(fieldName, LiteralRange.NodeOperand.AND));
            } else if (literal instanceof BigInteger) {
                range = getBigIntegerBoundedRange(rangeNodes, thisRangesNodes, new LiteralRange<>(fieldName, LiteralRange.NodeOperand.AND));
            } else if (literal instanceof Float) {
                range = getFloatBoundedRange(rangeNodes, thisRangesNodes, new LiteralRange<>(fieldName, LiteralRange.NodeOperand.AND));
            } else if (literal instanceof Double) {
                range = getDoubleBoundedRange(rangeNodes, thisRangesNodes, new LiteralRange<>(fieldName, LiteralRange.NodeOperand.AND));
            } else if (literal instanceof BigDecimal) {
                range = getBigDecimalBoundedRange(rangeNodes, thisRangesNodes, new LiteralRange<>(fieldName, LiteralRange.NodeOperand.AND));
            } else {
                QueryException qe = new QueryException(DatawaveErrorCode.NODE_LITERAL_TYPE_ASCERTAIN_ERROR, MessageFormat.format("{0}", literal));
                throw new DatawaveFatalQueryException(qe);
            }
            if (range.isBounded()) {
                ranges.put(range, thisRangesNodes);
            } else {
                if (otherNodes != null) {
                    otherNodes.addAll(thisRangesNodes);
                }
            }
        }
        
        return ranges;
    }
    
    public static LiteralRange<String> getStringBoundedRange(List<JexlNode> nodes, List<JexlNode> rangeNodes, LiteralRange<String> range) {
        Iterator<JexlNode> it = nodes.iterator();
        while (it.hasNext()) {
            JexlNode node = it.next();
            String newFieldName = JexlASTHelper.getIdentifier(node);
            
            if (range.getFieldName().equals(newFieldName)) {
                String literal = (String) JexlASTHelper.getLiteralValue(node);
                
                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, true);
                    } else {
                        range.updateLower(literal, true);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, false);
                    } else {
                        range.updateLower(literal, false);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else {
                    log.warn("Could not determine class of node: " + node);
                }
            }
        }
        
        return range;
    }
    
    protected static LiteralRange<Integer> getIntegerBoundedRange(List<JexlNode> nodes, List<JexlNode> rangeNodes, LiteralRange<Integer> range) {
        Iterator<JexlNode> it = nodes.iterator();
        while (it.hasNext()) {
            JexlNode node = it.next();
            String newFieldName = JexlASTHelper.getIdentifier(node);
            
            if (range.getFieldName() == null || range.getFieldName().equals(newFieldName)) {
                Integer literal = (Integer) JexlASTHelper.getLiteralValue(node);
                
                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, true);
                    } else {
                        range.updateLower(literal, true);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, false);
                    } else {
                        range.updateLower(literal, false);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else {
                    log.warn("Could not determine class of node: " + node);
                }
            }
        }
        
        return range;
    }
    
    public static LiteralRange<Long> getLongBoundedRange(List<JexlNode> nodes, List<JexlNode> rangeNodes, LiteralRange<Long> range) {
        Iterator<JexlNode> it = nodes.iterator();
        while (it.hasNext()) {
            JexlNode node = it.next();
            String newFieldName = JexlASTHelper.getIdentifier(node);
            
            if (range.getFieldName() == null || range.getFieldName().equals(newFieldName)) {
                Long literal = (Long) JexlASTHelper.getLiteralValue(node);
                
                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, true);
                    } else {
                        range.updateLower(literal, true);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, false);
                    } else {
                        range.updateLower(literal, false);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else {
                    log.warn("Could not determine class of node: " + node);
                }
            }
        }
        
        return range;
    }
    
    protected static LiteralRange<BigInteger> getBigIntegerBoundedRange(List<JexlNode> nodes, List<JexlNode> rangeNodes, LiteralRange<BigInteger> range) {
        Iterator<JexlNode> it = nodes.iterator();
        while (it.hasNext()) {
            JexlNode node = it.next();
            String newFieldName = JexlASTHelper.getIdentifier(node);
            
            if (range.getFieldName().equals(newFieldName)) {
                BigInteger literal = (BigInteger) JexlASTHelper.getLiteralValue(node);
                
                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, true);
                    } else {
                        range.updateLower(literal, true);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, false);
                    } else {
                        range.updateLower(literal, false);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else {
                    log.warn("Could not determine class of node: " + node);
                }
            }
        }
        
        return range;
    }
    
    public static LiteralRange<Float> getFloatBoundedRange(List<JexlNode> nodes, List<JexlNode> rangeNodes, LiteralRange<Float> range) {
        Iterator<JexlNode> it = nodes.iterator();
        while (it.hasNext()) {
            JexlNode node = it.next();
            String newFieldName = JexlASTHelper.getIdentifier(node);
            
            if (range.getFieldName().equals(newFieldName)) {
                Float literal = (Float) JexlASTHelper.getLiteralValue(node);
                
                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, true);
                    } else {
                        range.updateLower(literal, true);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, false);
                    } else {
                        range.updateLower(literal, false);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else {
                    log.warn("Could not determine class of node: " + node);
                }
            }
        }
        
        return range;
    }
    
    protected static LiteralRange<Double> getDoubleBoundedRange(List<JexlNode> nodes, List<JexlNode> rangeNodes, LiteralRange<Double> range) {
        Iterator<JexlNode> it = nodes.iterator();
        while (it.hasNext()) {
            JexlNode node = it.next();
            String newFieldName = JexlASTHelper.getIdentifier(node);
            
            if (range.getFieldName().equals(newFieldName)) {
                Double literal = (Double) JexlASTHelper.getLiteralValue(node);
                
                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, true);
                    } else {
                        range.updateLower(literal, true);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, false);
                    } else {
                        range.updateLower(literal, false);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else {
                    log.warn("Could not determine class of node: " + node);
                }
            }
        }
        
        return range;
    }
    
    protected static LiteralRange<BigDecimal> getBigDecimalBoundedRange(List<JexlNode> nodes, List<JexlNode> rangeNodes, LiteralRange<BigDecimal> range) {
        Iterator<JexlNode> it = nodes.iterator();
        while (it.hasNext()) {
            JexlNode node = it.next();
            String newFieldName = JexlASTHelper.getIdentifier(node);
            
            if (range.getFieldName().equals(newFieldName)) {
                BigDecimal literal = (BigDecimal) JexlASTHelper.getLiteralValue(node);
                
                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, true);
                    } else {
                        range.updateLower(literal, true);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(node.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(node.getClass())) {
                        range.updateUpper(literal, false);
                    } else {
                        range.updateLower(literal, false);
                    }
                    if (rangeNodes != null) {
                        rangeNodes.add(node);
                    }
                    it.remove();
                } else {
                    log.warn("Could not determine class of node: " + node);
                }
            }
        }
        
        return range;
    }
    
    protected static List<JexlNode> getIndexRangeOperatorNodes(JexlNode root, Set<String> datatypeFilterSet, MetadataHelper helper,
                    List<JexlNode> nonIndexedRangeNodes, List<JexlNode> otherNodes, boolean includeDelayed, int maxDepth) {
        List<JexlNode> nodes = Lists.newArrayList();
        
        Class<?> clz = root.getClass();
        
        if (root.jjtGetNumChildren() > 0) {
            getRangeOperatorNodes(root, clz, nodes, otherNodes, datatypeFilterSet, helper, nonIndexedRangeNodes, includeDelayed, maxDepth);
        }
        
        return nodes;
    }
    
    protected static List<JexlNode> getRangeOperatorNodes(JexlNode root, List<JexlNode> otherNodes, boolean includeDelayed, int maxDepth) {
        List<JexlNode> nodes = Lists.newArrayList();
        
        Class<?> clz = root.getClass();
        
        if (root.jjtGetNumChildren() > 0) {
            getRangeOperatorNodes(root, clz, nodes, otherNodes, null, null, null, includeDelayed, maxDepth);
        }
        
        return nodes;
    }
    
    protected static boolean isDelayedPredicate(JexlNode currNode) {
        if (ASTDelayedPredicate.instanceOf(currNode) || ExceededOrThresholdMarkerJexlNode.instanceOf(currNode)
                        || ExceededValueThresholdMarkerJexlNode.instanceOf(currNode) || ExceededTermThresholdMarkerJexlNode.instanceOf(currNode)
                        || IndexHoleMarkerJexlNode.instanceOf(currNode) || ASTEvaluationOnly.instanceOf(currNode))
            return true;
        else
            return false;
    }
    
    /**
     * Get the range operator nodes. If "mustBeIndexed" is true, then a config and helper must be supplied to check if the fields are indexed.
     * 
     * @param root
     * @param clz
     * @param nodes
     *            : filled with the range operator nodes
     * @param otherNodes
     *            : if not null, filled with all other nodes (minimal depth)
     * @param helper
     *            Required if mustBeIndexed
     * @param nonIndexedRangeNodes
     * @param maxDepth
     *            The maximum depth to traverse the tree. -1 represents unlimited depth.
     *
     */
    protected static void getRangeOperatorNodes(JexlNode root, Class<?> clz, List<JexlNode> nodes, List<JexlNode> otherNodes, Set<String> datatypeFilterSet,
                    MetadataHelper helper, List<JexlNode> nonIndexedRangeNodes, boolean includeDelayed, int maxDepth) {
        if ((!includeDelayed && isDelayedPredicate(root)) || maxDepth == 0) {
            return;
        }
        for (int i = 0; i < root.jjtGetNumChildren(); i++) {
            JexlNode child = root.jjtGetChild(i);
            // When checking for a bounded range in a subtree, need to consider nodes of the same class
            // as the root, reference expression nodes, and reference nodes
            if (child.getClass().equals(clz) || child.getClass().equals(ASTReferenceExpression.class) || child.getClass().equals(ASTReference.class)) {
                // ignore getting range nodes out of delayed expressions as they have already been processed
                if (includeDelayed || !isDelayedPredicate(child)) {
                    getRangeOperatorNodes(child, clz, nodes, otherNodes, datatypeFilterSet, helper, nonIndexedRangeNodes, includeDelayed, maxDepth - 1);
                } else if (otherNodes != null) {
                    otherNodes.add(JexlASTHelper.rereference(child));
                }
            } else if (RANGE_NODE_CLASSES.contains(child.getClass())) {
                
                boolean hasMethod = ((AtomicBoolean) child.jjtAccept(new JexlASTHelper.HasMethodVisitor(), new AtomicBoolean(false))).get();
                
                String fieldName = JexlASTHelper.getIdentifier(child);
                
                if (hasMethod && otherNodes != null) {
                    otherNodes.add(JexlASTHelper.rereference(child));
                    
                } else if (nonIndexedRangeNodes != null) {
                    try {
                        // We can do a better job here by actually using the type
                        if (fieldName != null && helper.isIndexed(fieldName, datatypeFilterSet)) {
                            nodes.add(child);
                        } else {
                            nonIndexedRangeNodes.add(child);
                        }
                    } catch (TableNotFoundException e) {
                        NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.TABLE_NOT_FOUND, e);
                        throw new DatawaveFatalQueryException(qe);
                    }
                } else {
                    nodes.add(child);
                }
            } else {
                // else, one of the other nodes or subtrees
                if (otherNodes != null) {
                    otherNodes.add(JexlASTHelper.rereference(child));
                }
            }
        }
    }
    
    public static boolean isWithinOr(JexlNode node) {
        if (null != node && null != node.jjtGetParent()) {
            JexlNode parent = node.jjtGetParent();
            
            if (parent instanceof ASTOrNode) {
                return true;
            }
            
            return isWithinOr(parent);
        }
        
        return false;
    }
    
    public static boolean isWithinNot(JexlNode node) {
        while (null != node && null != node.jjtGetParent()) {
            JexlNode parent = node.jjtGetParent();
            
            if (parent instanceof ASTNotNode) {
                return true;
            }
            
            return isWithinNot(parent);
        }
        
        return false;
    }
    
    public static boolean isWithinAnd(JexlNode node) {
        while (null != node && null != node.jjtGetParent()) {
            JexlNode parent = node.jjtGetParent();
            
            if (parent instanceof ASTAndNode) {
                return true;
            }
            
            return isWithinAnd(parent);
        }
        
        return false;
    }
    
    /**
     * Performs an order-dependent AST equality check
     * 
     * @param one
     * @param two
     * @return
     */
    public static boolean equals(JexlNode one, JexlNode two) {
        // If we have the same object or they're both null, they're equal
        if (one == two) {
            return true;
        }
        
        if (null == one || null == two) {
            return false;
        }
        
        // Not equal if the concrete classes are not the same
        if (!one.getClass().equals(two.getClass())) {
            return false;
        }
        
        // Not equal if the number of children differs
        if (one.jjtGetNumChildren() != two.jjtGetNumChildren()) {
            return false;
        }
        
        for (int i = 0; i < one.jjtGetNumChildren(); i++) {
            if (!equals(one.jjtGetChild(i), two.jjtGetChild(i))) {
                return false;
            }
        }
        
        // We already asserted one and two are the same concrete class
        if (one instanceof ASTNumberLiteral) {
            ASTNumberLiteral oneLit = (ASTNumberLiteral) one, twoLit = (ASTNumberLiteral) two;
            if (!oneLit.getLiteralClass().equals(twoLit.getLiteralClass()) || !oneLit.getLiteral().equals(twoLit.getLiteral())) {
                return false;
            }
        } else if (one instanceof Literal) {
            Literal<?> oneLit = (Literal<?>) one, twoLit = (Literal<?>) two;
            if (!oneLit.getLiteral().equals(twoLit.getLiteral())) {
                return false;
            }
        } else if (one instanceof ASTIdentifier) {
            if (!one.image.equals(two.image)) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * When at an operand, this method will find the first Identifier and replace its {image} value with the supplied {String}. This is intended to be used when
     * the query model is being supplied and we want to replace the field name in some expression.
     * 
     * This method returns a new operand node with an updated {Identifier}.
     * 
     * If neither of the operand's children are an {Identifier}, then an {IllegalArgumentException} is thrown.
     * 
     * @param <T>
     * @param operand
     * @param field
     * @return
     */
    public static <T extends JexlNode> T setField(T operand, String field) {
        ASTIdentifier identifier = findIdentifier(operand);
        if (identifier == null) {
            throw new IllegalArgumentException();
        } else {
            identifier.image = JexlASTHelper.rebuildIdentifier(field);
            return operand;
        }
    }
    
    private static ASTIdentifier findIdentifier(JexlNode node) {
        if (node instanceof ASTIdentifier) {
            return (ASTIdentifier) node;
        }
        
        for (JexlNode child : JexlNodes.children(node)) {
            ASTIdentifier test = findIdentifier(child);
            if (test != null) {
                return test;
            }
        }
        
        return null;
    }
    
    public static ASTReference normalizeLiteral(JexlNode literal, Type<?> normalizer) throws NormalizationException {
        String normalizedImg = normalizer.normalize(literal.image);
        ASTStringLiteral normalizedLiteral = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        normalizedLiteral.image = normalizedImg;
        return JexlNodes.makeRef(normalizedLiteral);
    }
    
    public static JexlNode findLiteral(JexlNode node) {
        if (node instanceof Literal<?>) {
            return node;
        }
        
        for (JexlNode child : JexlNodes.children(node)) {
            JexlNode test = findLiteral(child);
            if (test != null && test instanceof Literal<?>) {
                return test;
            }
        }
        
        return null;
    }
    
    public static <T extends JexlNode> T swapLiterals(T operand, ASTReference literal) {
        JexlNode oldLiteral = findLiteral(operand);
        // we need the direct child of this operand (should be at most one level too deep)
        while (oldLiteral.jjtGetParent() != operand) {
            oldLiteral = oldLiteral.jjtGetParent();
        }
        return JexlNodes.swap(operand, oldLiteral, literal);
    }
    
    public static <T extends JexlNode> T applyNormalization(T operand, Type<?> normalizer) throws NormalizationException {
        return swapLiterals(operand, normalizeLiteral(findLiteral(operand), normalizer));
    }
    
    public static List<ASTReferenceExpression> wrapInParens(List<? extends JexlNode> intersections) {
        return Lists.transform(intersections, (com.google.common.base.Function<JexlNode,ASTReferenceExpression>) JexlNodes::wrap);
    }
    
    /**
     * Jexl's Literal interface sucks and doesn't actually line up with things we would call "literals" (constants) notably, "true", "false", and "null"
     * keywords
     * 
     * @param node
     * @return
     */
    public static boolean isLiteral(Object node) {
        if (null == node) {
            return false;
        }
        
        Class<?> clz = node.getClass();
        return Literal.class.isAssignableFrom(clz) || ASTTrueNode.class.isAssignableFrom(clz) || ASTFalseNode.class.isAssignableFrom(clz)
                        || ASTNullLiteral.class.isAssignableFrom(clz);
        
    }
    
    /**
     * Check if the provided JexlNode is an ASTEQNode and is of the form `identifier eq literal`
     * 
     * @param node
     * @return
     */
    public static boolean isLiteralEquality(JexlNode node) {
        Preconditions.checkNotNull(node);
        
        if (node instanceof ASTEQNode) {
            if (node.jjtGetNumChildren() == 2) {
                List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
                List<JexlNode> literals = JexlASTHelper.getLiterals(node);
                return identifiers.size() == 1 && literals.size() == 1;
            }
        }
        
        return false;
    }
    
    /**
     * Determine if the given ASTEQNode is indexed based off of the Multimap of String fieldname to TextNormalizer.
     * 
     * @param node
     * @param config
     * @return
     */
    public static boolean isIndexed(JexlNode node, ShardQueryConfiguration config) {
        Preconditions.checkNotNull(config);
        
        final Multimap<String,Type<?>> indexedFieldsDatatypes = config.getQueryFieldsDatatypes();
        
        Preconditions.checkNotNull(indexedFieldsDatatypes);
        
        // We expect the node to be `field op value` here
        final Collection<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        if (1 != identifiers.size()) {
            return false;
        }
        
        // Clean the image off of the ASTIdentifier
        final String fieldName = deconstructIdentifier(identifiers.iterator().next());
        
        // Determine if the field name has associated dataTypes (is indexed)
        return RangeStream.isIndexed(fieldName, indexedFieldsDatatypes);
    }
    
    /**
     * Return the selectivity of the node's identifier, or IndexStatsClient.DEFAULT_VALUE if there's an error getting the selectivity
     * 
     * @param node
     * @param config
     * @param stats
     * @return
     */
    public static Double getNodeSelectivity(JexlNode node, ShardQueryConfiguration config, IndexStatsClient stats) {
        List<ASTIdentifier> idents = getIdentifiers(node);
        
        // If there isn't one identifier you don't need to check the selectivity
        if (idents.size() != 1) {
            return IndexStatsClient.DEFAULT_VALUE;
        }
        
        return getNodeSelectivity(Sets.newHashSet(JexlASTHelper.deconstructIdentifier(idents.get(0))), config, stats);
    }
    
    /**
     * Return the selectivity of the node's identifier, or IndexStatsClient.DEFAULT_VALUE if there's an error getting the selectivity
     * 
     * @param fieldNames
     * @param config
     * @param stats
     * @return
     */
    public static Double getNodeSelectivity(Set<String> fieldNames, ShardQueryConfiguration config, IndexStatsClient stats) {
        
        boolean foundSelectivity = false;
        
        Double maxSelectivity = Double.valueOf("-1");
        if (null != config.getIndexStatsTableName()) {
            Map<String,Double> stat = stats.safeGetStat(fieldNames, config.getDatatypeFilter(), config.getBeginDate(), config.getEndDate());
            for (Entry<String,Double> entry : stat.entrySet()) {
                Double val = entry.getValue();
                // Should only get DEFAULT_STRING and DEFAULT_VALUE if there was some sort of issue getting the stats,
                // so skip this entry
                if (entry.getKey().equals(IndexStatsClient.DEFAULT_STRING) && val.equals(IndexStatsClient.DEFAULT_VALUE)) {
                    // do nothin
                } else if (val > maxSelectivity) {
                    maxSelectivity = val;
                    foundSelectivity = true;
                }
            }
        }
        // No selectivities were found, so return the default selectivity
        // from the IndexStatsClient
        if (!foundSelectivity) {
            
            return IndexStatsClient.DEFAULT_VALUE;
        }
        
        return maxSelectivity;
    }
    
    /**
     * Checks to see if the tree contains any null children, children with null parents, or children with conflicting parentage.
     *
     * @param rootNode
     *            the tree to validate
     * @param failHard
     *            whether or not to throw an exception if validation fails
     * @return true if valid, false otherwise
     */
    // checks to see if the tree contains any null children, children with null parents, or children with conflicting parentage
    public static boolean validateLineage(JexlNode rootNode, boolean failHard) {
        boolean result = true;
        
        // add all the nodes to the stack and iterate...
        Deque<JexlNode> workingStack = new LinkedList<>();
        workingStack.push(rootNode);
        
        // go through all of the nodes from parent to children, and ensure that parent and child relationships are correct
        while (!workingStack.isEmpty()) {
            JexlNode node = workingStack.pop();
            
            if (node.jjtGetNumChildren() > 0) {
                for (JexlNode child : children(node)) {
                    if (child != null) {
                        if (child.jjtGetParent() == null) {
                            if (failHard)
                                throw new RuntimeException("Failed to validate lineage: Tree included a child with a null parent.");
                            else
                                log.error("Failed to validate lineage: Tree included a child with a null parent.");
                            
                            result = false;
                        } else if (child.jjtGetParent() != node) {
                            if (failHard)
                                throw new RuntimeException("Failed to validate lineage:  Included a child with a conflicting parent.");
                            else
                                log.error("Failed to validate lineage:  Included a child with a conflicting parent.");
                            
                            result = false;
                        }
                        workingStack.push(child);
                    } else {
                        if (failHard)
                            throw new RuntimeException("Failed to validate lineage: Included a null child.");
                        else
                            log.error("Failed to validate lineage: Included a null child.");
                        
                        result = false;
                    }
                }
            }
        }
        return result;
    }
    
    private JexlASTHelper() {}
    
    public static JexlNode addEqualityToOr(ASTOrNode lhsSource, ASTEQNode rhsSource) {
        lhsSource.jjtAddChild(rhsSource, lhsSource.jjtGetNumChildren());
        rhsSource.jjtSetParent(lhsSource);
        return lhsSource;
    }
    
    public static class HasMethodVisitor extends BaseVisitor {
        
        public static <T extends JexlNode> boolean hasMethod(T script) {
            return ((AtomicBoolean) script.jjtAccept(new HasMethodVisitor(), new AtomicBoolean(false))).get();
        }
        
        @Override
        public Object visit(ASTMethodNode node, Object data) {
            AtomicBoolean state = (AtomicBoolean) data;
            state.set(true);
            return data;
        }
        
        @Override
        public Object visit(ASTSizeMethod node, Object data) {
            AtomicBoolean state = (AtomicBoolean) data;
            state.set(true);
            return data;
        }
    }
    
    public static class HasUnfieldedTermVisitor extends BaseVisitor {
        
        @Override
        public Object visit(ASTIdentifier node, Object data) {
            if (node.image != null && Constants.ANY_FIELD.equals(node.image)) {
                AtomicBoolean state = (AtomicBoolean) data;
                state.set(true);
            }
            return data;
        }
    }
    
    public static class InvertNodeVisitor extends RebuildingVisitor {
        
        public static <T extends JexlNode> T invertSwappedNodes(T script) {
            InvertNodeVisitor visitor = new InvertNodeVisitor();
            
            return (T) script.jjtAccept(visitor, null);
        }
        
        private JexlNode reparent(JexlNode in, JexlNode out) {
            int j = 0;
            for (int i = in.jjtGetNumChildren() - 1; i >= 0; i--) {
                JexlNode kid = in.jjtGetChild(i);
                kid = (JexlNode) kid.jjtAccept(this, null);
                out.jjtAddChild(kid, j++);
                kid.jjtSetParent(out);
            }
            return out;
        }
        
        @Override
        public Object visit(ASTGENode node, Object data) {
            
            if (node.jjtGetNumChildren() == 2) {
                JexlNode child1 = JexlASTHelper.dereference(node.jjtGetChild(0));
                if (isLiteral(child1)) {
                    return reparent(node, new ASTLENode(ParserTreeConstants.JJTLENODE));
                }
            }
            return super.visit(node, data);
        }
        
        @Override
        public Object visit(ASTLENode node, Object data) {
            if (node.jjtGetNumChildren() == 2) {
                JexlNode child1 = JexlASTHelper.dereference(node.jjtGetChild(0));
                if (isLiteral(child1)) {
                    return reparent(node, new ASTGENode(ParserTreeConstants.JJTGENODE));
                }
            }
            return super.visit(node, data);
        }
        
        @Override
        public Object visit(ASTGTNode node, Object data) {
            if (node.jjtGetNumChildren() == 2) {
                JexlNode child1 = JexlASTHelper.dereference(node.jjtGetChild(0));
                if (isLiteral(child1)) {
                    return reparent(node, new ASTLTNode(ParserTreeConstants.JJTLTNODE));
                }
            }
            return super.visit(node, data);
        }
        
        @Override
        public Object visit(ASTLTNode node, Object data) {
            if (node.jjtGetNumChildren() == 2) {
                JexlNode child1 = JexlASTHelper.dereference(node.jjtGetChild(0));
                if (isLiteral(child1)) {
                    return reparent(node, new ASTGTNode(ParserTreeConstants.JJTGTNODE));
                }
            }
            return super.visit(node, data);
        }
        
        @Override
        public Object visit(ASTEQNode node, Object data) {
            if (node.jjtGetNumChildren() == 2) {
                JexlNode child1 = JexlASTHelper.dereference(node.jjtGetChild(0));
                if (isLiteral(child1)) {
                    return reparent(node, new ASTEQNode(ParserTreeConstants.JJTEQNODE));
                }
            }
            return super.visit(node, data);
        }
        
        @Override
        public Object visit(ASTERNode node, Object data) {
            if (node.jjtGetNumChildren() == 2) {
                JexlNode child1 = JexlASTHelper.dereference(node.jjtGetChild(0));
                if (isLiteral(child1)) {
                    return reparent(node, new ASTERNode(ParserTreeConstants.JJTERNODE));
                }
            }
            return super.visit(node, data);
        }
        
        @Override
        public Object visit(ASTNENode node, Object data) {
            if (node.jjtGetNumChildren() == 2) {
                JexlNode child1 = JexlASTHelper.dereference(node.jjtGetChild(0));
                if (isLiteral(child1)) {
                    return reparent(node, new ASTNENode(ParserTreeConstants.JJTNENODE));
                }
            }
            return super.visit(node, data);
        }
        
        @Override
        public Object visit(ASTNRNode node, Object data) {
            if (node.jjtGetNumChildren() == 2) {
                JexlNode child1 = JexlASTHelper.dereference(node.jjtGetChild(0));
                if (isLiteral(child1)) {
                    return reparent(node, new ASTNRNode(ParserTreeConstants.JJTNRNODE));
                }
            }
            return super.visit(node, data);
        }
        
    }
}
