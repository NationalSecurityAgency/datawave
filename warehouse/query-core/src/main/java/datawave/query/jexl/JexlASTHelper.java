package datawave.query.jexl;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static org.apache.commons.jexl3.parser.JexlNodes.wrap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.Parser;
import org.apache.commons.jexl3.parser.StringProvider;
import org.apache.commons.jexl3.parser.TokenMgrException;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Ascii;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.index.lookup.RangeStream;
import datawave.query.index.stats.IndexStatsClient;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.InvertNodeVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.NodeTypeCountVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.jexl.visitors.validate.JunctionValidatingVisitor;
import datawave.query.postprocessing.tf.Function;
import datawave.query.postprocessing.tf.FunctionReferenceVisitor;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;

/**
 *
 */
public class JexlASTHelper {

    protected static final Logger log = Logger.getLogger(JexlASTHelper.class);

    // Compile patterns once up front.
    private static Pattern AND_PATTERN = Pattern.compile("\\s+[Aa][Nn][Dd]\\s+");
    private static Pattern OR_PATTERN = Pattern.compile("\\s+[Oo][Rr]\\s+");
    private static Pattern NOT_PATTERN = Pattern.compile("\\s+[Nn][Oo][Tt]\\s+");

    public static final Character GROUPING_CHARACTER_SEPARATOR = '.';
    public static final Character IDENTIFIER_PREFIX = '$';

    public static final String SINGLE_BACKSLASH = "\\";
    public static final String DOUBLE_BACKSLASH = "\\\\";

    protected static final Set<Class<?>> RANGE_NODE_CLASSES = Sets.newHashSet(ASTGTNode.class, ASTGENode.class, ASTLTNode.class, ASTLENode.class);

    protected static final Set<Class<?>> INCLUSIVE_RANGE_NODE_CLASSES = Sets.newHashSet(ASTGENode.class, ASTLENode.class);
    protected static final Set<Class<?>> EXCLUSIVE_RANGE_NODE_CLASSES = Sets.newHashSet(ASTGTNode.class, ASTLTNode.class);

    protected static final Set<Class<?>> LESS_THAN_NODE_CLASSES = Sets.newHashSet(ASTLTNode.class, ASTLENode.class);

    protected static final Set<Class<?>> GREATER_THAN_NODE_CLASSES = Sets.newHashSet(ASTGTNode.class, ASTLENode.class);

    protected static final Set<Class<?>> NON_RANGE_NODE_CLASSES = Sets.newHashSet(ASTEQNode.class, ASTNENode.class, ASTERNode.class, ASTNRNode.class);

    public static final Map<Class<?>,Class<?>> NEGATED_NON_RANGE_NODE_CLASSES = ImmutableMap.of(ASTEQNode.class, ASTNENode.class, ASTNENode.class,
                    ASTEQNode.class, ASTERNode.class, ASTNRNode.class, ASTNRNode.class, ASTERNode.class);

    /**
     * Parses a query tree from a query string and also flattens the query (flattening ORs and ANDs).
     * <p>
     * Note: Flattening does not remove reference nodes or reference expressions from the query tree. To do so requires explicit call to
     * {@link TreeFlatteningRebuildingVisitor#flattenAll(JexlNode)}.
     *
     * @param query
     *            string representation of a query
     * @return a fully parsed and flattened query tree
     * @throws ParseException
     *             when unable to parse Jexl Query
     */
    public static ASTJexlScript parseAndFlattenJexlQuery(String query) throws ParseException {
        ASTJexlScript script = parseJexlQuery(query);
        return TreeFlatteningRebuildingVisitor.flatten(script);
    }

    public static JexlFeatures jexlFeatures() {
        // @formatter:off
        return new JexlFeatures()
                // mostly used internally by Jexl
                .register(false) // default false
                // allow usage of let, const, and var variables
                .localVar(false) // default true
                // allow side-effect operators (e.g. +=, -=, |=, <<=, etc) for global vars.  needed to assign values (e.g. _Value_ = true)
                .sideEffectGlobal(true) // default true
                // allow side-effect operators (e.g. +=, -=, |=, <<=, etc).  needed to assign values (e.g. _Value_ = true)
                .sideEffect(true) // default true
                // allow array indexing via reference expression (e.g. array[some expression])
                .arrayReferenceExpr(false) // default true
                // allow method calls on expressions
                .methodCall(true) // default true
                // allow array/map/set literal expressions (e.g. [1, 2, "three"], {"one": 1, "two": 2}, {"one", 2, "more"}
                .structuredLiteral(false) // default true
                // allow creation of new instances
                .newInstance(false) // default true
                // allow loop constructs
                .loops(false) // default true
                // allow lambda/function constructs (not the same as namespaced functions)
                .lambda(false) // default true
                // allow thin-arrow lambda syntax (e.g. ->)
                .thinArrow(false) // default true
                // allow fat-arrow lambda syntax (e.g. =>)
                .fatArrow(false) // default false
                // allow comparator names (e.g. eq, ne, lt, gt, etc)
                .comparatorNames(false) // default true
                // allow pragma constructs (e.g. #pragma some.option some-value)
                .pragma(false) // default true
                // allow pragma constructs anywhere in the code
                .pragmaAnywhere(false) // default true
                // allow namespace pragmas (e.g. '#pragma jexl.namespace.str java.lang.String' to use 'str' in place of 'java.lang.String')
                .namespacePragma(false) // default true
                // allow import pragmas (e.g. '#pragma jexl.import java.net' to use new URL() instead of new java.net.URL())
                .importPragma(false) // default true
                // allow annotations
                .annotation(false) // default true
                // allow multiple semicolon-terminated expressions per jexl string
                .script(false) // default true
                // whether redefining local variables is an error
                .lexical(false) // default false
                // whether local variables shade global variables
                .lexicalShade(false); // default false
        // @formatter: on
    }

    /**
     * Parse a query string using a JEXL parser and transform it into a parse tree of our RefactoredDatawaveTreeNodes. This also sets all convenience maps that
     * the analyzer provides.
     *
     * @param query
     *            The query string in JEXL syntax to parse
     * @return Root node of the query parse tree.
     * @throws ParseException
     *             when unable to parse Jexl Query
     */
    public static ASTJexlScript parseJexlQuery(String query) throws ParseException {
        // Instantiate a parser and visitor
        Parser parser = new Parser(new StringProvider(";"));

        // lowercase all 'and', 'or', and 'not' portions of the query.
        String caseFixQuery = AND_PATTERN.matcher(query).replaceAll(" and ");
        caseFixQuery = OR_PATTERN.matcher(caseFixQuery).replaceAll(" or ");
        caseFixQuery = NOT_PATTERN.matcher(caseFixQuery).replaceAll(" not ");

        if (caseFixQuery.contains(DOUBLE_BACKSLASH)) {
            try {
                return parseQueryWithBackslashes(query, parser);
            } catch (Exception e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                        "Unable to perform backslash substitution while parsing the query: " + e.getMessage());
                throw new IllegalArgumentException(qe);
            }
        } else {
            // Parse the original query
            try {
                return parser.parse(null, jexlFeatures(), caseFixQuery, null);
            } catch (TokenMgrException | JexlException e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                        "Unable to parse the query: " + e.getMessage());
                throw new IllegalArgumentException(qe);
            }
        }
    }

    // generate a random alphanumeric placeholder value which will replace instances
    // of double backslashes in the query before parsing. This algorithm ensures that
    // the placeholder string does not exist in the original query.
    private static String generatePlaceholder(String query) {
        String placeholder;
        do {
            placeholder = RandomStringUtils.randomAlphanumeric(4);
        } while (query.contains(placeholder));

        return "_" + placeholder + "_";
    }

    // we need to replace double backslashes in the query with a placeholder value
    // before parsing in order to prevent the parser from interpreting doubles as singles
    private static ASTJexlScript parseQueryWithBackslashes(String query, Parser parser) throws Exception {
        // determine how many doubles need to be replaced
        int numFound = StringUtils.countMatches(query, DOUBLE_BACKSLASH);

        // replace the doubles with a unique placeholder
        String placeholder = generatePlaceholder(query);
        query = query.replace(DOUBLE_BACKSLASH, placeholder);

        // Parse the query with the placeholders
        ASTJexlScript jexlScript;
        try {
            jexlScript = parser.parse(null, jexlFeatures(), query, null);
        } catch (TokenMgrException e) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                    "Unable to parse the query: " + e.getMessage());
            throw new IllegalArgumentException(qe);
        }

        Deque<JexlNode> workingStack = new LinkedList<>();
        workingStack.push(jexlScript);
        int numReplaced = 0;

        // iteratively traverse the tree, and replace the placeholder with single or double backslashes
        while (!workingStack.isEmpty()) {
            JexlNode node = workingStack.pop();

            if (node instanceof ASTStringLiteral) {
                ASTStringLiteral stringLiteralNode = (ASTStringLiteral) node;
                int numToReplace = StringUtils.countMatches(stringLiteralNode.getLiteral(), placeholder);
                if (numToReplace > 0) {
                    // get the parent node
                    JexlNode parent = node.jjtGetParent();

                    // if not a regex, use single backslash. otherwise, use double.
                    // this is necessary to ensure that non-regex nodes use the escaped
                    // value when determining equality, and to ensure that regex nodes
                    // use the pre-compiled form of the string literal.
                    if (!(parent instanceof ASTERNode || parent instanceof ASTNRNode))
                        JexlNodes.setLiteral(stringLiteralNode, stringLiteralNode.getLiteral().replace(placeholder, SINGLE_BACKSLASH));
                    else
                        JexlNodes.setLiteral(stringLiteralNode, stringLiteralNode.getLiteral().replace(placeholder, DOUBLE_BACKSLASH));
                    numReplaced += numToReplace;
                }
            }

            if (node.jjtGetNumChildren() > 0) {
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode child = node.jjtGetChild(i);
                    if (child != null) {
                        workingStack.push(child);
                    }
                }
            }
        }

        if (numFound != numReplaced) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                    "Did not find the expected number of backslash placeholders in the query. Expected: " + numFound + ", Actual: " + numReplaced);
            throw new IllegalArgumentException(qe);
        }
        return jexlScript;
    }

    /**
     * Fetch the literal off of the grandchild. Returns null if there is no literal
     *
     * @param node
     *            to get the literal of
     * @return literal node
     * @throws NoSuchElementException
     *             if child doesn't exist
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
                    if (isLiteral(child)) {
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
     *            the node to check
     * @return true if the child is a literal
     */
    public static boolean isLiteral(final JexlNode child) {
        //  @formatter:off
        return child instanceof ASTTrueNode ||
                child instanceof ASTFalseNode ||
                child instanceof ASTNullLiteral ||
                child instanceof JexlNode.Constant;   //  also true for ASTNumberLiteral
        //  @formatter:on
    }

    /**
     * Fetch the literal off of the grandchild. Throws an exception if there is no literal
     *
     * @param node
     *            the node
     * @return the literal
     * @throws NoSuchElementException
     *             if the child doesn't exist
     */
    @SuppressWarnings("rawtypes")
    public static Object getLiteralValue(JexlNode node) throws NoSuchElementException {
        Object literal = getLiteral(node);
        // If the grandchild and its image is non-null and equal to the any-field identifier
        if (literal instanceof JexlNode.Constant) {
            return ((JexlNode.Constant) literal).getLiteral();
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
     * Fetch the literal off of the grandchild safely. Return null if there's an exception.
     *
     * @param node
     *            the node
     * @return the literal value
     */
    @SuppressWarnings("rawtypes")
    public static Object getLiteralValueSafely(JexlNode node) {
        try {
            return getLiteralValue(node);
        } catch (NoSuchElementException nsee) {
            return null;
        }
    }

    /**
     * Fetch the identifier off of the grandchild, removing a leading {@link #IDENTIFIER_PREFIX} if present. Throws an exception if there is no identifier This
     * identifier will be deconstructed
     *
     * @param node
     *            the node
     * @return the deconstructed identifier
     * @throws NoSuchElementException
     *             if the child doesn't exist
     */
    public static String getIdentifier(JexlNode node) throws NoSuchElementException {
        return getIdentifier(node, true);
    }

    /**
     * Fetch the identifier off of the grandchild, removing a leading {@link #IDENTIFIER_PREFIX} if present. Throws an exception if there is no identifier
     *
     * @param node
     *            the node
     * @param deconstruct
     *            flag to turn on/off option
     * @return the identifier, deconstructed if requested
     * @throws NoSuchElementException
     *             if the child doesn't exist
     */
    public static String getIdentifier(JexlNode node, boolean deconstruct) throws NoSuchElementException {
        if (null != node && 2 == node.jjtGetNumChildren()) {
            JexlNode child = node.jjtGetChild(0);

            if (child instanceof ASTIdentifier) {
                // as of jexl3, identifiers are NOT parented by ASTReference nodes unless they are 'ant-style' and contain '.' characters
                return getIdentifier((ASTIdentifier) child, deconstruct);
            } else {
                return null;
            }
        } else if (node instanceof ASTIdentifier && node.jjtGetNumChildren() == 0) {
            return deconstructIdentifier(JexlNodes.getIdentifierOrLiteralAsString(node), deconstruct);
        }

        NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.IDENTIFIER_MISSING);
        throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
    }

    public static String getIdentifier(ASTIdentifier identifier, boolean deconstruct) {
        return (deconstruct ? deconstructIdentifier(identifier.getName()) : identifier.getName());
    }

    /**
     * Finds all the functions and returns a map indexed by function context name to the function.
     *
     * @param node
     *            a node in the query
     * @return a multimap of namespaces to functions
     */
    public static Multimap<String,Function> getFunctions(JexlNode node) {
        return FunctionReferenceVisitor.functions(node);
    }

    /**
     * Finds all the functions and returns a map indexed by function context name to the function.
     *
     * @param node
     *            a node in the query
     * @param namespaceFilter
     *            a filter that limits the returned functions
     * @return a multimap of namespaces to functions
     */
    public static Multimap<String,Function> getFunctions(JexlNode node, Set<String> namespaceFilter) {
        return FunctionReferenceVisitor.functions(node, namespaceFilter);
    }

    public static List<ASTIdentifier> getFunctionIdentifiers(ASTFunctionNode node) {
        Preconditions.checkNotNull(node);
        Preconditions.checkArgument(node.jjtGetNumChildren() == 2);
        Preconditions.checkArgument(node.jjtGetChild(1) instanceof ASTArguments);

        ASTArguments argsNode = (ASTArguments) node.jjtGetChild(1);
        List<ASTIdentifier> identifiers = Lists.newArrayList();

        for (int i = 0; i < argsNode.jjtGetNumChildren(); i++) {
            identifiers.addAll(getIdentifiers(argsNode.jjtGetChild(i)));
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
            names.add(identifier.getName());
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
            // the first child of a method node is typically the method name.
            // identifiers may exist as arguments to methods in later children
            for (int i = 1; i < node.jjtGetNumChildren(); i++) {
                getIdentifiers(node.jjtGetChild(i), identifiers);
            }
            return;
        } else if (node instanceof ASTAssignment) {
            // Don't get identifiers under assignments as they are only used for QueryPropertyMarkers
            return;
        } else if (node instanceof ASTIdentifier) {
            identifiers.add((ASTIdentifier) node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                getIdentifiers(node.jjtGetChild(i), identifiers);
            }
        }
    }

    /**
     * Unwraps ASTReferenceExpressions from a JexlNode
     *
     * @param node
     *            a JexlNode
     * @return an unwrapped JexlNode
     */
    public static JexlNode dereference(JexlNode node) {
        while (node.jjtGetNumChildren() == 1 && node instanceof ASTReferenceExpression) {
            node = node.jjtGetChild(0);
        }
        return node;
    }

    /**
     * Unwraps ASTReferenceExpressions from a JexlNode. If the final node is a MarkerNode, wrap it
     *
     * @param node
     *            a JexlNode
     * @return an unwrapped JexlNode
     */
    public static JexlNode dereferenceSafely(JexlNode node) {
        JexlNode unwrapped = dereference(node);

        if (QueryPropertyMarker.findInstance(unwrapped).isAnyType()) {
            // ensure we create a proper ref -> refExpr chain
            unwrapped = wrap(unwrapped);
        }

        return unwrapped;
    }

    /**
     * This is the opposite of dereference in that this will climb back up reference expression nodes that only contain one child.
     *
     * @param node
     *            a JexlNode
     * @return the parent reference/referenceexpression or this node
     */
    public static JexlNode rereference(JexlNode node) {
        while (node.jjtGetParent() != null && node.jjtGetParent().jjtGetNumChildren() == 1 && node.jjtGetParent() instanceof ASTReferenceExpression) {
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
                node = InvertNodeVisitor.invertSwappedNodes(node);
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
        return deconstructIdentifier(identifier.getName());
    }

    /**
     * Remove the {@link #IDENTIFIER_PREFIX} from the beginning of a fieldName if it exists
     *
     * @param fieldName
     *            fieldName to deconstruct
     * @return the deconstructed identifier
     */
    public static String deconstructIdentifier(String fieldName) {
        return deconstructIdentifier(fieldName, false);
    }

    /**
     * Remove the {@link #IDENTIFIER_PREFIX} from the beginning of a fieldName if it exists and remove the {@link #GROUPING_CHARACTER_SEPARATOR} from the end of
     * a fieldName if it exists
     *
     * @param fieldName
     *            fieldName to deconstruct
     * @param includeGroupingContext
     *            flag for including/excluding grouping context
     * @return the deconstructed identifier
     */
    public static String deconstructIdentifier(String fieldName, boolean includeGroupingContext) {
        if (fieldName != null && fieldName.length() > 1) {
            boolean idPrefix = fieldName.charAt(0) == IDENTIFIER_PREFIX;
            int startIndex = idPrefix ? 1 : 0;
            int stopLength = idPrefix ? fieldName.length() - 1 : fieldName.length();

            if (!includeGroupingContext) {
                int groupingOffset = fieldName.indexOf(GROUPING_CHARACTER_SEPARATOR);
                if (groupingOffset != -1) {
                    stopLength = idPrefix ? groupingOffset - 1 : groupingOffset;
                }
            }

            if (startIndex != 0 || stopLength != -1) {
                fieldName = new String(fieldName.getBytes(), startIndex, stopLength);
            }
        }

        return fieldName;
    }

    /**
     * Rebuild the identifier with the {@link #IDENTIFIER_PREFIX} if the identifier starts with an invalid character per the Jexl IDENTIFIER definition
     *
     * @param fieldName
     *            the identifier to rebuild
     * @return the rebuilt identifier
     */
    public static String rebuildIdentifier(String fieldName) {
        return rebuildIdentifier(fieldName, false);
    }

    /**
     * Rebuild the identifier with the {@link #IDENTIFIER_PREFIX} if the identifier starts with an invalid character per the Jexl IDENTIFIER definition
     *
     * @param fieldName
     *            the fieldname to rebuild
     * @param includeGroupingContext
     *            flag to include/exclude grouping context
     * @return the rebuilt identifier
     */
    public static String rebuildIdentifier(String fieldName, boolean includeGroupingContext) {
        // fieldName may be null if it is from a Function node
        if (fieldName != null && fieldName.length() > 1) {
            if (!includeGroupingContext) {
                fieldName = removeGroupingContext(fieldName);
            }

            char firstChar = fieldName.charAt(0);

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
            if (!isNodeNegated(n)) {
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
            return numNegations % 2 != 0;
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

    /**
     * Iterate through provided node and its children, then return a list of nodes that are an instance of ASTEQNode.
     *
     * @param node
     *            a JexlNode
     * @return List of ASTEQNode nodes.
     */
    public static List<ASTEQNode> getEQNodes(JexlNode node) {
        List<ASTEQNode> eqNodes = Lists.newArrayList();

        getEQNodes(node, eqNodes);

        return eqNodes;
    }

    /**
     * Check if the provided node is an instance of ASTEQNode. If yes, then add the node to the provided list.
     *
     * @param node
     *            a JexlNode
     * @param eqNodes
     *            List of nodes to potentially add the provided node to
     */
    private static void getEQNodes(JexlNode node, List<ASTEQNode> eqNodes) {
        if (node instanceof ASTEQNode) {
            eqNodes.add((ASTEQNode) node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                getEQNodes(node.jjtGetChild(i), eqNodes);
            }
        }
    }

    /**
     * Iterate through provided node and its children, then return a list of nodes that are an instance of ASTERNode.
     *
     * @param node
     *            a JexlNode
     * @return List of ASTERNode nodes.
     */
    public static List<ASTERNode> getERNodes(JexlNode node) {
        List<ASTERNode> erNodes = Lists.newArrayList();

        getERNodes(node, erNodes);

        return erNodes;
    }

    /**
     * Check if the provided node is an instance of ASTERNode. If yes, then add the node to the provided list.
     *
     * @param node
     *            a JexlNode
     * @param erNodes
     *            List of nodes to potentially add the provided node to
     */
    private static void getERNodes(JexlNode node, List<ASTERNode> erNodes) {
        if (node instanceof ASTERNode) {
            erNodes.add((ASTERNode) node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                getERNodes(node.jjtGetChild(i), erNodes);
            }
        }
    }

    /**
     * Iterate through provided node and its children, then return a list of nodes matching the provided class. The provided class must extend JexlNode.
     *
     * @see org.apache.commons.jexl3.parser.JexlNode
     *
     * @param node
     *            a JexlNode
     * @param typeKey
     *            class type to check node against
     * @param <T>
     *            class extending JexlNode
     *
     * @return List of nodes matching provided class.
     */
    public static <T extends JexlNode> List<T> getNodesOfType(JexlNode node, Class<T> typeKey) {
        List<T> nodes = Lists.newArrayList();

        getNodesOfType(node, nodes, typeKey);

        return nodes;
    }

    /**
     * Check if the provided node is an instance of the provided class. If yes, then add the node to the provided list. The provided class must extend JexlNode.
     *
     * @see org.apache.commons.jexl3.parser.JexlNode
     *
     * @param node
     *            a JexlNode
     * @param nodes
     *            list of JexlNodes to add node to, potentially
     * @param typeKey
     *            class to check node against
     * @param <T>
     *            class extending JexlNode
     */
    @SuppressWarnings("unchecked")
    private static <T extends JexlNode> void getNodesOfType(JexlNode node, List<T> nodes, Class<T> typeKey) {
        if (typeKey.isInstance(node)) {
            nodes.add((T) node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                getNodesOfType(node.jjtGetChild(i), nodes, typeKey);
            }
        }
    }

    public static List<Object> getLiteralValues(JexlNode node) {
        return getLiterals(node).stream().map(JexlASTHelper::getLiteralValue).collect(Collectors.toList());
    }

    public static List<JexlNode> getLiterals(JexlNode node) {
        return getLiterals(node, Lists.newLinkedList());
    }

    private static List<JexlNode> getLiterals(JexlNode node, List<JexlNode> literals) {
        if (node instanceof ASTAssignment) {
            // Don't get literals under assignments as they are only used for QueryPropertyMarkers
        } else if (isLiteral(node)) {
            literals.add(node);
        } else {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                literals = getLiterals(node.jjtGetChild(i), literals);
            }
        }

        return literals;
    }

    public static Map<String,Object> getAssignments(JexlNode node) {
        return getAssignments(node, Maps.newHashMap());
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
     * Ranges: A range prior to being "tagged" must be of the form "(term1 &amp;&amp; term2)" where term1 and term2 refer to the same field and denote two sides
     * of the range ((LE or LT) and (GE or GT)). A tagged range is of the form "(BoundedRange=true) &amp;&amp; (term1 &amp;&amp; term2))"
     *
     * @return the range
     */
    public static RangeFinder findRange() {
        return new RangeFinder();
    }

    public static class RangeFinder {
        boolean includeDelayed = true;
        MetadataHelper helper = null;
        Set<String> dataTypeFilter = null;
        boolean recursive = false;
        boolean withMarker = true;

        public RangeFinder notDelayed() {
            includeDelayed = false;
            return this;
        }

        public RangeFinder indexedOnly(Set<String> dataTypeFilter, MetadataHelper helper) {
            this.dataTypeFilter = dataTypeFilter;
            this.helper = helper;
            return this;
        }

        public RangeFinder recursively() {
            this.recursive = true;
            return this;
        }

        public RangeFinder notMarked() {
            this.withMarker = false;
            return this;
        }

        public boolean isRange(JexlNode node) {
            return getRange(node) != null;
        }

        public LiteralRange<?> getRange(JexlNode node) {
            LiteralRange<?> range = _getRange(node);
            if (range == null && recursive) {
                for (int i = 0; range == null && i < node.jjtGetNumChildren(); i++) {
                    range = getRange(node.jjtGetChild(i));
                }
            }
            return range;
        }

        private LiteralRange<?> _getRange(JexlNode node) {
            QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
            boolean marked = instance.isType(BOUNDED_RANGE);

            // first unwrap any delayed expression except for a tag
            if (includeDelayed && !marked && instance.isAnyType()) {
                node = instance.getSource();
                instance = QueryPropertyMarker.findInstance(node);
                marked = instance.isType(BOUNDED_RANGE);
            }

            // It must be marked
            if (withMarker && !marked) {
                return null;
            }

            // remove the marker
            if (marked) {
                node = instance.getSource();
            }

            // remove reference and expression nodes
            node = dereference(node);

            // must be an and node at this point
            if (!(node instanceof ASTAndNode)) {
                if (marked)
                    throw new DatawaveFatalQueryException("A bounded range must contain an AND node with two bounds");
                return null;
            }

            // and has exactly two children
            if (node.jjtGetNumChildren() != 2) {
                if (marked)
                    throw new DatawaveFatalQueryException("A bounded range must contain two bounds");
                return null;
            }

            JexlNode child1 = dereference(node.jjtGetChild(0));
            JexlNode child2 = dereference(node.jjtGetChild(1));

            // and the fieldnames match
            String fieldName1 = null;
            String fieldName2 = null;
            try {
                fieldName1 = JexlASTHelper.getIdentifier(child1);
                fieldName2 = JexlASTHelper.getIdentifier(child2);
            } catch (NoSuchElementException ignored) {}
            if (fieldName1 == null || fieldName2 == null || !fieldName1.equals(fieldName2)) {
                if (marked)
                    throw new DatawaveFatalQueryException("A bounded range must contain two bounds against the same field");
                return null;
            }

            // and is indexed (if we care) {
            try {
                if (helper != null && !helper.isIndexed(fieldName1, dataTypeFilter)) {
                    return null;
                }
            } catch (TableNotFoundException tnfe) {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.TABLE_NOT_FOUND, tnfe);
                throw new DatawaveFatalQueryException(qe);
            }

            Object literal1 = null;
            Object literal2 = null;
            try {
                literal1 = JexlASTHelper.getLiteralValue(child1);
                literal2 = JexlASTHelper.getLiteralValue(child2);
            } catch (NoSuchElementException ignored) {}

            if (literal1 == null || literal2 == null) {
                if (marked)
                    throw new DatawaveFatalQueryException("A bounded range must contain two bounds with literals");
                return null;
            }

            LiteralRange<?> range = null;
            JexlNode[] children = new JexlNode[] {child1, child2};
            if (literal1 instanceof String || literal2 instanceof String) {
                range = getStringBoundedRange(children, new LiteralRange<>(fieldName1, LiteralRange.NodeOperand.AND));
            } else if (literal1 instanceof BigDecimal || literal2 instanceof BigDecimal) {
                range = getBigDecimalBoundedRange(children, new LiteralRange<>(fieldName1, LiteralRange.NodeOperand.AND));
            } else if (literal1 instanceof Double || literal2 instanceof Double) {
                range = getDoubleBoundedRange(children, new LiteralRange<>(fieldName1, LiteralRange.NodeOperand.AND));
            } else if (literal1 instanceof Float || literal2 instanceof Float) {
                range = getFloatBoundedRange(children, new LiteralRange<>(fieldName1, LiteralRange.NodeOperand.AND));
            } else if (literal1 instanceof BigInteger || literal2 instanceof BigInteger) {
                range = getBigIntegerBoundedRange(children, new LiteralRange<>(fieldName1, LiteralRange.NodeOperand.AND));
            } else if (literal1 instanceof Long || literal2 instanceof Long) {
                range = getLongBoundedRange(children, new LiteralRange<>(fieldName1, LiteralRange.NodeOperand.AND));
            } else if (literal1 instanceof Integer || literal2 instanceof Integer) {
                range = getIntegerBoundedRange(children, new LiteralRange<>(fieldName1, LiteralRange.NodeOperand.AND));
            } else {
                QueryException qe = new QueryException(DatawaveErrorCode.NODE_LITERAL_TYPE_ASCERTAIN_ERROR, MessageFormat.format("{0}", literal1));
                throw new DatawaveFatalQueryException(qe);
            }

            if (range.isBounded()) {
                return range;
            }

            if (marked)
                throw new DatawaveFatalQueryException("A bounded range must contain bounds with comparable types");
            return null;
        }
    }

    public static LiteralRange<String> getStringBoundedRange(JexlNode[] children, LiteralRange<String> range) {
        for (int i = 0; i < 2; i++) {
            JexlNode child = children[i];
            String newFieldName = JexlASTHelper.getIdentifier(child);

            if (range.getFieldName().equals(newFieldName)) {
                String literal = String.valueOf(JexlASTHelper.getLiteralValue(child));

                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, true, child);
                    } else {
                        range.updateLower(literal, true, child);
                    }
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, false, child);
                    } else {
                        range.updateLower(literal, false, child);
                    }
                } else {
                    log.warn("Could not determine class of node: " + child);
                }
            }
        }

        return range;
    }

    public static LiteralRange<Integer> getIntegerBoundedRange(JexlNode[] children, LiteralRange<Integer> range) {
        for (int i = 0; i < 2; i++) {
            JexlNode child = children[i];
            String newFieldName = JexlASTHelper.getIdentifier(child);

            if (range.getFieldName() == null || range.getFieldName().equals(newFieldName)) {
                Integer literal = (Integer) JexlASTHelper.getLiteralValue(child);

                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, true, child);
                    } else {
                        range.updateLower(literal, true, child);
                    }
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, false, child);
                    } else {
                        range.updateLower(literal, false, child);
                    }
                } else {
                    log.warn("Could not determine class of node: " + child);
                }
            }
        }

        return range;
    }

    public static LiteralRange<Long> getLongBoundedRange(JexlNode[] children, LiteralRange<Long> range) {
        for (int i = 0; i < 2; i++) {
            JexlNode child = children[i];
            String newFieldName = JexlASTHelper.getIdentifier(child);

            if (range.getFieldName() == null || range.getFieldName().equals(newFieldName)) {
                Long literal = (Long) JexlASTHelper.getLiteralValue(child);

                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, true, child);
                    } else {
                        range.updateLower(literal, true, child);
                    }
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, false, child);
                    } else {
                        range.updateLower(literal, false, child);
                    }
                } else {
                    log.warn("Could not determine class of node: " + child);
                }
            }
        }

        return range;
    }

    public static LiteralRange<BigInteger> getBigIntegerBoundedRange(JexlNode[] children, LiteralRange<BigInteger> range) {
        for (int i = 0; i < 2; i++) {
            JexlNode child = children[i];
            String newFieldName = JexlASTHelper.getIdentifier(child);

            if (range.getFieldName().equals(newFieldName)) {
                BigInteger literal = (BigInteger) JexlASTHelper.getLiteralValue(child);

                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, true, child);
                    } else {
                        range.updateLower(literal, true, child);
                    }
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, false, child);
                    } else {
                        range.updateLower(literal, false, child);
                    }
                } else {
                    log.warn("Could not determine class of node: " + child);
                }
            }
        }

        return range;
    }

    public static LiteralRange<Float> getFloatBoundedRange(JexlNode[] children, LiteralRange<Float> range) {
        for (int i = 0; i < 2; i++) {
            JexlNode child = children[i];
            String newFieldName = JexlASTHelper.getIdentifier(child);

            if (range.getFieldName().equals(newFieldName)) {
                Float literal = (Float) JexlASTHelper.getLiteralValue(child);

                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, true, child);
                    } else {
                        range.updateLower(literal, true, child);
                    }
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, false, child);
                    } else {
                        range.updateLower(literal, false, child);
                    }
                } else {
                    log.warn("Could not determine class of node: " + child);
                }
            }
        }

        return range;
    }

    public static LiteralRange<Double> getDoubleBoundedRange(JexlNode[] children, LiteralRange<Double> range) {
        for (int i = 0; i < 2; i++) {
            JexlNode child = children[i];
            String newFieldName = JexlASTHelper.getIdentifier(child);

            if (range.getFieldName().equals(newFieldName)) {
                Double literal = (Double) JexlASTHelper.getLiteralValue(child);

                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, true, child);
                    } else {
                        range.updateLower(literal, true, child);
                    }
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, false, child);
                    } else {
                        range.updateLower(literal, false, child);
                    }
                } else {
                    log.warn("Could not determine class of node: " + child);
                }
            }
        }

        return range;
    }

    public static LiteralRange<BigDecimal> getBigDecimalBoundedRange(JexlNode[] children, LiteralRange<BigDecimal> range) {
        for (int i = 0; i < 2; i++) {
            JexlNode child = children[i];
            String newFieldName = JexlASTHelper.getIdentifier(child);

            if (range.getFieldName().equals(newFieldName)) {
                BigDecimal literal = (BigDecimal) JexlASTHelper.getLiteralValue(child);

                if (INCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, true, child);
                    } else {
                        range.updateLower(literal, true, child);
                    }
                } else if (EXCLUSIVE_RANGE_NODE_CLASSES.contains(child.getClass())) {
                    if (LESS_THAN_NODE_CLASSES.contains(child.getClass())) {
                        range.updateUpper(literal, false, child);
                    } else {
                        range.updateLower(literal, false, child);
                    }
                } else {
                    log.warn("Could not determine class of node: " + child);
                }
            }
        }

        return range;
    }

    /**
     * Iterate through provided node's ancestors and return true if any are an instance of ASTOrNode.
     *
     * @see org.apache.commons.jexl3.parser.ASTOrNode
     *
     * @param node
     *            a JexlNode
     * @return True if any ancestor is an instance of ASTOrNode. If not, then False.
     */
    public static boolean isDescendantOfOr(JexlNode node) {
        if (null != node && null != node.jjtGetParent()) {
            JexlNode parent = node.jjtGetParent();

            if (parent instanceof ASTOrNode) {
                return true;
            }

            return isDescendantOfOr(parent);
        }

        return false;
    }

    /**
     * Iterate through provided node's ancestors and return true if any are an instance of ASTNotNode.
     *
     * @see org.apache.commons.jexl3.parser.ASTNotNode
     *
     * @param node
     *            a JexlNode
     * @return True if any ancestor is an instance of ASTNotNode. If not, then False.
     */
    public static boolean isDescendantOfNot(JexlNode node) {
        if (null != node && null != node.jjtGetParent()) {
            JexlNode parent = node.jjtGetParent();

            if (parent instanceof ASTNotNode) {
                return true;
            }

            return isDescendantOfNot(parent);
        }

        return false;
    }

    /**
     * Iterate through provided node's ancestors and return true if any are an instance of ASTAndNode.
     *
     * @see org.apache.commons.jexl3.parser.ASTAndNode
     *
     * @param node
     *            a JexlNode
     * @return True if any ancestor is an instance of ASTAndNode. If not, then False.
     */
    public static boolean isDescendantOfAnd(JexlNode node) {
        if (null != node && null != node.jjtGetParent()) {
            JexlNode parent = node.jjtGetParent();

            if (parent instanceof ASTAndNode) {
                return true;
            }

            return isDescendantOfAnd(parent);
        }

        return false;
    }

    /**
     * Iterate through provided node's ancestors and return true if any are an instance of the provided class. The provided class must extend JexlNode.
     *
     * @see org.apache.commons.jexl3.parser.JexlNode
     *
     * @param node
     *            a JexlNode
     * @param typeKey
     *            class type to match against
     * @param <T>
     *            class extending JexlNode
     *
     * @return True if any ancestor is an instance of the provided class. If not, then False.
     */
    public static <T extends JexlNode> boolean isDescendantOfNodeType(JexlNode node, Class<T> typeKey) {
        if (null != node && null != node.jjtGetParent()) {
            JexlNode parent = node.jjtGetParent();

            if (typeKey.isInstance(parent)) {
                return true;
            }

            return isDescendantOfNodeType(parent, typeKey);
        }

        return false;
    }

    /**
     * Iterate through all descendants of the provided node and return true if any are an instance of the provided class. The provided class must extend
     * JexlNode.
     *
     * @see org.apache.commons.jexl3.parser.JexlNode
     *
     * @param node
     *            a JexlNode
     * @param typeKey
     *            class type to match against
     * @param <T>
     *            class extending JexlNode
     *
     * @return True if any descendant is an instance of the provided class. If not, then False.
     */
    public static <T extends JexlNode> boolean descendantsContainNodeType(JexlNode node, Class<T> typeKey) {
        if (node != null) {
            int numChildren = node.jjtGetNumChildren();

            for (int i = 0; i < numChildren; i++) {
                JexlNode child = node.jjtGetChild(i);

                if (typeKey.isInstance(child) || descendantsContainNodeType(child, typeKey)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Performs an order-dependent AST equality check
     *
     * @param one
     *            a JexlNode
     * @param two
     *            a JexlNode
     * @return true if equal, false if not
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
        } else if (one instanceof JexlNode.Constant) {
            JexlNode.Constant<?> oneLit = (JexlNode.Constant<?>) one, twoLit = (JexlNode.Constant<?>) two;
            if (!oneLit.getLiteral().equals(twoLit.getLiteral())) {
                return false;
            }
        } else if (one instanceof ASTIdentifier) {
            if (!JexlNodes.getIdentifierOrLiteral(one).equals(JexlNodes.getIdentifierOrLiteral(two))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Generate a key for the given JexlNode. This may be used to determine node equality.
     * <p>
     * Original Comment: <code>
     * // Note: This method assumes that the node passed in is already flattened.
     * // If not, and our tree contains functionally equivalent subtrees, we would
     * // be duplicating some of our efforts which is bad, m'kay?
     * </code>
     *
     * @param node
     *            a JexlNode.
     * @return a key for the node.
     */
    public static String nodeToKey(JexlNode node) {
        return JexlStringBuildingVisitor.buildQueryWithoutParse(node, true);
    }

    /**
     * <p>
     * When at an operand, this method will find the first Identifier and replace its {image} value with the supplied {String}. This is intended to be used when
     * the query model is being supplied and we want to replace the field name in some expression.
     * </p>
     * <p>
     * This method returns a new operand node with an updated {Identifier}.
     * </p>
     * If neither of the operand's children are an {Identifier}, then an {IllegalArgumentException} is thrown.
     *
     * @param operand
     *            a JexlNode
     * @param field
     *            the new field name
     * @param <T>
     *            class extending JexlNode
     *
     * @return node with field set
     */
    public static <T extends JexlNode> T setField(T operand, String field) {
        ASTIdentifier identifier = findIdentifier(operand);
        if (identifier == null) {
            throw new IllegalArgumentException();
        } else {
            JexlNodes.setIdentifier(identifier, JexlASTHelper.rebuildIdentifier(field));
            return operand;
        }
    }

    private static ASTIdentifier findIdentifier(JexlNode node) {
        if (node instanceof ASTIdentifier) {
            return (ASTIdentifier) node;
        }

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            ASTIdentifier test = findIdentifier(node.jjtGetChild(i));
            if (test != null) {
                return test;
            }
        }

        return null;
    }

    public static ASTStringLiteral normalizeLiteral(JexlNode literal, Type<?> normalizer) {
        String normalizedImg = normalizer.normalize(JexlNodes.getIdentifierOrLiteralAsString(literal));
        ASTStringLiteral normalizedLiteral = JexlNodes.makeStringLiteral();
        JexlNodes.setLiteral(normalizedLiteral, normalizedImg);
        return normalizedLiteral;
    }

    public static JexlNode findLiteral(JexlNode node) {
        if (node instanceof JexlNode.Constant<?>) {
            return node;
        }

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode test = findLiteral(node.jjtGetChild(i));
            if (test instanceof JexlNode.Constant<?>) {
                return test;
            }
        }

        return null;
    }

    public static <T extends JexlNode> T swapLiterals(T operand, ASTStringLiteral literal) {
        JexlNode oldLiteral = findLiteral(operand);
        // we need the direct child of this operand (should be at most one level too deep)
        while (oldLiteral.jjtGetParent() != operand) {
            oldLiteral = oldLiteral.jjtGetParent();
        }
        return JexlNodes.swap(operand, oldLiteral, literal);
    }

    public static <T extends JexlNode> T applyNormalization(T operand, Type<?> normalizer) {
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
     *            a JexlNode
     * @return if it is literal or not
     */
    public static boolean isLiteral(Object node) {
        if (null == node) {
            return false;
        }

        Class<?> clz = node.getClass();
        return JexlNode.Constant.class.isAssignableFrom(clz) || ASTTrueNode.class.isAssignableFrom(clz) || ASTFalseNode.class.isAssignableFrom(clz)
                        || ASTNullLiteral.class.isAssignableFrom(clz);

    }

    /**
     * Check if the provided JexlNode is an ASTEQNode and is of the form `identifier eq literal`
     *
     * @param node
     *            a JexlNode
     * @return if it is a literal
     */
    public static boolean isLiteralEquality(JexlNode node) {
        Preconditions.checkNotNull(node);

        if (node instanceof ASTEQNode && node.jjtGetNumChildren() == 2) {
            List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
            List<JexlNode> literals = JexlASTHelper.getLiterals(node);
            return identifiers.size() == 1 && literals.size() == 1;
        }

        return false;
    }

    /**
     * Determine if the given ASTEQNode is indexed based off of the Multimap of String fieldname to TextNormalizer.
     *
     * @param node
     *            a JexlNode
     * @param config
     *            query configuration
     * @return if it is indexed
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
     *            a JexlNode
     * @param config
     *            query configuration
     * @param stats
     *            index stats client
     * @return the selectivity of the node's identifier
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
     *            Set of field names
     * @param config
     *            shard query configuration
     * @param stats
     *            the IndexStatsClient
     * @return the selectivity of the node's identifier
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
     * Returns whether the tree contains any null children, children with null parents, or children with conflicting parentage.
     *
     * @param rootNode
     *            the tree to validate
     * @param failHard
     *            whether to throw an exception if validation fails
     * @return true if valid, false otherwise
     */
    public static boolean validateLineage(JexlNode rootNode, boolean failHard) {
        return validateLineageVerbosely(rootNode, failHard).isValid();
    }

    /**
     * Checks to see if the tree contains any null children, children with null parents, or children with conflicting parentage, and returns a
     * {@link LineageValidation} with any identified violations.
     *
     * @param rootNode
     *            the tree to validate
     * @param failHard
     *            if true, throws an exception when a violation is encountered for the first time
     * @return the {@link LineageValidation}
     */
    public static LineageValidation validateLineageVerbosely(JexlNode rootNode, boolean failHard) {
        // Prepare a working stack to iterate through.
        Deque<JexlNode> workingStack = new LinkedList<>();
        workingStack.push(rootNode);

        LineageValidation validation = new LineageValidation();

        // Go through all the nodes from parent to children, and ensure that parent and child relationships are correct.
        while (!workingStack.isEmpty()) {
            JexlNode node = workingStack.pop();

            if (node.jjtGetNumChildren() > 0) {
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode child = node.jjtGetChild(i);
                    if (child != null) {
                        if (child.jjtGetParent() == null) {
                            String message = "Tree included child " + child + " with a null parent";
                            recordViolation(message, failHard, validation);
                        } else if (child.jjtGetParent() != node) {
                            String message = "Included a child " + child + " with conflicting parent. Expected " + node + " but was " + child.jjtGetParent();
                            recordViolation(message, failHard, validation);
                        }
                        workingStack.push(child);
                    } else {
                        String message = "Included a null child under parent " + node;
                        recordViolation(message, failHard, validation);
                    }
                }
            }
        }

        return validation;
    }

    // Record the specified violation message. Throw an exception if failHard is true.
    private static void recordViolation(String message, boolean failHard, LineageValidation validation) {
        if (failHard) {
            throw new RuntimeException("Failed to validate lineage: " + message);
        } else {
            log.error("Failed to validate lineage: " + message);
            validation.addViolation(message);
        }
    }

    public static class LineageValidation {

        private final List<String> violations = new ArrayList<>();

        /**
         * Returns whether a valid lineage was confirmed.
         *
         * @return true if no violations were found, or false otherwise
         */
        public boolean isValid() {
            return violations.isEmpty();
        }

        /**
         * Add a message describing an encountered violation.
         *
         * @param message
         *            the description message
         */
        public void addViolation(String message) {
            violations.add(message);
        }

        /**
         * Return a string containing each violation message on a new line.
         *
         * @return the formatted string, or null if there are no violations.
         */
        public String getFormattedViolations() {
            if (isValid()) {
                return null;
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append(violations.get(0));
                for (int i = 1; i < violations.size(); i++) {
                    sb.append("\n").append(violations.get(i));
                }
                return sb.toString();
            }
        }
    }

    /**
     * Checks to see if the tree contains any AND/OR nodes with less than 2 children.
     *
     * @param node
     *            the tree to validate
     * @return true if valid, or false otherwise
     */
    public static boolean validateJunctionChildren(JexlNode node) {
        return validateJunctionChildren(node, false);
    }

    /**
     * Checks to see if the tree contains any AND/OR nodes with less than 2 children.
     *
     * @param node
     *            the tree to validate
     * @param failHard
     *            if true, throw a {@link RuntimeException} if a violation was found
     * @return true if valid, or false otherwise
     */
    public static boolean validateJunctionChildren(JexlNode node, boolean failHard) {
        boolean valid = JunctionValidatingVisitor.validate(node);
        if (!valid && failHard) {
            QueryException qe = new QueryException(DatawaveErrorCode.NODE_PROCESSING_ERROR, "Instance of AND/OR node found with less than 2 children");
            throw new RuntimeException(qe);
        }
        return valid;
    }

    public static NodeTypeCount getIvarators(JexlNode node) {
        return NodeTypeCountVisitor.countNodes(node, QueryPropertyMarker.getIvaratorTypes());
    }

    public static int getIvaratorCount(NodeTypeCount nodeCount) {
        int count = 0;
        for (QueryPropertyMarker.MarkerType marker : QueryPropertyMarker.getIvaratorTypes()) {
            count += nodeCount.getTotal(marker);
        }

        return count;
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
    }

    public static class HasUnfieldedTermVisitor extends BaseVisitor {

        @Override
        public Object visit(ASTIdentifier node, Object data) {
            if (node.getName() != null && Constants.ANY_FIELD.equals(node.getName())) {
                AtomicBoolean state = (AtomicBoolean) data;
                state.set(true);
            }
            return data;
        }
    }
}
