package datawave.query.jexl.visitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.SimpleNode;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.data.type.Type;
import datawave.edge.model.EdgeModelFields;
import datawave.edge.util.EdgeKeyUtil;
import datawave.query.tables.edge.contexts.EdgeContext;
import datawave.query.tables.edge.contexts.IdentityContext;
import datawave.query.tables.edge.contexts.QueryContext;
import datawave.query.tables.edge.contexts.VisitationContext;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Once an edge query has been parsed into a jexl tree this class is run to traverse the nodes of the tree gathering up necessary information to use to build
 * the accumulo ranges and the normalized query that will be sent to the EdgeFilterIterator for further evaluation <br>
 * <br>
 * <p>
 * The high level design of how the parsing works is as follows: There are two data structures that are built during the traversal to keep track of information.
 * Once the traversal is complete and the data structures are built they are sent to the VisitationContext which then builds the ranges and normalized query and
 * gets returned.<br>
 * </p>
 * <br>
 * <p>
 * All data is expected to be passed up since this is a depth first search nothing will be passed down<br>
 * <br>
 * <p>
 * The two data structures used:<br>
 * 1) IdentityContext<br>
 * This class stores 3 things:<br>
 * Identity: eg SOURCE, SINK, TYPE ect<br>
 * Operation: eg equals, equals regex, not equals, and not regex<br>
 * Literal: eg 'search term'<br>
 * <br>
 * <p>
 * During the Traversal lists of IdentityContexts are built. A list of IdentityContexts must all have the same Identity. For example you can only have a list of
 * IdentityContexts with all SOURCE identities or all TYPE identities. Once a list of IdentityContexts is finished being built it is stored in a QueryContext
 * The only exception is for exclusion expressions (!= !~) and functions.<br>
 * <br>
 * <p>
 * 2) QueryContext<br>
 * This class stores a list of IdentityContexts for each supported search term. (Eg 1 list of IdentityContexts for source or edge type ect...) Once a list has
 * been set it cannot be changed/modified/added to<br>
 * <br>
 * <p>
 * During the Traversal lists of QueryContexts are built. Once traversal is over we must have 1 or more QueryContexts which are then used to build the
 * ranges/normalized query.<br>
 * <br>
 * <p>
 * The 3 basic rules that are enforced are:<br>
 * for equivalence expressions (== =~)<br>
 * 1) cant and like identifiers<br>
 * 2) can't or unlike identifiers<br>
 * for exclude expressions (!= !~)<br>
 * 3) only use and<br>
 * <br>
 * <p>
 * There are two basic types of queries that are allowed/expected to be run:<br>
 * <br>
 * <p>
 * {@code (SOURCE == 's1' || SOURCE == 's2'|| SOURCE == ...) && (SINK == 't1' || SINK == 't2' || ...)}<br>
 * <br>
 * <p>
 * or<br>
 * <br>
 * <p>
 * {@code (SOURCE == 's1' && SINK == 's2') || (SOURCE == 's2 && SINK == 's2) || ...}<br>
 * <br>
 */
public class EdgeTableRangeBuildingVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(EdgeTableRangeBuildingVisitor.class);
    protected boolean includeStats;
    protected List<? extends Type<?>> regexDataTypes;
    protected List<? extends Type<?>> dataTypes;

    private static final char BACKSLASH = '\\';
    private static final char STRING_QUOTE = '\'';
    private Set<String> allowedFunctions = Sets.newHashSet("has_all");
    private int termCount = 0;
    private long maxTerms = 10000;
    private boolean sawEquivalenceRegexSource = false;
    private boolean sawEquivalenceRegexSink = false;
    private final EdgeModelFields fields;

    public EdgeTableRangeBuildingVisitor(boolean stats, List<? extends Type<?>> types, long maxTerms, List<? extends Type<?>> rTypes, EdgeModelFields fields) {
        this.includeStats = stats;
        this.dataTypes = types;
        this.maxTerms = maxTerms;
        regexDataTypes = rTypes;
        this.fields = fields;
    }

    /*
     * This is treated as the root node of the tree it can only have one child
     *
     * The job of this node is to take the results of its child and create the visitation context to be returned
     */
    public Object visit(ASTJexlScript node, Object data) {

        int numChildren = node.jjtGetNumChildren();

        if (numChildren != 1) {
            log.error("JexlScript node had an unexpected number of children: " + numChildren);
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.NODE_PROCESSING_ERROR);
            throw new RuntimeException(qe);
        }

        @SuppressWarnings("unchecked")
        List<? extends EdgeContext> context = (List<? extends EdgeContext>) node.jjtGetChild(0).jjtAccept(this, null);

        if (context.get(0) instanceof IdentityContext) {
            // this can only happen if there is no AND node in the query
            // Build singleton list of QueryContexts then create VisitationContext
            QueryContext qContext = new QueryContext(fields);
            qContext.packageIdentities((List<IdentityContext>) context);

            return computeVisitaionContext(Collections.singletonList(qContext));

        } else if (context.get(0) instanceof QueryContext) {
            return computeVisitaionContext((List<QueryContext>) context);
            // return context;
        } else {
            log.error("JexlScript node recieved unexpected return type: " + context);
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.NODE_PROCESSING_ERROR);
            throw new RuntimeException(qe);
        }

    }

    // Overridden to return the results of children
    @Override
    public Object visit(ASTReference node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    // Overridden to return the results of children
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    /*
     * And node should have exactly two children This node's only function is to pack lists of IdentityContexts into a QueryContext
     *
     * Returns a QueryContext
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // run the visitor against all of the children
        List<List<? extends EdgeContext>> childContexts = new ArrayList<>(node.jjtGetNumChildren());
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            childContexts.add((List<? extends EdgeContext>) node.jjtGetChild(i).jjtAccept(this, null));
        }

        if (childContexts.isEmpty()) {
            log.error("Unable to get edge context from AND node");
            throw new IllegalArgumentException("Unable to get edge context from AND node");
        }

        List<? extends EdgeContext> mergedContext = childContexts.remove(childContexts.size() - 1);

        // now merge the child contexts
        while (!childContexts.isEmpty()) {
            List<? extends EdgeContext> childContext = childContexts.remove(childContexts.size() - 1);

            if ((childContext.get(0) instanceof IdentityContext) && (mergedContext.get(0) instanceof IdentityContext)) {
                QueryContext qContext = new QueryContext(fields);

                qContext.packageIdentities((List<IdentityContext>) childContext);
                qContext.packageIdentities((List<IdentityContext>) mergedContext);

                ArrayList<QueryContext> aList = new ArrayList<>();
                aList.add(qContext);
                mergedContext = aList;
            } else if ((childContext.get(0) instanceof IdentityContext) && (mergedContext.get(0) instanceof QueryContext)) {

                for (QueryContext qContext : (List<QueryContext>) mergedContext) {
                    qContext.packageIdentities((List<IdentityContext>) childContext);
                }

            } else if ((childContext.get(0) instanceof QueryContext) && (mergedContext.get(0) instanceof IdentityContext)) {

                for (QueryContext qContext : (List<QueryContext>) childContext) {
                    qContext.packageIdentities((List<IdentityContext>) mergedContext);
                }

                mergedContext = childContext;
                /*
                 * On rare occasion a group of Query contexts without a source can get grouped together, this happens with queries like: SOURCE == 's1' &&
                 * ((TYPE == 't1' && RELATIONSHIP == 'r1') || (TYPE == 't2' && RELATIONSHIP == 'r2'))
                 *
                 * This probably was not supposed to be allowed, you should only be ANDing groups of sources with groups of other identifiers rather here it is
                 * ANDing source(s) with groups of expressions. Honestly it would be safer to split that type of query up into two separate ones but limited
                 * support has been included. However it can be dangerous if the user tries to use SINK in one of the grouped expressions so that is not
                 * allowed.
                 */
            } else if ((childContext.get(0) instanceof QueryContext) && (mergedContext.get(0) instanceof QueryContext)) {
                // Assumes that if the first query context does not have a row context then they all don't
                if (((List<QueryContext>) childContext).get(0).getRowContext() != null) {
                    // The size of the list for contexts1 is usually going to be 1
                    for (QueryContext qContext : ((List<QueryContext>) childContext)) {
                        // Combine the query contexts if anything fails blame the user
                        if (!(qContext.combineQueryContexts(((List<QueryContext>) mergedContext), false))) {
                            log.error("And node had unexpected return type");
                            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_SYNTAX_PARSE_ERROR,
                                            "Error: problem with query syntax");
                            throw new IllegalArgumentException(qe);
                        }
                    }
                    mergedContext = childContext;
                } else if (((List<QueryContext>) mergedContext).get(0).getRowContext() != null) {
                    for (QueryContext qContext : ((List<QueryContext>) mergedContext)) {
                        if (!(qContext.combineQueryContexts(((List<QueryContext>) childContext), false))) {
                            log.error("And node had unexpected return type");
                            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_SYNTAX_PARSE_ERROR,
                                            "Error: problem with query syntax");
                            throw new IllegalArgumentException(qe);
                        }
                    }
                } else {
                    log.error("Problem parsing query");
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_SYNTAX_PARSE_ERROR,
                                    "Error: problem with query syntax");
                    throw new IllegalArgumentException(qe);
                }
            } else {

                log.error("And node had unexpected return type");
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_SYNTAX_PARSE_ERROR, "Error: problem with query syntax");
                throw new IllegalArgumentException(qe);
            }
        }

        return mergedContext;
    }

    /**
     * Or node should have exactly two children This or node's only function is to combine the lists returned by its children.
     * <p>
     * The Children can return lists of type IdentityContext or QueryContext both children must return the same type of lists else its an improper query ex:
     * Both children return lists of IdentityContexts where the identity is source Both children return lists of QueryContexts
     * <p>
     * There is one exception where a QueryContext list and a IdentityContext list could be returned by the children in which case the returned IdentityContext
     * list is immediately packaged into a new QueryContext and is then combined with the returned other QueryContext list Happens with a query like this:
     * {@code (SOURCE == 'source1' && SINK == 'sink') || (SOURCE == 'source2')}
     * <p>
     * Or node will return either a list of IdentityContexts or QueryContexts
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return list of IdentityContexts or QueryContexts
     */
    @Override
    public Object visit(ASTOrNode node, Object data) {
        // run the visitor against all of the children
        List<List<? extends EdgeContext>> childContexts = new ArrayList<>(node.jjtGetNumChildren());
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            childContexts.add((List<? extends EdgeContext>) node.jjtGetChild(i).jjtAccept(this, null));
        }

        if (childContexts.isEmpty()) {
            log.error("Unable to get edge context from OR node");
            throw new IllegalArgumentException("Unable to get edge context from OR node");
        }

        List<? extends EdgeContext> mergedContext = childContexts.remove(childContexts.size() - 1);

        // now merge the child contexts
        while (!childContexts.isEmpty()) {
            List<? extends EdgeContext> childContext = childContexts.remove(childContexts.size() - 1);

            if ((childContext.get(0) instanceof IdentityContext) && (mergedContext.get(0) instanceof IdentityContext)) {
                // Combine two lists of Identity contexts
                IdentityContext iContext1 = (IdentityContext) childContext.get(0);
                IdentityContext iContext2 = (IdentityContext) mergedContext.get(0);

                checkNotExclusion(iContext1, "Can't OR exclusion expressions");
                checkNotExclusion(iContext2, "Can't OR exclusion expressions");
                if (iContext1.getIdentity().equals(iContext2.getIdentity())) {
                    ((List<IdentityContext>) childContext).addAll((List<IdentityContext>) mergedContext);
                    mergedContext = childContext;
                } else {
                    log.error("Query attempted to or like terms: " + iContext1.getIdentity() + " and " + iContext1.getIdentity());
                    throw new IllegalArgumentException("Can't OR unlike terms: " + iContext1.getIdentity() + " and " + iContext2.getIdentity());
                }

            } else if ((childContext.get(0) instanceof QueryContext) && (mergedContext.get(0) instanceof QueryContext)) {
                List<QueryContext> context1 = (List<QueryContext>) childContext;
                List<QueryContext> context2 = (List<QueryContext>) mergedContext;

                if (context1.size() == 1 && context1.get(0).hasSourceList() == false) {
                    runCombine(context2, context1);

                    mergedContext = context2;
                } else if (context2.size() == 1 && context2.get(0).hasSourceList() == false) {
                    runCombine(context1, context2);

                    mergedContext = context1;
                } else {
                    context1.addAll(context2);
                    mergedContext = context1;
                }
            }

            /*
             * The next two else if statements are used when we have a query that technically follows the rules but it is not clear how it should be evaluated.
             * Basically we make an effort to keep going. No guarantees as to what happens Ex) SOURCE == 's1' || SOURCE == 's2 && SINK == 't1' || SINK == 't2'
             * Instead of: (SOURCE == 's1' || SOURCE == 's2) && (SINK == 't1' || SINK == 't2') Or: SOURCE == 's1' || (SOURCE == 's2 && (SINK == 't1' || SINK ==
             * 't2'))
             */

            else if ((childContext.get(0) instanceof IdentityContext) && (mergedContext.get(0) instanceof QueryContext)) {
                checkNotExclusion((IdentityContext) childContext.get(0), "Can't OR exclusion expressions");

                QueryContext queryContext = new QueryContext(fields);
                queryContext.packageIdentities((List<IdentityContext>) childContext, false);

                if (isSourceList((List<IdentityContext>) childContext)) {
                    ((List<QueryContext>) mergedContext).add(queryContext);
                } else {
                    List<QueryContext> otherContexts = new ArrayList<>();
                    otherContexts.add(queryContext);
                    runCombine((List<QueryContext>) mergedContext, otherContexts);
                }
            } else if ((childContext.get(0) instanceof QueryContext) && (mergedContext.get(0) instanceof IdentityContext)) {
                checkNotExclusion((IdentityContext) mergedContext.get(0), "Can't OR exclusion expressions");

                QueryContext queryContext = new QueryContext(fields);
                queryContext.packageIdentities((List<IdentityContext>) mergedContext, false);

                if (isSourceList((List<IdentityContext>) mergedContext)) {
                    ((List<QueryContext>) childContext).add(queryContext);
                } else {
                    List<QueryContext> otherContexts = new ArrayList<>();
                    otherContexts.add(queryContext);
                    runCombine((List<QueryContext>) childContext, otherContexts);
                }

                mergedContext = childContext;
            } else {
                log.error("OR node had unexpected return type");
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_SYNTAX_PARSE_ERROR, "Error: problem with query syntax");
                throw new IllegalArgumentException(qe);
            }
        }

        return mergedContext;
    }

    private void runCombine(List<QueryContext> q1, List<QueryContext> q2) {
        for (QueryContext qContext : (List<QueryContext>) q1) {
            if (!qContext.combineQueryContexts(q2, true)) {
                log.error("Unable to combine query contexts");
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_SYNTAX_PARSE_ERROR, "Error: problem with query syntax");
                throw new IllegalArgumentException(qe);
            }
        }
    }

    private boolean isSourceList(List<IdentityContext> context) {
        if (context.get(0).getIdentity().equals(EdgeModelFields.FieldKey.EDGE_SOURCE)) {
            return true;
        } else {
            return false;
        }
    }

    private void checkNotExclusion(IdentityContext context, String msg) {
        if (!context.isEquivalence()) {
            throw new RuntimeException(msg);
        }
    }

    /**
     * Equals node (==) should have exactly two children (reference nodes) one child will have the identifier eg: SOURCE ... the other child will have the
     * string literal eg: 'searchTerm'
     * <p>
     * Returns a list of 1 or more IdentityContexts
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return list of IdentityContexts or QueryContexts
     */
    @Override
    public Object visit(ASTEQNode node, Object data) {
        incrementTermCountAndCheck();
        return visitExpresionNode(node, EdgeModelFields.EQUALS);
    }

    private void incrementTermCountAndCheck() {
        if (++termCount > maxTerms) {
            String message = "Exceeded max term limit of " + maxTerms;
            log.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Equals node (=~) should have exactly two children (reference nodes) one child will have the identifier eg: SOURCE ... the other child will have the
     * string literal eg: 'searchTerm'
     * <p>
     * Returns a list of 1 or more IdentityContexts
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return list of IdentityContexts
     */
    @Override
    public Object visit(ASTERNode node, Object data) {
        incrementTermCountAndCheck();
        List<IdentityContext> contexts = (List<IdentityContext>) visitExpresionNode(node, EdgeModelFields.EQUALS_REGEX);
        if (contexts.get(0).getIdentity().equals(EdgeModelFields.FieldKey.EDGE_SOURCE)) {
            sawEquivalenceRegexSource = true;
        } else if (contexts.get(0).getIdentity().equals(EdgeModelFields.FieldKey.EDGE_SINK)) {
            sawEquivalenceRegexSink = true;
        }
        return contexts;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        incrementTermCountAndCheck();
        return visitExpresionNode(node, EdgeModelFields.NOT_EQUALS_REGEX);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        incrementTermCountAndCheck();
        return visitExpresionNode(node, EdgeModelFields.NOT_EQUALS);
    }

    private Object visitExpresionNode(SimpleNode node, String operator) {
        int numChildren = node.jjtGetNumChildren();

        if (numChildren != 2) {
            log.error("Equals node had unexpected number of children: " + numChildren);
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY, "problem parsing query");
            throw new IllegalArgumentException(qe);
        }

        String identifier = JexlNodes.getIdentifierOrLiteralAsString(node.jjtGetChild(0)).toUpperCase();
        String literal = JexlNodes.getIdentifierOrLiteralAsString(node.jjtGetChild(1));
        List<IdentityContext> contexts = new ArrayList<>();

        if (identifier.equals(fields.getSourceFieldName()) || identifier.equals(fields.getSinkFieldName()) || identifier.equals(fields.getAttribute3FieldName())
                        || identifier.equals(fields.getAttribute2FieldName())) {

            if (operator.equals(EdgeModelFields.EQUALS_REGEX) || operator.equals(EdgeModelFields.NOT_EQUALS_REGEX)) {
                for (String normalizedLiteral : EdgeKeyUtil.normalizeRegexSource(literal, regexDataTypes, true)) {
                    try { // verify that the normalized regex is valid here instead of letting it fail on tserver
                          // TODO: right now the edge filter iterator calls toLowerCase on the query string by default
                          // some valid regex characters need to be uppercase and will fail to be valid regex after being lowercased
                          // so for right now throw out any regex's that would cause the edge filter iterator to fail but this should probably change in the
                          // future
                        Pattern.compile(normalizedLiteral.toLowerCase());
                        IdentityContext iContext = new IdentityContext(identifier, normalizedLiteral, operator, fields);
                        contexts.add(iContext);
                    } catch (PatternSyntaxException e) {
                        continue;
                    }
                }
                if (contexts.isEmpty()) {
                    log.error("Couldn't normalize users regex: " + literal);
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_REGEX, "Can't build query invalid regex: " + literal);
                    throw new RuntimeException(qe);
                }
            } else {
                for (String normalizedLiteral : EdgeKeyUtil.normalizeSource(literal, dataTypes, true)) {
                    IdentityContext iContext = new IdentityContext(identifier, normalizedLiteral, operator, fields);
                    contexts.add(iContext);
                }
            }

        } else {
            IdentityContext iContext = new IdentityContext(identifier, literal, operator, fields);
            contexts.add(iContext);
        }

        return contexts;
    }

    /**
     * Used only for the Edge'Source'QueryLogic Basically function support was an afterthought only to support old code in the existing Edge'Source'QueryLogic A
     * list with a single Identity context is returned in the following format: Identity = FUNCTION Opperator = FUNCTION LITER = "{function string}" //eg
     * source.has_all(SINK, "t1", "t2", ...)
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return list of IdentityContexts
     */
    public Object visit(ASTFunctionNode node, Object data) {
        if (node.jjtGetParent() instanceof ASTJexlScript) {
            // then we're the only node in here, we need to come up with ranges from the function itself, or fail the query
            throw new IllegalArgumentException("Cannot perform has_all without specifying a pattern for the edge 'SOURCE'.");
        }

        StringBuilder sb = new StringBuilder();

        ASTNamespaceIdentifier namespaceNode = (ASTNamespaceIdentifier) node.jjtGetChild(0);
        if (!allowedFunctions.contains(namespaceNode.getName().toLowerCase())) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FUNCTION_NOT_FOUND,
                            namespaceNode.getName() + " not supported function for EdgeQuery");
            throw new UnsupportedOperationException(qe);
        }

        sb.append(namespaceNode.getNamespace());
        sb.append(":");
        sb.append(namespaceNode.getName());

        ASTArguments argsNode = (ASTArguments) node.jjtGetChild(1);
        for (int i = 0; i < argsNode.jjtGetNumChildren(); i++) {
            if (i == 0) {
                sb.append("(");
                sb.append(JexlNodes.getIdentifierOrLiteral(argsNode.jjtGetChild(i)));
            } else {
                sb.append(", ");
                sb.append("'").append(JexlNodes.getIdentifierOrLiteralAsString(argsNode.jjtGetChild(i)).toLowerCase()).append("'");
            }
        }

        sb.append(")");

        List<IdentityContext> contexts = new ArrayList<>();
        IdentityContext iContext = new IdentityContext(EdgeModelFields.FieldKey.FUNCTION.name(), sb.toString(), EdgeModelFields.FieldKey.FUNCTION.name(),
                        fields);

        contexts.add(iContext);
        return contexts;
    }

    /**
     * This method creates a new VisitationContext object to be returned and loads the final list of queryContexts which are then used to build the ranges and
     * normalized query One of the main purposes of this method is to create the normalized query that is used to filter column families from ranges. This is a
     * problem when there are multiple query contexts because the allowlist will exclude certain column family values, which will affect what gets returned by
     * the query. This is addressed by the columnFamilyAreDifferent boolean which is passed down to populateQuery()
     *
     * @param queryContexts
     *            query contexts
     * @return a visitation context
     */
    private VisitationContext computeVisitaionContext(List<QueryContext> queryContexts) {
        // if both edge types and edge relationships are complete (not regex) for each query context then we can use the
        // batch scanners built in fetch column method to do the filtering for us so we can drop the edge types, and relations
        // from the normalized query that goes to the edge filter iterator.
        boolean includColumnFamilyTerms = false;
        boolean columnFamilyAreDifferent = false;

        // If the sink field appears in a query contexts, 'otherContext' list then it will have to be included in the normalized
        // query string sent to the edge filter iterator
        boolean includeSink = false;
        boolean includeSource = false;
        for (QueryContext queryContext : queryContexts) {
            if (queryContext.hasCompleteColumnFamily() == false) {
                includColumnFamilyTerms = true;
                break;
            }
        }

        // If there are multiple query contexts, and their column families are not the same, we will pass down a boolean
        // so that the allowlist will not get updated to improve column family filtering against ranges

        if (queryContexts.size() > 1) {
            int i;
            QueryContext.ColumnContext firstColumn = (queryContexts.get(0).getColumnContext());
            for (i = 1; i < queryContexts.size(); i++) {
                QueryContext.ColumnContext currentContext = queryContexts.get(i).getColumnContext();
                if ((firstColumn != null && currentContext == null) || (firstColumn == null && currentContext != null)) {
                    columnFamilyAreDifferent = true;
                    break;
                }
                if (firstColumn != null) {
                    if (!(firstColumn.equals(queryContexts.get(i).getColumnContext()))) {
                        columnFamilyAreDifferent = true;
                        break;
                    }
                }
            }
        }
        VisitationContext vContext = new VisitationContext(fields, includeStats);
        vContext.setHasAllCompleteColumnFamilies(!includColumnFamilyTerms);

        for (QueryContext qContext : queryContexts) {
            includeSink = vContext.updateQueryRanges(qContext);
            includeSink = includeSink || sawEquivalenceRegexSink || columnFamilyAreDifferent;
            // If there is only one query context you don't need to include the source, you do if there are multiple QCs
            if (queryContexts.size() > 1) {
                // If there are multiple query contexts, that means that there are multiple sources. If there are multiple
                // sources, then they need to be included with sinks.
                includeSource = includeSink || sawEquivalenceRegexSource || columnFamilyAreDifferent;
            } else if (queryContexts.size() == 1) {
                // If there is only one source, you don't need to include it with the sink. It is implied.
                includeSource = sawEquivalenceRegexSource || columnFamilyAreDifferent;
            }
            // boolean for source and sink inclusion for normalized query
            // boolean to include column family terms to the normalized query
            // boolean to create allowlist for column family terms
            vContext.updateQueryStrings(qContext, includeSource, includeSink, includColumnFamilyTerms, !columnFamilyAreDifferent);
            if (!includColumnFamilyTerms) {
                vContext.buildColumnFamilyList(qContext, includeStats);
            }
        }
        vContext.setTermCount(termCount);
        return vContext;
    }
}
