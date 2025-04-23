package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * A {@link BaseVisitor} implementation that will search a query for any sub-phrases that represent a fielded term that is directly followed by unfielded terms
 * conjoined by the specified junction type. For example, this visitor would identify cases like {@code FOO:"abc" "def"} and {@code FOO:"abc" AND "def"} if the
 * junction type {@link JUNCTION#AND} is specified, and cases like {@code FOO:"abc" OR "def"} if the junction type {@link JUNCTION#OR} is specified.
 */
public class AmbiguousUnfieldedTermsVisitor extends BaseVisitor {

    public enum JUNCTION {
        AND(QueryNodeType.AND, AndQueryNode::new), OR(QueryNodeType.OR, OrQueryNode::new);

        private final QueryNodeType type;
        private final Function<List<QueryNode>,QueryNode> constructor;

        JUNCTION(QueryNodeType type, Function<List<QueryNode>,QueryNode> constructor) {
            this.type = type;
            this.constructor = constructor;
        }

        public QueryNodeType getType() {
            return type;
        }

        public QueryNode getNewInstance(List<QueryNode> children) {
            return constructor.apply(children);
        }
    }

    /**
     * Returns a list of copies of nodes representing fielded terms with unfielded terms directly following them that are conjoined by the specified junction.
     *
     * @param node
     *            the node
     * @param junction
     *            the junction type AND/OR
     * @return the list of ambiguous nodes
     */
    public static List<QueryNode> check(QueryNode node, JUNCTION junction) {
        AmbiguousUnfieldedTermsVisitor visitor = new AmbiguousUnfieldedTermsVisitor(junction);
        // noinspection unchecked
        return (List<QueryNode>) visitor.visit(node, new ArrayList<QueryNode>());
    }

    private final JUNCTION junction;

    private AmbiguousUnfieldedTermsVisitor(JUNCTION junction) {
        this.junction = junction;
    }

    @Override
    public Object visit(AndQueryNode node, Object data) {
        return this.junction == JUNCTION.AND ? checkJunction(node, data) : super.visit(node, data);
    }

    @Override
    public Object visit(OrQueryNode node, Object data) {
        return this.junction == JUNCTION.OR ? checkJunction(node, data) : super.visit(node, data);
    }

    @Override
    public Object visit(GroupQueryNode node, Object data) {
        // If the group node consists entirely of a single fielded term with ambiguously ORed unfielded phrases, add a copy of the group node to the data.
        if (groupConsistsOfUnfieldedTerms(node, false)) {
            // noinspection unchecked
            ((List<QueryNode>) data).add(copy(node));
            return data;
        } else {
            // Otherwise, examine the children.
            return super.visit(node, data);
        }
    }

    /**
     * Checks the given junction (AND/OR) node for any unfielded terms directly following a fielded term.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the updated data
     */
    @SuppressWarnings("unchecked")
    private Object checkJunction(QueryNode node, Object data) {
        // The list of linked ambiguous phrases.
        List<QueryNode> ambiguousPhrases = null;
        // The first fielded term that should be part of the list above.
        QueryNode fieldedTerm = null;

        // Examine each child of the OR.
        for (QueryNode child : node.getChildren()) {
            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch (type) {
                // The current child is a FIELD.
                case FIELD:
                    // The current child is an unfielded term.
                    if (((FieldQueryNode) child).getFieldAsString().isEmpty()) {
                        // If we have found a fielded term in the preceding terms, the child is part of the current set of ambiguous phrases.
                        if (fieldedTerm != null) {
                            // Ensure the list of ambiguous phrases is initialized with a copy of the fielded term as the first element.
                            if (ambiguousPhrases == null) {
                                ambiguousPhrases = new ArrayList<>();
                                ambiguousPhrases.add(copy(fieldedTerm));
                            }
                            // Add a copy of the unfielded terms.
                            ambiguousPhrases.add(copy(child));
                        }
                        // The current child is a fielded term.
                    } else {
                        // We are already tracking a fielded term.
                        if (fieldedTerm != null) {
                            // The current child is a new fielded term. If we found ambiguous phrases in the preceding terms, add a new OR node with the phrases
                            // to the data and reset the list.
                            if (ambiguousPhrases != null) {
                                ((List<QueryNode>) data).add(junction.getNewInstance(ambiguousPhrases));
                                ambiguousPhrases = null;
                            }
                        }
                        // Update the fielded term.
                        fieldedTerm = child;
                    }
                    break;
                // The current child is a GROUP.
                case GROUP:
                    // We have previously found a fielded term that may be the start of ambiguous phrases.
                    if (fieldedTerm != null) {
                        // Check if the group consists solely of unfielded OR'd phrases.
                        if (groupConsistsOfUnfieldedTerms((GroupQueryNode) child, true)) {
                            // It does. Ensure the list of ambiguous phrases is initialized with a copy of the fielded term as the first element.
                            if (ambiguousPhrases == null) {
                                ambiguousPhrases = new ArrayList<>();
                                ambiguousPhrases.add(copy(fieldedTerm));
                            }
                            // Add a copy of the group.
                            ambiguousPhrases.add(copy(child));
                        } else {
                            // The group does not consist solely of unfielded OR'd phrases. If we found ambiguous phrases in the preceding terms, add a new
                            // OR node with the phrases to the data. Reset the list and fielded term.
                            if (ambiguousPhrases != null) {
                                ((List<QueryNode>) data).add(junction.getNewInstance(ambiguousPhrases));
                                ambiguousPhrases = null;
                            }
                            fieldedTerm = null;
                            // Examine the children of the GROUP node
                            super.visit(child, data);
                        }
                    } else {
                        // Check if the group consists solely of a fielded term followed by unfielded OR'd phrases.
                        if (groupConsistsOfUnfieldedTerms((GroupQueryNode) child, false)) {
                            // If it does, add a copy of it to the data.
                            ((List<QueryNode>) data).add(copy(child));
                        } else {
                            // Otherwise, examine the children of the GROUP node.
                            super.visit(child, data);
                        }
                    }
                    break;
                default:
                    // If the child is any type other than a GROUP or FIELD, then this is the end of any previously found ambiguous phrases. Add a new OR node
                    // with the previously found phrases to the data, and then reset the list and fielded term.
                    if (ambiguousPhrases != null) {
                        ((List<QueryNode>) data).add(junction.getNewInstance(ambiguousPhrases));
                        ambiguousPhrases = null;
                    }
                    fieldedTerm = null;
                    // Examine the children of the child.
                    super.visit(child, data);
                    break;
            }
        }

        // If we have a list of ambiguous phrases after examining all the children, add a new OR node to the data.
        if (ambiguousPhrases != null) {
            ((List<QueryNode>) data).add(junction.getNewInstance(ambiguousPhrases));
        }

        return data;
    }

    /**
     * Return whether the given {@link GroupQueryNode} consists entirely of ambiguously ORed unfielded phrases.
     *
     * @param node
     *            the group node
     * @param fieldedTermFound
     *            whether a fielded term has already been found
     * @return true if the group node consists of ambiguously ORed phrases, or false otherwise
     */
    private boolean groupConsistsOfUnfieldedTerms(GroupQueryNode node, boolean fieldedTermFound) {
        // A GROUP node will have just one child.
        QueryNode child = node.getChild();
        QueryNodeType type = QueryNodeType.get(child.getClass());
        if (type == QueryNodeType.GROUP) {
            // The child is a nested group. Examine it.
            return groupConsistsOfUnfieldedTerms((GroupQueryNode) child, fieldedTermFound);
        } else if (type == junction.getType()) {
            // The child is an OR. Examine the OR's children.
            return junctionConsistsOfUnfieldedTerms(child, fieldedTermFound);
        } else if (type == QueryNodeType.FIELD) {
            // If the child is a single field term, return true if it is unfielded and we have found a fieldedTerm. Otherwise, return false.
            return fieldedTermFound && ((FieldQueryNode) child).getFieldAsString().isEmpty();
        } else {
            // The child is not one of the target types we want..
            return false;
        }
    }

    /**
     * Return whether the given {@link OrQueryNode} consists entirely of ambiguously ORed unfielded phrases.
     *
     * @param node
     *            the OR node
     * @param fieldedTermFound
     *            whether a fielded term has already been found.
     * @return true if the OR node consists of ambiguously ORed phrases, or false otherwise
     */
    private boolean junctionConsistsOfUnfieldedTerms(QueryNode node, boolean fieldedTermFound) {
        List<QueryNode> children = node.getChildren();
        boolean unfieldedTermsFound = false;
        boolean fieldTermFoundInGroupSibling = false;
        // Examine the children.
        for (QueryNode child : children) {
            QueryNodeType type = QueryNodeType.get(child.getClass());
            // If the child is a group, check if it consists of ambiguously ORed phrases.
            if (type == QueryNodeType.GROUP) {
                // If we found the field term specifically in a previous GROUP sibling, the top-level group cannot consist of ambigously ORed unfielded phrases.
                // Instead, we have something like ((FOO:abc OR def) OR (aaa OR bbb)) which cannot be flattened to FOO:(abc OR def OR aaa OR bbb).
                if (fieldTermFoundInGroupSibling) {
                    return false;
                }
                if (groupConsistsOfUnfieldedTerms((GroupQueryNode) child, fieldedTermFound)) {
                    // If it does, we know the group is something like one of the following:
                    // (FOO:abc OR def).
                    // (abc OR def OR ghi)
                    if (!fieldedTermFound) {
                        fieldedTermFound = true;
                        fieldTermFoundInGroupSibling = true;
                    }
                    unfieldedTermsFound = true;
                } else {
                    // If it does not, the top-level group does not consist solely of ambiguous phrases.
                    return false;
                }
            } else if (type == QueryNodeType.FIELD) {
                // If the child is a field term, check if it is fielded or unfielded.
                if (!((FieldQueryNode) child).getFieldAsString().isEmpty()) {
                    // If the field name is not empty, and we have not found a fielded term yet, mark that we've found one.
                    if (!fieldedTermFound) {
                        fieldedTermFound = true;
                    } else {
                        // If a fielded term was found previously, then we have may something like (FOO:abc OR BAR:abc).
                        return false;
                    }
                } else {
                    // The current child is an unfielded term. If no fielded term has been found yet, then we may have something like (abc OR FOO:abc).
                    if (!fieldedTermFound) {
                        return false;
                    } else {
                        // Otherwise, mark that we've found an unfielded term.
                        unfieldedTermsFound = true;
                    }
                }
            } else {
                return false;
            }
        }
        // Return whether we found at least one unfielded term following the fielded term like (FOO:abc OR def).
        return unfieldedTermsFound;
    }

}
