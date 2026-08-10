package datawave.query.jexl.functions;

import static datawave.query.jexl.functions.ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_SCORED_PHRASE_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.JexlNodeFactory.ContainerType;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;

public class ContentFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {

    /**
     * This is the JexlNode descriptor which can be used to normalize and optimize function node queries
     */
    public static class ContentJexlArgumentDescriptor implements JexlArgumentDescriptor {

        private static final Logger log = Logger.getLogger(ContentJexlArgumentDescriptor.class);

        private final ASTFunctionNode node;
        private final String namespace, name;
        private final List<JexlNode> args;

        public ContentJexlArgumentDescriptor(ASTFunctionNode node, String namespace, String name, List<JexlNode> args) {
            this.node = node;
            this.namespace = namespace;
            this.name = name;
            this.args = args;
        }

        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            try {
                Set<String> tfFields = new HashSet<>(helper.getTermFrequencyFields(datatypeFilter));
                Set<String> indexedFields = new HashSet<>(helper.getIndexedFields(datatypeFilter));
                Set<String> contentFields = new HashSet<>(helper.getContentFields(datatypeFilter));

                if (config != null && !config.getNoExpansionFields().isEmpty()) {
                    // exclude fields from expansion
                    Set<String> noExpansionFields = config.getNoExpansionFields();
                    tfFields.removeAll(noExpansionFields);
                    indexedFields.removeAll(noExpansionFields);
                    contentFields.removeAll(noExpansionFields);
                }

                return getIndexQuery(tfFields, indexedFields, contentFields);
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }
        }

        public JexlNode getIndexQuery(Set<String> termFrequencyFields, Set<String> indexedFields, Set<String> contentFields) {

            LinkedList<JexlNode> nodes = Lists.newLinkedList();

            // get the cartesian product of all the fields and terms
            MutableBoolean oredFields = new MutableBoolean();
            FieldTerms fieldsAndTerms = fieldsAndTerms(termFrequencyFields, indexedFields, contentFields, oredFields, true);
            Set<String> fields = fieldsAndTerms.getFields();
            if (!fields.isEmpty()) {
                final JexlNode eq = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
                Set<String> terms = fieldsAndTerms.getTerms();
                fields.forEach(field -> nodes.add(JexlNodeFactory.createNodeTreeFromFieldValues(ContainerType.AND_NODE, eq, null, field, terms)));
            }

            if (fields.isEmpty()) {
                log.warn("No fields found for content function, will not expand index query");
                return new ASTTrueNode(ParserTreeConstants.JJTTRUENODE);
            } else if (fields.size() == 1) {
                // A single field needs no wrapper node.
                return nodes.iterator().next();
            } else if (oredFields.booleanValue()) {
                return JexlNodeFactory.createOrNode(nodes);
            } else {
                return JexlNodeFactory.createAndNode(nodes);
            }
        }

        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }

        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // normalize everything past the termOffsetMap

            Set<String> fields = Collections.emptySet();
            int firstTermIndex = 1;

            final String funcName = name;

            PeekingIterator<JexlNode> args = Iterators.peekingIterator(this.args.iterator());

            if (args.hasNext()) {
                if (CONTENT_ADJACENT_FUNCTION_NAME.equals(funcName) || CONTENT_PHRASE_FUNCTION_NAME.equals(funcName)) {
                    JexlNode firstArg = args.next();

                    // we override the zones if the first argument is a string
                    if (firstArg instanceof ASTStringLiteral) {
                        firstTermIndex = 2;
                    } else {
                        JexlNode nextArg = args.peek();

                        // The zones may (more likely) be specified as an identifier
                        if (!JexlASTHelper.getIdentifiers(firstArg).isEmpty() && !JexlASTHelper.getIdentifiers(nextArg).isEmpty()) {
                            firstTermIndex = 2;
                        }
                    }
                } else if (CONTENT_WITHIN_FUNCTION_NAME.equals(funcName) || CONTENT_SCORED_PHRASE_FUNCTION_NAME.equals(funcName)) {
                    firstTermIndex = 2;
                    JexlNode nextArg = args.next();

                    // we override the zones if the first argument is a string or identifier
                    if (nextArg instanceof ASTStringLiteral || !JexlASTHelper.getIdentifiers(nextArg).isEmpty()) {
                        firstTermIndex = 3;
                        nextArg = args.next();
                    }

                    // we can trash the distance
                    if (!(nextArg instanceof ASTNumberLiteral || nextArg instanceof ASTUnaryMinusNode)) {
                        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.NUMERIC_DISTANCE_ARGUMENT_MISSING);
                        throw new IllegalArgumentException(qe);
                    }
                } else {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FUNCTION_ARGUMENTS_MISSING);
                    throw new IllegalArgumentException(qe);
                }

            } else {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.JEXL_NODES_MISSING,
                                MessageFormat.format("Class: {0}, Namespace: {1}, Function: {2}", this.getClass().getSimpleName(), namespace, funcName));
                throw new IllegalArgumentException(qe);
            }

            if (arg >= firstTermIndex) {
                fields = fields(helper, datatypeFilter);
            }

            return fields;
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            try {
                return fieldsAndTerms(helper.getTermFrequencyFields(datatypeFilter), helper.getIndexedFields(datatypeFilter),
                                helper.getContentFields(datatypeFilter), null).getFields();
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }

        }

        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            try {
                MutableBoolean oredFields = new MutableBoolean();
                FieldTerms fieldsAndTerms = fieldsAndTerms(helper.getTermFrequencyFields(datatypeFilter), helper.getIndexedFields(datatypeFilter),
                                helper.getContentFields(datatypeFilter), oredFields);
                Set<Set<String>> fieldSets = new HashSet<>();
                if (oredFields.booleanValue()) {
                    for (String field : fieldsAndTerms.getFields()) {
                        fieldSets.add(Collections.singleton(field));
                    }
                } else {
                    fieldSets.add(fieldsAndTerms.getFields());
                }
                return fieldSets;
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }

        }

        public FieldTerms fieldsAndTerms(Set<String> termFrequencyFields, Set<String> indexedFields, Set<String> contentFields, MutableBoolean oredFields) {
            return fieldsAndTerms(termFrequencyFields, indexedFields, contentFields, oredFields, false);
        }

        @SuppressWarnings("unchecked")
        public FieldTerms fieldsAndTerms(Set<String> termFrequencyFields, Set<String> indexedFields, Set<String> contentFields, MutableBoolean oredFields,
                        boolean validateFields) {
            if (this.args.isEmpty()) {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.JEXL_NODES_MISSING,
                                MessageFormat.format("Class: {0}, Namespace: {1}, Function: {2}", this.getClass().getSimpleName(), this.namespace, this.name));
                throw new IllegalArgumentException(qe);
            }

            // Can any one of the fields satisfy the query? Always true unless the zone is specified in an AND clause.
            if (oredFields != null) {
                oredFields.setValue(true);
            }

            PeekingIterator<JexlNode> argsIterator = Iterators.peekingIterator(this.args.iterator());
            FieldTerms fieldTerms = new FieldTerms();
            JexlNode termOffsetMap;

            switch (this.name) {
                case CONTENT_ADJACENT_FUNCTION_NAME:
                    termOffsetMap = examineContentAdjacentFunction(argsIterator, fieldTerms, oredFields);
                    break;
                case CONTENT_PHRASE_FUNCTION_NAME:
                    termOffsetMap = examineContentPhraseFunction(argsIterator, fieldTerms, oredFields);
                    break;
                case CONTENT_SCORED_PHRASE_FUNCTION_NAME:
                    termOffsetMap = examineContentScoredPhraseFunction(argsIterator, fieldTerms, oredFields);
                    break;
                case CONTENT_WITHIN_FUNCTION_NAME:
                    termOffsetMap = examineContentWithinFunction(argsIterator, fieldTerms, oredFields);
                    break;
                default:
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FUNCTION_ARGUMENTS_MISSING);
                    throw new IllegalArgumentException(qe);
            }

            // Verify that a term offset map with terms were specified.
            validateTermsOffsetMapAndTermsPresent(termOffsetMap, argsIterator);

            // If the fields were not established above, ensure that the fields at least contain any term frequency fields that are indexed and, if any content
            // fields were specified, present within there as well.
            if (fieldTerms.fields == null) {
                Set<String> fields = termFrequencyFields.stream()
                                .filter(f -> indexedFields.contains(f) && (contentFields.isEmpty() || contentFields.contains(f))).collect(Collectors.toSet());
                fieldTerms.fields = fields;
            }

            // Moving this validation later in the call stack, since it requires other processing (i.e. apply query model)
            if (validateFields) {
                for (String field : fieldTerms.fields) {
                    // Deconstruct & upcase the fieldname for testing in case we have not normalized the field names yet. Return the unnormalized fieldname.
                    if (!termFrequencyFields.contains(JexlASTHelper.deconstructIdentifier(field.toUpperCase()))) {
                        PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.FIELD_PHRASE_QUERY_NOT_INDEXED,
                                        MessageFormat.format("Field: {0}", field));
                        throw new IllegalArgumentException(qe);
                    }
                }
            }

            // Now take the remaining string literals in the arguments as terms.
            Set<String> terms = Sets.newHashSetWithExpectedSize(this.args.size() - 1);
            // @formatter:off
            Streams.stream(argsIterator)
                            .filter(ASTStringLiteral.class::isInstance)
                            .map(JexlNodes::getIdentifierOrLiteralAsString)
                            .forEach(terms::add);
            // @formatter:on
            fieldTerms.terms = terms;

            return fieldTerms;
        }

        // Finds and sets the fields for a content:adjacent functions, and returns the anticpatated terms offset map node.
        private JexlNode examineContentAdjacentFunction(PeekingIterator<JexlNode> argsIterator, FieldTerms fieldTerms, MutableBoolean oredFields) {
            JexlNode firstArg = argsIterator.next();
            if (firstArg instanceof ASTStringLiteral) {
                fieldTerms.fields = Collections.singleton(JexlNodes.getIdentifierOrLiteralAsString(firstArg));
                return argsIterator.next();
            } else {
                JexlNode nextArg = argsIterator.peek();
                // The zones may (more likely) be specified as an idenfifier
                if (!JexlASTHelper.getIdentifiers(firstArg).isEmpty() && !JexlASTHelper.getIdentifiers(nextArg).isEmpty()) {
                    if (oredFields != null && firstArg instanceof ASTAndNode) {
                        oredFields.setValue(false);
                    }
                    fieldTerms.fields = JexlASTHelper.getIdentifierNames(firstArg);
                    return argsIterator.next();
                } else {
                    return firstArg;
                }
            }
        }

        // Finds and sets the fields for a content:phrase functions, and returns the anticpatated terms offset map node.
        private JexlNode examineContentPhraseFunction(PeekingIterator<JexlNode> argsIterator, FieldTerms fieldTerms, MutableBoolean oredFields) {
            JexlNode firstArg = argsIterator.next();
            // we override the zones if the first argument is a string
            if (firstArg instanceof ASTStringLiteral) {
                fieldTerms.fields = Collections.singleton(((ASTStringLiteral) firstArg).getLiteral());
                return argsIterator.next();
            } else {
                JexlNode nextArg = argsIterator.peek();
                // The zones may (more likely) be specified as an identifier
                if (!JexlASTHelper.getIdentifiers(firstArg).isEmpty() && !JexlASTHelper.getIdentifiers(nextArg).isEmpty()) {
                    if (oredFields != null && firstArg instanceof ASTAndNode) {
                        oredFields.setValue(false);
                    }
                    fieldTerms.fields = JexlASTHelper.getIdentifierNames(firstArg);
                    return argsIterator.next();
                } else {
                    return firstArg;
                }
            }
        }

        // Finds and sets the fields for a content:scoredPhrase functions, and returns the anticpatated terms offset map node.
        private JexlNode examineContentScoredPhraseFunction(PeekingIterator<JexlNode> argsIterator, FieldTerms fieldTerms, MutableBoolean oredFields) {
            JexlNode firstArg = argsIterator.next();
            if (firstArg instanceof ASTNumberLiteral || firstArg instanceof ASTUnaryMinusNode) {
                // if the first argument is a number, then no field exists
                // for example, content:scoredPhrase(-1.5, termOffsetMap, 'value')
                return argsIterator.next();
            } else {
                if (firstArg instanceof ASTIdentifier) {
                    // single field case
                    // for example, content:scoredPhrase(FIELD, -1.5, termOffsetMap, 'value')
                    fieldTerms.fields = Collections.singleton(String.valueOf(JexlASTHelper.getIdentifier(firstArg)));
                } else {
                    // multi field case
                    // for example, content:scoredPhrase((FIELD_A || FIELD_B), -1.5, termOffsetMap, 'value')
                    Set<String> identifiers = JexlASTHelper.getIdentifierNames(firstArg);
                    if (!identifiers.isEmpty()) {
                        fieldTerms.fields = identifiers;
                        if (oredFields != null && firstArg instanceof ASTAndNode) {
                            oredFields.setValue(false);
                        }
                    }
                }

                // skip score because it is not needed when gathering just the fields and values from a function
                argsIterator.next();
                return argsIterator.next();
            }
        }

        // Finds and sets the fields for a content:within functions, and returns the anticpatated terms offset map node.
        private JexlNode examineContentWithinFunction(PeekingIterator<JexlNode> argsIterator, FieldTerms fieldTerms, MutableBoolean oredFields) {
            JexlNode arg = argsIterator.next();
            // we override the zones if the first argument is a string or identifier
            if (arg instanceof ASTStringLiteral) {
                fieldTerms.fields = Collections.singleton(JexlNodes.getIdentifierOrLiteralAsString(arg));
                arg = argsIterator.next();
            } else if (!JexlASTHelper.getIdentifiers(arg).isEmpty()) {
                if (oredFields != null && arg instanceof ASTAndNode) {
                    oredFields.setValue(false);
                }

                fieldTerms.fields = JexlASTHelper.getIdentifierNames(arg);
                arg = argsIterator.next();
            }

            // we can trash the distance
            if (!(arg instanceof ASTNumberLiteral || arg instanceof ASTUnaryMinusNode)) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.NUMERIC_DISTANCE_ARGUMENT_MISSING);
                throw new IllegalArgumentException(qe);
            }

            return argsIterator.next();
        }

        /**
         * Throws a {@link BadRequestQueryException} if termsOffsetMap is not an instance of {@link ASTIdentifier} or if there are no more nodes in the
         * iterator.
         *
         * @param termOffsetMap
         *            the terms offset map node
         * @param argsIterator
         *            the iterator of arguments
         */
        private void validateTermsOffsetMapAndTermsPresent(JexlNode termOffsetMap, PeekingIterator<JexlNode> argsIterator) {
            if (!(termOffsetMap instanceof ASTIdentifier)) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.TERMOFFSETMAP_AND_TERMS_MISSING);
                throw new IllegalArgumentException(qe);
            }

            if (!argsIterator.hasNext()) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.TERMS_MISSING);
                throw new IllegalArgumentException(qe);
            }

        }

        /**
         * Distributes a function node into one or more components of an index query. If applicable, the function node will be updated with the field of the
         * intersected component of index query.
         * <p>
         * For example, for a function that expands into field F1
         *
         * <pre>
         * content:phrase(termOffsetMap, 'foo', 'bar')
         * </pre>
         *
         * becomes
         *
         * <pre>
         * (content:phrase(F1, termOffsetMap, 'foo', 'bar') &amp;&amp; F1 == 'foo' &amp;&amp; F2 == 'bar'))
         * </pre>
         *
         * @param function
         *            a function of indeterminate namespace
         * @param indexQuery
         *            an intersection of equality nodes or a union of such nodes
         * @return the root of a distributed index query
         */
        public static JexlNode distributeFunctionIntoIndexQuery(JexlNode function, JexlNode indexQuery) {

            List<JexlNode> components = new LinkedList<>();

            JexlNode deref = JexlASTHelper.dereference(indexQuery);
            if (deref instanceof ASTAndNode || deref instanceof ASTEQNode) {

                // the index query may be a single equality node when the phrase is composed of repeated terms
                // i.e., "phrase(termOffsetMap, 'bar', 'bar')" will produce an index query of "FOO == 'bar'"
                components.add(deref);

            } else if (deref instanceof ASTOrNode) {
                // multiple components
                for (int i = 0; i < deref.jjtGetNumChildren(); i++) {
                    components.add(JexlASTHelper.dereference(deref.jjtGetChild(i)));
                }
            } else {
                throw new IllegalStateException(
                                "Expected a dereferenced IndexQuery node to be an AND, OR, or EQ node, but was " + deref.getClass().getSimpleName());
            }

            // distribute functions into components
            List<JexlNode> rebuiltComponents = new LinkedList<>();
            for (JexlNode component : components) {
                rebuiltComponents.add(distributeFunctionIntoComponent(function, component));
            }

            // build proper return node
            switch (rebuiltComponents.size()) {
                case 0:
                    throw new IllegalStateException("Expected one or more index query components to exist but none do");
                case 1:
                    // single component, just return it
                    return rebuiltComponents.get(0);
                default:
                    // join multiple components under a union
                    return JexlNodeFactory.createOrNode(rebuiltComponents);
            }
        }

        /**
         * Distribute a function into a component of an index query. Update the function with the common field
         *
         * @param function
         *            an arbitrary function
         * @param component
         *            an ASTAndNode or ASTEqNode
         * @return nothing
         */
        public static JexlNode distributeFunctionIntoComponent(JexlNode function, JexlNode component) {
            List<JexlNode> children = new LinkedList<>();
            String field = findCommonField(component);
            if (field != null) {
                children.add(updateFunctionWithField(function, field));
                if (component instanceof ASTEQNode) {
                    children.add(component);
                } else if (component instanceof ASTAndNode) {
                    for (int i = 0; i < component.jjtGetNumChildren(); i++) {
                        children.add(component.jjtGetChild(i));
                    }
                } else {
                    throw new IllegalStateException("Unexpected component. Expected ASTAndNode or ASTEqNode but was: " + component.getClass().getSimpleName());
                }
                return JexlNodeFactory.createAndNode(children);
            } else {
                throw new IllegalStateException("Expected index query component to only contain a single field");
            }
        }

        /**
         * Extract the common field from the component. If more than one field exists, or none exist, return null.
         *
         * @param component
         *            a component of an index query
         * @return the common field, or null if no such field exists or more than one field exists
         */
        private static String findCommonField(JexlNode component) {
            Set<String> fields = new HashSet<>();
            List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(component);
            for (ASTIdentifier identifier : identifiers) {
                fields.add(JexlASTHelper.deconstructIdentifier(identifier));
            }

            if (fields.size() == 1) {
                return fields.iterator().next();
            } else {
                return null;
            }
        }

        /**
         * Return a copy of the provided content function, updated to always include the provided field
         *
         * @param function
         *            a content function
         * @param field
         *            the field common to a query index component
         * @return a copy of the original function with the specified field added as an argument
         */
        public static JexlNode updateFunctionWithField(JexlNode function, String field) {
            JexlNode functionCopy = RebuildingVisitor.copy(function);
            JexlNode replacementField = JexlNodeFactory.buildIdentifier(field);

            FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
            functionCopy.jjtAccept(visitor, null);

            // if a zone does not exist, add it as the first argument.
            // if the zone does exist, replace it with the common field from the index query component
            // within(int, ...)
            // adjacent(termOffsetMap, ...)
            // phrase(termOffsetMap, ...)
            // scoredPhrase(float, ...)

            // inspect the first argument to see if we need to insert the field
            boolean zoneExists = true;
            PeekingIterator<JexlNode> args = Iterators.peekingIterator(visitor.args().iterator());
            JexlNode first = args.peek();

            // switch on the function name
            switch (visitor.name()) {
                case CONTENT_PHRASE_FUNCTION_NAME:
                case CONTENT_ADJACENT_FUNCTION_NAME:
                    if (first instanceof ASTIdentifier) {
                        String identifier = JexlASTHelper.deconstructIdentifier((ASTIdentifier) first);
                        if (identifier.equals(TERM_OFFSET_MAP_JEXL_VARIABLE_NAME)) {
                            zoneExists = false;
                        }
                    }
                    break;
                case CONTENT_WITHIN_FUNCTION_NAME:
                case CONTENT_SCORED_PHRASE_FUNCTION_NAME:
                    if (first instanceof ASTNumberLiteral || first instanceof ASTUnaryMinusNode) {
                        zoneExists = false;
                    }
                    break;
                default:
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FUNCTION_NOT_FOUND,
                                    MessageFormat.format("Found unexpected content function with name: {0}", visitor.name()));
                    throw new IllegalArgumentException(qe);
            }

            List<JexlNode> updated = new LinkedList<>();
            updated.add(replacementField);

            ASTArguments argsNode = (ASTArguments) functionCopy.jjtGetChild(1);
            // Skip the first child if a zone exists.
            for (int i = zoneExists ? 1 : 0; i < argsNode.jjtGetNumChildren(); i++) {
                updated.add(argsNode.jjtGetChild(i));
            }
            JexlNodes.setChildren(argsNode, updated.toArray(new JexlNode[0]));
            return functionCopy;
        }

        /**
         * Get a space-delimited value when populating the HIT_TERMs
         *
         * @return the content args
         */
        public String getHitTermValue() {
            StringBuilder sb = new StringBuilder();
            JexlNode child;
            Iterator<JexlNode> iter = args.iterator();
            while (iter.hasNext()) {
                child = JexlASTHelper.dereference(iter.next());
                if (child instanceof ASTStringLiteral) {
                    sb.append(((ASTStringLiteral) child).getLiteral());
                    if (iter.hasNext()) {
                        sb.append(" ");
                    }
                }
            }
            return sb.toString();
        }

        public Set<String> getHitTermValues() {
            Set<String> values = new HashSet<>();
            JexlNode child;
            for (JexlNode arg : args) {
                child = JexlASTHelper.dereference(arg);
                if (child instanceof ASTStringLiteral) {
                    values.add(((ASTStringLiteral) child).getLiteral());
                }
            }
            return values;
        }

        @Override
        public boolean useOrForExpansion() {
            return true;
        }

        @Override
        public boolean regexArguments() {
            return false;
        }

        @Override
        public boolean allowIvaratorFiltering() {
            return true;
        }
    }

    public static class FieldTerms {

        private Set<String> fields;
        private Set<String> terms;

        public FieldTerms() {
            fields = null;
            terms = null;
        }

        public Set<String> getFields() {
            return fields;
        }

        public int totalFields() {
            return fields.size();
        }

        public Set<String> getTerms() {
            return terms;
        }
    }

    @Override
    public ContentJexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
        fvis.visit(node, null);

        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(fvis.namespace());

        if (!CONTENT_FUNCTION_NAMESPACE.equals(fvis.namespace())) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NAMESPACE_UNEXPECTED,
                            MessageFormat.format("Class: {0}, Namespace: {1}", this.getClass().getSimpleName(), fvis.namespace()));
            throw new IllegalArgumentException(qe);
        }
        if (!functionClass.equals(ContentFunctions.class)) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NODE_FOR_FUNCTION,
                            MessageFormat.format("Class: {0}, Function: {1}", this.getClass().getSimpleName(), functionClass));
            throw new IllegalArgumentException(qe);
        }

        return new ContentJexlArgumentDescriptor(node, fvis.namespace(), fvis.name(), fvis.args());
    }
}
