package datawave.query.jexl.functions;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTFunctionNode;

import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Argument-descriptor factory for the {@code document:*} JEXL namespace.
 * <p>
 * {@code document:match(...)} is an evaluation-only function. It does not contribute field normalization rules, event-data filters, index expansion, or
 * ivarator pushdown. This descriptor exists primarily to validate the namespace/function pairing and to return a descriptor that tells the planner to leave the
 * function in the evaluation phase.
 */
@SuppressWarnings("unused")
public class DocumentFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {

    /**
     * Descriptor for {@code document:match(...)}.
     * <p>
     * The function is evaluated only after a candidate document has been materialized, so all index-planning hooks intentionally report no fields and no index
     * query contribution.
     */
    public static class DocumentJexlArgumentDescriptor implements JexlArgumentDescriptor {
        @Override
        public org.apache.commons.jexl3.parser.JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper,
                        Set<String> datatypeFilter) {
            return TRUE_NODE;
        }

        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {}

        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            return Collections.emptySet();
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            return Collections.emptySet();
        }

        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            return Collections.emptySet();
        }

        @Override
        public boolean useOrForExpansion() {
            return false;
        }

        @Override
        public boolean regexArguments() {
            return false;
        }

        @Override
        public boolean allowIvaratorFiltering() {
            return false;
        }
    }

    /**
     * Validates that the supplied function node represents {@code document:match(...)} and returns the evaluation-only descriptor for it.
     *
     * @param node
     *            function node from the parsed JEXL tree
     * @return descriptor describing the planning behavior for {@code document:match(...)}
     * @throws IllegalArgumentException
     *             if the namespace, function class, or argument count is invalid
     */
    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor visitor = FunctionJexlNodeVisitor.eval(node);
        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(visitor.namespace());

        if (!DocumentFunctions.DOCUMENT_FUNCTION_NAMESPACE.equals(visitor.namespace())) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NAMESPACE_UNEXPECTED,
                            "Unexpected namespace " + visitor.namespace());
            throw new IllegalArgumentException(qe);
        }
        if (!functionClass.equals(DocumentFunctions.class)) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NODE_FOR_FUNCTION,
                            "Unexpected function class " + functionClass);
            throw new IllegalArgumentException(qe);
        }
        if (!DocumentFunctions.DOCUMENT_MATCH_FUNCTION_NAME.equals(visitor.name()) || visitor.args().isEmpty() || visitor.args().size() > 3) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.WRONG_NUMBER_OF_ARGUMENTS,
                            "Wrong number of arguments to document:match");
            throw new IllegalArgumentException(qe);
        }
        return new DocumentJexlArgumentDescriptor();
    }
}
