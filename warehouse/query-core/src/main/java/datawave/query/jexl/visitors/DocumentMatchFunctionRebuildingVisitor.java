package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.DocumentFunctions;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;

/**
 * Rewrites user-facing {@code document:match(...)} calls into the internal evaluation form that carries the reserved {@code documentMatchContext} argument
 * explicitly.
 * <p>
 * This mirrors the way {@code content:*} functions are evaluated with an explicit {@code termOffsetMap} argument, but preserves the external user syntax for
 * document matching.
 */
public class DocumentMatchFunctionRebuildingVisitor extends RebuildingVisitor {
    protected static final Logger log = Logger.getLogger(DocumentMatchFunctionRebuildingVisitor.class);

    private DocumentMatchFunctionRebuildingVisitor() {
        // no-op, local construction only.
    }

    /**
     * Determines whether the supplied script contains any {@code document:match(...)} calls.
     *
     * @param script
     *            script to inspect
     * @return {@code true} if any document-match functions are present
     */
    public static boolean requiresDocumentMatchContext(ASTJexlScript script) {
        return JexlASTHelper.getFunctionNodes(script).stream().map(FunctionJexlNodeVisitor::eval)
                        .anyMatch(function -> DocumentFunctions.DOCUMENT_FUNCTION_NAMESPACE.equals(function.namespace())
                                        && DocumentFunctions.DOCUMENT_MATCH_FUNCTION_NAME.equals(function.name()));
    }

    /**
     * Rewrites all {@code document:match(...)} calls in the supplied script to include the reserved context identifier.
     *
     * @param script
     *            script to rewrite
     * @return rewritten script
     */
    public static ASTJexlScript rewrite(ASTJexlScript script) {
        return (ASTJexlScript) script.jjtAccept(new DocumentMatchFunctionRebuildingVisitor(), null);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        FunctionJexlNodeVisitor visitor = FunctionJexlNodeVisitor.eval(node);
        if (DocumentFunctions.DOCUMENT_FUNCTION_NAMESPACE.equals(visitor.namespace())) {
            return handeDocumentFunction(visitor, data);
        }
        return data; // no-op
    }

    protected Object handeDocumentFunction(FunctionJexlNodeVisitor visitor, Object data) {
        // noinspection SwitchStatementWithTooFewBranches - placeholder for future expansion
        switch (visitor.name()) {
            case DocumentFunctions.DOCUMENT_MATCH_FUNCTION_NAME:
                if (visitor.args().size() == 1) {
                    return FunctionJexlNodeVisitor.makeFunctionFrom(visitor.namespace(), visitor.name(),
                                    JexlNodeFactory.buildIdentifier(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME),
                                    RebuildingVisitor.copy(visitor.args().get(0)));
                } else if (visitor.args().size() == 2) {
                    return FunctionJexlNodeVisitor.makeFunctionFrom(visitor.namespace(), visitor.name(), RebuildingVisitor.copy(visitor.args().get(0)),
                                    JexlNodeFactory.buildIdentifier(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME),
                                    RebuildingVisitor.copy(visitor.args().get(1)));
                }
            default:
                log.warn("unknown document function:" + visitor.name());
                return data; // no-op
        }
    }
}
