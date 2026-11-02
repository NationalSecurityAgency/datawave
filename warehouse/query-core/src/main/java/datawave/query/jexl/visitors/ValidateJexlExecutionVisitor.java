package datawave.query.jexl.visitors;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.DatawaveJexlPermissions;

/**
 * Reject forbidden calls before execution. In particular, strict(false) must not turn a forbidden function into null and a negated function into a match.
 * Runtime permissions independently constrain the classes on which approved method names can be invoked.
 */
public final class ValidateJexlExecutionVisitor {
    private static final Set<String> FORBIDDEN_PROPERTIES = Set.of("class", "classLoader", "source", "sourceMetadata");

    private ValidateJexlExecutionVisitor() {}

    public static void validate(ASTJexlScript script, Map<String,Object> functions) {
        // Queries can contain deeply nested OR trees. Do not consume a Java stack frame for every node.
        Deque<JexlNode> pending = new ArrayDeque<>();
        pending.push(script);
        while (!pending.isEmpty()) {
            JexlNode node = pending.pop();
            if (node instanceof ASTFunctionNode) {
                validateFunction(node, functions);
            } else if (node instanceof ASTMethodNode) {
                validateMethod(node);
            } else if (node instanceof ASTIdentifierAccess) {
                validateProperty(node, ((ASTIdentifierAccess) node).getName());
            } else if (node instanceof ASTArrayAccess) {
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    if (node.jjtGetChild(i) instanceof ASTStringLiteral) {
                        validateProperty(node, ((ASTStringLiteral) node.jjtGetChild(i)).getLiteral());
                    }
                }
            }
            for (int i = node.jjtGetNumChildren() - 1; i >= 0; i--) {
                pending.push(node.jjtGetChild(i));
            }
        }
    }

    private static void validateFunction(JexlNode node, Map<String,Object> functions) {
        if (!(node.jjtGetChild(0) instanceof ASTNamespaceIdentifier)) {
            throw new JexlException(node, "Only registered DataWave query functions are permitted");
        }
        ASTNamespaceIdentifier function = (ASTNamespaceIdentifier) node.jjtGetChild(0);
        if (!DatawaveJexlPermissions.allowsFunction(functions.get(function.getNamespace()), function.getName())) {
            throw new JexlException(node, "Query function is not permitted: " + function.getNamespace() + ":" + function.getName());
        }
    }

    private static void validateMethod(JexlNode node) {
        if (!(node.jjtGetChild(0) instanceof ASTIdentifierAccess)
                        || !DatawaveJexlPermissions.allowsQueryMethod(((ASTIdentifierAccess) node.jjtGetChild(0)).getName())) {
            throw new JexlException(node, "Query method is not permitted");
        }
    }

    private static void validateProperty(JexlNode node, String name) {
        if (FORBIDDEN_PROPERTIES.contains(name)) {
            throw new JexlException(node, "Query property is not permitted: " + name);
        }
    }
}
