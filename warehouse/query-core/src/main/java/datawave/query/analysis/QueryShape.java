package datawave.query.analysis;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTUnaryPlusNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.analysis.QueryAnalyzer.Category;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.nodes.QueryPropertyMarker;

/** Builds signatures without modifying or executing the parsed tree. */
final class QueryShape {
    final Set<Category> categories = EnumSet.noneOf(Category.class);
    final Set<String> fields = new TreeSet<>();
    final Set<String> functions = new TreeSet<>();

    String analyze(ASTJexlScript script) {
        if (script.jjtGetNumChildren() != 1) {
            throw new UnsupportedOperationException("Expected a single query expression");
        }
        String signature = shape(script.jjtGetChild(0));
        if (categories.isEmpty()) {
            categories.add(Category.OTHER);
        }
        return signature;
    }

    private String shape(JexlNode original) {
        JexlNode node = unwrap(original);
        if (node instanceof ASTAndNode || node instanceof ASTOrNode) {
            QueryPropertyMarker.Instance marker = QueryPropertyMarker.findInstance(node);
            if (marker.isAnyType()) {
                // The existing marker visitor checks names but not assignment values or placement. Do not let a malformed marker discard query structure.
                if (!hasMarkerAssignment(node, marker.getType().getLabel()) || marker.getSource() == null) {
                    throw new UnsupportedOperationException("Expected a query property marker assignment to true and a source expression");
                }
                categories.add(Category.PROPERTY_MARKER);
                return encode("MARKER:" + marker.getType().getLabel(), Collections.singletonList(shape(marker.getSource())));
            }
            categories.add(node instanceof ASTAndNode ? Category.CONJUNCTION : Category.DISJUNCTION);
            List<String> children = new ArrayList<>();
            collectJunction(node, node.getClass(), children);
            Collections.sort(children);
            return encode(node instanceof ASTAndNode ? "AND" : "OR", children);
        }
        if (node instanceof ASTFunctionNode) {
            FunctionJexlNodeVisitor function = FunctionJexlNodeVisitor.eval(node);
            if (function.name() == null || function.args() == null) {
                throw new UnsupportedOperationException("Unsupported function expression");
            }
            String name = function.namespace() + ":" + function.name();
            functions.add(name);
            categories.add(Category.FUNCTION);
            if ("content".equals(function.namespace())) {
                categories.add(Category.CONTENT);
            } else if ("geo".equals(function.namespace()) || "geowave".equals(function.namespace())) {
                categories.add(Category.GEOSPATIAL);
            }
            List<String> arguments = new ArrayList<>();
            for (JexlNode argument : function.args()) {
                JexlNode value = unwrap(argument);
                if ("content".equals(function.namespace()) && value instanceof ASTIdentifier && "termOffsetMap".equals(((ASTIdentifier) value).getName())) {
                    arguments.add("VARIABLE:termOffsetMap");
                } else {
                    arguments.add(shape(argument));
                }
            }
            return encode("FUNCTION:" + name, arguments);
        }
        if (node instanceof ASTIdentifier || isFieldReference(node)) {
            String name;
            if (node instanceof ASTIdentifier) {
                name = ((ASTIdentifier) node).getName();
            } else {
                StringBuilder field = new StringBuilder(((ASTIdentifier) node.jjtGetChild(0)).getName());
                for (int i = 1; i < node.jjtGetNumChildren(); i++) {
                    field.append('.').append(((ASTIdentifierAccess) node.jjtGetChild(i)).getName());
                }
                name = field.toString();
            }
            if ("_ANYFIELD_".equals(name)) {
                categories.add(Category.UNFIELDED);
            } else {
                fields.add(name);
            }
            return "FIELD:" + name;
        }
        if (node instanceof ASTStringLiteral || node instanceof ASTNumberLiteral) {
            return "VALUE";
        }
        if (node instanceof ASTNullLiteral) {
            return "NULL";
        }
        if (node instanceof ASTTrueNode || node instanceof ASTFalseNode) {
            categories.add(Category.OTHER);
            return node instanceof ASTTrueNode ? "TRUE" : "FALSE";
        }
        // Signed numeric values have the same shape as unsigned literals.
        if ((node instanceof ASTUnaryMinusNode || node instanceof ASTUnaryPlusNode) && node.jjtGetNumChildren() == 1
                        && unwrap(node.jjtGetChild(0)) instanceof ASTNumberLiteral) {
            return "VALUE";
        }
        if (node instanceof ASTEQNode || node instanceof ASTNENode) {
            categories.add(Category.EQUALITY);
            if (unwrap(node.jjtGetChild(0)) instanceof ASTNullLiteral || unwrap(node.jjtGetChild(1)) instanceof ASTNullLiteral) {
                categories.add(Category.NULL_CHECK);
            }
        } else if (node instanceof ASTERNode || node instanceof ASTNRNode) {
            categories.add(Category.REGEX);
        } else if (node instanceof ASTLTNode || node instanceof ASTLENode || node instanceof ASTGTNode || node instanceof ASTGENode) {
            categories.add(Category.RANGE);
        } else if (!(node instanceof ASTNotNode)) {
            throw new UnsupportedOperationException("Unsupported query expression: " + node.getClass().getSimpleName());
        }
        if (node instanceof ASTNotNode || node instanceof ASTNENode || node instanceof ASTNRNode) {
            categories.add(Category.NEGATION);
        }
        List<String> children = new ArrayList<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            children.add(shape(node.jjtGetChild(i)));
        }
        return encode(node.getClass().getSimpleName(), children);
    }

    private void collectJunction(JexlNode original, Class<?> type, List<String> children) {
        Deque<JexlNode> pending = new ArrayDeque<>();
        pending.push(original);
        while (!pending.isEmpty()) {
            JexlNode node = unwrap(pending.pop());
            if (node.getClass() == type && !QueryPropertyMarker.findInstance(node).isAnyType()) {
                // Reverse the push order to retain left-to-right traversal without recursing through wide junctions.
                for (int i = node.jjtGetNumChildren() - 1; i >= 0; i--) {
                    pending.push(node.jjtGetChild(i));
                }
            } else {
                children.add(shape(node));
            }
        }
    }

    private JexlNode unwrap(JexlNode node) {
        while ((node instanceof ASTReference || node instanceof ASTReferenceExpression) && node.jjtGetNumChildren() == 1) {
            node = node.jjtGetChild(0);
        }
        return node;
    }

    private boolean isFieldReference(JexlNode node) {
        if (!(node instanceof ASTReference) || node.jjtGetNumChildren() < 2 || !(node.jjtGetChild(0) instanceof ASTIdentifier)) {
            return false;
        }
        for (int i = 1; i < node.jjtGetNumChildren(); i++) {
            if (!(node.jjtGetChild(i) instanceof ASTIdentifierAccess)) {
                return false;
            }
        }
        return true;
    }

    private boolean hasMarkerAssignment(JexlNode node, String label) {
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            // Match the marker visitor's flattening: a parenthesized conjunction is a separate expression.
            if (child instanceof ASTAndNode && hasMarkerAssignment(child, label)) {
                return true;
            }
            child = unwrap(child);
            if (child instanceof ASTAssignment && child.jjtGetNumChildren() == 2) {
                JexlNode identifier = unwrap(child.jjtGetChild(0));
                if (identifier instanceof ASTIdentifier && label.equals(((ASTIdentifier) identifier).getName())
                                && unwrap(child.jjtGetChild(1)) instanceof ASTTrueNode) {
                    return true;
                }
            }
        }
        return false;
    }

    private String encode(String label, List<String> children) {
        // Length-prefix components so quoted identifiers cannot create signature collisions.
        StringBuilder result = new StringBuilder(label.length() + ":" + label + "(");
        for (String child : children) {
            result.append(child.length()).append(':').append(child);
        }
        return result.append(')').toString();
    }
}
