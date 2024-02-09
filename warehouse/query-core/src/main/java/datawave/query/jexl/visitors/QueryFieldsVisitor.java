package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;

/**
 * Return a set of all fields present in the query
 */
public class QueryFieldsVisitor extends BaseVisitor {

    private final MetadataHelper helper;

    public static Set<String> parseQueryFields(String query, MetadataHelper helper) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            return parseQueryFields(script, helper);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return Collections.emptySet();
    }

    public static Set<String> parseQueryFields(ASTJexlScript script, MetadataHelper helper) {
        QueryFieldsVisitor visitor = new QueryFieldsVisitor(helper);
        return (Set<String>) script.jjtAccept(visitor, new HashSet<>());
    }

    public QueryFieldsVisitor(MetadataHelper helper) {
        this.helper = helper;
    }

    private Object parseSingleField(JexlNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        ((Set<String>) data).add(field);
        return data;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return parseSingleField(node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return parseSingleField(node, data);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return parseSingleField(node, data);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return parseSingleField(node, data);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return parseSingleField(node, data);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return parseSingleField(node, data);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return parseSingleField(node, data);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return parseSingleField(node, data);
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    /**
     * Have to handle JexlArg, GeoFunctions, and GeoWaveFunctions
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return list of IdentityContexts or QueryContexts
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        Set<String> fields = desc.fields(helper, null);
        ((Set<String>) data).addAll(fields);
        return data;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // Special handling for node with json-encoded params
        QueryPropertyMarker.Instance marker = QueryPropertyMarker.findInstance(node);
        if (marker.isType(EXCEEDED_OR)) {
            ExceededOr exceededOr = new ExceededOr(marker.getSource());
            if (exceededOr.getField() != null) {
                ((Set<String>) data).add(exceededOr.getField());
            }
            return data;
        }

        return super.visit(node, data);
    }

    // Short circuits

    @Override
    public Object visit(ASTBlock node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTAddNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTSubNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return data;
    }
}
