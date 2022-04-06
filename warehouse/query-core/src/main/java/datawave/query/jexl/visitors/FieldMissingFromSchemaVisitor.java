package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Class to check that each query node contains a field which exists in the schema.
 *
 * <pre>
 * 1. If a datatype filter was specified, then the existence check is limited to only those datatypes
 * 2. If a datatype filter is NOT specified (null or empty), this implies ALL datatypes.
 * </pre>
 */
public class FieldMissingFromSchemaVisitor extends BaseVisitor {
    
    private static final Logger log = Logger.getLogger(FieldMissingFromSchemaVisitor.class);
    
    private final MetadataHelper helper;
    private final Set<String> allFieldsForDatatypes; // All fields for the specified datatypes pulled from MetadataHelper
    private final Set<String> specialFields;
    private final Set<String> datatypeFilter;
    
    public FieldMissingFromSchemaVisitor(MetadataHelper helper, Set<String> datatypeFilter, Set<String> specialFields) {
        this.helper = helper;
        this.specialFields = specialFields;
        try {
            // if given datatypeFilter is empty or null, assume that means ALL datatypes
            if (datatypeFilter == null) {
                datatypeFilter = Collections.emptySet();
            }
            this.allFieldsForDatatypes = this.helper.getAllFields(datatypeFilter);
        } catch (TableNotFoundException e) {
            log.error(e);
            throw new RuntimeException("Unable to get metadata", e);
        }
        this.datatypeFilter = datatypeFilter;
    }
    
    @SuppressWarnings("unchecked")
    public static Set<String> getNonExistentFields(MetadataHelper helper, ASTJexlScript script, Set<String> datatypes, Set<String> specialFields) {
        FieldMissingFromSchemaVisitor visitor = new FieldMissingFromSchemaVisitor(helper, datatypes, specialFields);
        return (Set<String>) script.jjtAccept(visitor, new HashSet<>());
    }
    
    /**
     * @param node
     *            Jexl node
     * @param data
     *            The set of names which we have determined do not exist
     * @return the updated set of names which do not exist
     */
    protected Object genericVisit(JexlNode node, Object data) {
        @SuppressWarnings("unchecked")
        Set<String> nonExistentFieldNames = (null == data) ? new HashSet<>() : (Set<String>) data;
        List<ASTIdentifier> identifiers;
        
        // A node could be literal == literal in terms of an identityQuery
        try {
            identifiers = JexlASTHelper.getIdentifiers(node);
        } catch (NoSuchElementException e) {
            return nonExistentFieldNames;
        }
        
        if (identifiers.isEmpty()) {
            // Catch cases where we have two literals
            // essentially everything but identifier op literal
            return nonExistentFieldNames;
        }
        
        for (ASTIdentifier identifier : identifiers) {
            String fieldName = JexlASTHelper.deconstructIdentifier(identifier);
            if (!this.allFieldsForDatatypes.contains(fieldName) && !specialFields.contains(fieldName)) {
                nonExistentFieldNames.add(fieldName);
            }
        }
        return nonExistentFieldNames;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return genericVisit(node, data);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        @SuppressWarnings("unchecked")
        Set<String> nonExistentFieldNames = (null == data) ? new HashSet<>() : (Set<String>) data;
        
        for (String fieldName : desc.fields(this.helper, this.datatypeFilter)) {
            // deconstruct the identifier
            final String testFieldName = JexlASTHelper.deconstructIdentifier(fieldName);
            // changed to allow _ANYFIELD_ in functions
            if (!this.allFieldsForDatatypes.contains(testFieldName) && !specialFields.contains(fieldName)) {
                nonExistentFieldNames.add(testFieldName);
            }
        }
        
        return nonExistentFieldNames;
    }
    
    // Do not descend through these nodes
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    // Short Circuit at these nodes
    @Override
    public Object visit(SimpleNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
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
    public Object visit(ASTAssignment node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
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
    public Object visit(ASTAdditiveNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
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
    public Object visit(ASTIdentifier node, Object data) {
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
    public Object visit(ASTIntegerLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
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
    public Object visit(ASTSizeMethod node, Object data) {
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
