package nsa.datawave.query.rewrite.jexl.visitors;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;

import nsa.datawave.query.rewrite.jexl.JexlNodeFactory;
import nsa.datawave.query.rewrite.jexl.functions.RefactoredJexlFunctionArgumentDescriptorFactory;
import nsa.datawave.query.rewrite.jexl.functions.arguments.RefactoredJexlArgumentDescriptor;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Node;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

/**
 * 
 */
public class FunctionQueryModelRebuildingVisitor extends RebuildingVisitor {
    
    private Multimap<String,String> aliasMap;
    RefactoredJexlArgumentDescriptor desc;
    private static final Multimap EMPTY_MULTIMAP = new ImmutableMultimap.Builder().build();
    
    public FunctionQueryModelRebuildingVisitor(RefactoredJexlArgumentDescriptor desc, Multimap<String,String> aliasMap) {
        this.desc = desc;
        this.aliasMap = aliasMap;
    }
    
    public static ASTFunctionNode copyNode(ASTFunctionNode original) {
        return copyNode(original, EMPTY_MULTIMAP);
    }
    
    public static ASTFunctionNode copyNode(ASTFunctionNode original, Multimap<String,String> aliasMap) {
        FunctionQueryModelRebuildingVisitor visitor = new FunctionQueryModelRebuildingVisitor(
                        RefactoredJexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(original), aliasMap);
        
        return (ASTFunctionNode) visitor.visit(original, null);
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        JexlNode newNode = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        
        List<ASTIdentifier> nodes = Lists.newArrayList();
        // Set the alias if we have one, otherwise reuse the current name
        // Also apply '$' correction to the fieldName
        if (aliasMap.containsKey(node.image) && null != aliasMap.get(node.image)) {
            for (String alias : aliasMap.get(node.image)) {
                ASTIdentifier newKid = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
                newKid.image = JexlASTHelper.rebuildIdentifier(alias);
                nodes.add(newKid);
            }
            // always using OR for expansion in functions
            newNode = JexlNodeFactory.createUnwrappedOrNode(nodes);
            
        } else {
            newNode.image = JexlASTHelper.rebuildIdentifier(node.image);
        }
        
        newNode.jjtSetParent(node.jjtGetParent());
        
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            newNode.jjtAddChild((Node) node.jjtGetChild(i).jjtAccept(this, data), i);
        }
        
        return newNode;
    }
}
