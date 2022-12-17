package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.QueryPhraseFieldConfig;
import datawave.query.model.QueryModel;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Node;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class QueryPhraseFieldVisitor extends RebuildingVisitor {
    
    private final QueryPhraseFieldConfig phraseConfig;
    private final Set<String> fields;
    
    public QueryPhraseFieldVisitor(QueryPhraseFieldConfig phraseConfig, Set<String> fields) {
        this.phraseConfig = phraseConfig;
        this.fields = fields;
    }
    
    public static ASTJexlScript applyPhraseExpansion(ASTJexlScript script, QueryPhraseFieldConfig phraseConfig, Set<String> fields) {
        QueryPhraseFieldVisitor visitor = new QueryPhraseFieldVisitor(phraseConfig, fields);
        
        script = TreeFlatteningRebuildingVisitor.flatten(script);
        return (ASTJexlScript) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        JexlNode newNode;
        String fieldName = JexlASTHelper.getIdentifier(node);
        
        String phrase = getFieldPhrase(fieldName); // de-dupe
        
        Set<ASTIdentifier> nodes = Sets.newLinkedHashSet();
        
        if (phrase.isEmpty()) {
            return super.visit(node, data);
        }
        
        ASTIdentifier newKid = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        newKid.image = JexlASTHelper.rebuildIdentifier(phrase);
        nodes.add(newKid);
        
        if (nodes.size() == 1) {
            newNode = JexlNodeFactory.wrap(nodes.iterator().next());
        } else {
            newNode = JexlNodeFactory.createOrNode(nodes);
        }
        newNode.jjtSetParent(node.jjtGetParent());
        
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            newNode.jjtAddChild((Node) node.jjtGetChild(i).jjtAccept(this, data), i);
        }
        return newNode;
    }
    
    private String getFieldPhrase(String fieldName) {
        String phrase = this.phraseConfig.getFieldPhrase(fieldName);
        return phrase;
    }
}
