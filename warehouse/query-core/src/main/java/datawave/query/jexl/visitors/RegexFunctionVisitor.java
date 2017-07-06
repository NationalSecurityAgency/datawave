package datawave.query.jexl.visitors;

import java.util.List;
import java.util.Set;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.config.RefactoredShardQueryConfiguration;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

import org.apache.commons.jexl2.parser.*;
import org.apache.log4j.Logger;

public class RegexFunctionVisitor extends FunctionIndexQueryExpansionVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(RegexFunctionVisitor.class);
    protected Set<String> indexOnlyFields;
    protected Set<String> termFrequencyFields;
    
    public RegexFunctionVisitor(RefactoredShardQueryConfiguration config, MetadataHelper metadataHelper, Set<String> indexOnlyFields,
                    Set<String> termFrequencyFields) {
        super(config, metadataHelper, null);
        this.indexOnlyFields = indexOnlyFields;
        this.termFrequencyFields = termFrequencyFields;
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T expandRegex(RefactoredShardQueryConfiguration config, MetadataHelper metadataHelper, Set<String> indexOnlyFields,
                    Set<String> termFrequencyFields, T script) {
        RegexFunctionVisitor visitor = new RegexFunctionVisitor(config, metadataHelper, indexOnlyFields, termFrequencyFields);
        
        return (T) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlNode returnNode = node;
        FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
        node.jjtAccept(functionMetadata, null);
        
        if (functionMetadata.name().equals("includeRegex") || functionMetadata.name().equals("excludeRegex")) {
            List<JexlNode> arguments = functionMetadata.args();
            JexlNode node0 = arguments.get(0);
            if (node0 instanceof ASTIdentifier) {
                JexlNode regexNode = buildRegexNode((ASTIdentifier) node0, functionMetadata.name(), arguments.get(1).image);
                if (regexNode != null) {
                    returnNode = regexNode;
                }
            } else {
                JexlNode newParent = null;
                if (functionMetadata.name().equals("excludeRegex")) {
                    newParent = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
                } else {
                    // it is likely an 'or' node...
                    newParent = JexlNodeFactory.shallowCopy(node0);
                }
                for (int i = 0; i < node0.jjtGetNumChildren(); i++) {
                    
                    JexlNode child = node0.jjtGetChild(i);
                    
                    if (child instanceof ASTIdentifier) {
                        this.adopt(newParent, buildRegexNode((ASTIdentifier) child, functionMetadata.name(), arguments.get(1).image), i);
                    } else { // probably a Reference
                        for (int j = 0; j < child.jjtGetNumChildren(); j++) {
                            JexlNode maybeIdentifier = child.jjtGetChild(j);
                            if (maybeIdentifier instanceof ASTIdentifier) {
                                this.adopt(newParent, buildRegexNode((ASTIdentifier) maybeIdentifier, functionMetadata.name(), arguments.get(1).image), i + j);
                            }
                        }
                    }
                }
                if (newParent.jjtGetNumChildren() == node0.jjtGetNumChildren() && newParent.jjtGetNumChildren() != 0) {
                    returnNode = newParent;
                }
            }
        } else if (functionMetadata.name().equals("isNull")) {
            List<JexlNode> arguments = functionMetadata.args();
            JexlNode node0 = arguments.get(0);
            if (node0 instanceof ASTIdentifier) {
                returnNode = JexlNodeFactory.buildNode(new ASTEQNode(ParserTreeConstants.JJTEQNODE), node0.image, new ASTNullLiteral(
                                ParserTreeConstants.JJTNULLLITERAL));
            }
        } else if (functionMetadata.name().equals("isNotNull")) {
            List<JexlNode> arguments = functionMetadata.args();
            JexlNode node0 = arguments.get(0);
            if (node0 instanceof ASTIdentifier) {
                returnNode = JexlNodeFactory.buildNode(new ASTNENode(ParserTreeConstants.JJTNENODE), node0.image, new ASTNullLiteral(
                                ParserTreeConstants.JJTNULLLITERAL));
            }
        }
        return returnNode;
    }
    
    private void adopt(JexlNode parent, JexlNode child, int i) {
        if (child != null && parent != null) {
            child.jjtSetParent(parent);
            parent.jjtAddChild(child, i);
        }
    }
    
    private JexlNode buildRegexNode(ASTIdentifier identifier, String functionName, String regex) {
        String field = JexlASTHelper.deconstructIdentifier(identifier.image);
        if (indexOnlyFields.contains(field.toUpperCase())) {
            try {
                JavaRegexAnalyzer jra = new JavaRegexAnalyzer(regex);
                if (!jra.isNgram()) {
                    JexlNode kid = null;
                    if (functionName.equals("includeRegex")) {
                        if (log.isDebugEnabled())
                            log.debug("Building new ER Node");
                        return JexlNodeFactory.buildERNode(field, regex);
                    } else {
                        return JexlNodeFactory.buildNRNode(field, regex);
                    }
                }
            } catch (JavaRegexParseException e) {
                // this will be caught later
                log.error(e);
            }
        }
        return null;
    }
}
