package datawave.audit;

import java.util.ArrayList;
import java.util.List;
import datawave.query.jexl.JexlASTHelper;
import datawave.webservice.query.Query;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

public class DatawaveSelectorExtractor implements SelectorExtractor {
    
    private LuceneToJexlQueryParser luceneToJexlParser = new LuceneToJexlQueryParser();
    
    @Override
    public List<String> extractSelectors(Query query) throws IllegalArgumentException {
        List<String> selectorList = new ArrayList<>();
        List<ASTEQNode> eqNodes = null;
        QueryNode node = null;
        ASTJexlScript jexlScript = null;
        
        try {
            // Parse & Flatten to reduce the number of node traversals in later method calls
            jexlScript = JexlASTHelper.parseAndFlattenJexlQuery(query.getQuery());
        } catch (Throwable t1) {
            // not JEXL, try LUCENE
            try {
                node = luceneToJexlParser.parse(query.getQuery());
                String jexlQuery = node.getOriginalQuery();
                jexlScript = JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery);
            } catch (Throwable t2) {
                
            }
        }
        
        if (jexlScript != null) {
            eqNodes = JexlASTHelper.getPositiveEQNodes(jexlScript);
        }
        
        for (ASTEQNode n : eqNodes) {
            Object literal = JexlASTHelper.getLiteralValue(n);
            if (literal != null) {
                selectorList.add(literal.toString());
            }
        }
        return selectorList;
    }
}
