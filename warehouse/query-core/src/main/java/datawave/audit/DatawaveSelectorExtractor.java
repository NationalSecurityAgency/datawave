package datawave.audit;

import java.util.ArrayList;
import java.util.List;
import datawave.query.jexl.JexlASTHelper;
import datawave.webservice.query.Query;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import org.apache.log4j.Logger;

public class DatawaveSelectorExtractor implements SelectorExtractor {

    private static final Logger log = Logger.getLogger(DatawaveSelectorExtractor.class);
    private LuceneToJexlQueryParser luceneToJexlQueryParser = new LuceneToJexlQueryParser();

    @Override
    public List<String> extractSelectors(Query query) throws IllegalArgumentException {
        List<String> selectorList = new ArrayList<>();
        try {
            ASTJexlScript jexlScript;
            try {
                // Parse & Flatten to reduce the number of node traversals in later method calls
                jexlScript = JexlASTHelper.parseAndFlattenJexlQuery(query.getQuery());
            } catch (Exception e) {
                // not JEXL, try LUCENE
                QueryNode node = luceneToJexlQueryParser.parse(query.getQuery());
                String jexlQuery = node.getOriginalQuery();
                jexlScript = JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery);
            }

            if (jexlScript != null) {
                List<ASTEQNode> eqNodes = JexlASTHelper.getPositiveEQNodes(jexlScript);
                for (ASTEQNode n : eqNodes) {
                    Object literal = JexlASTHelper.getLiteralValue(n);
                    if (literal != null) {
                        selectorList.add(literal.toString());
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return selectorList;
    }

    public void setLuceneToJexlQueryParser(LuceneToJexlQueryParser luceneToJexlQueryParser) {
        this.luceneToJexlQueryParser = luceneToJexlQueryParser;
    }
}
