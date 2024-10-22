package datawave.query.lucene.visitors;

import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import datawave.query.language.tree.QueryNode;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.junit.jupiter.api.Test;

public class FetchFunctionFieldsVisitorTest {
    
    @Test
    void testTimeFunction() throws QueryNodeParseException, ParseException {
        printQueryStructure("#INCLUDE(FIELDA, 'test')");
        System.out.println();
    }
    
    private void printQueryStructure(String query) throws QueryNodeParseException {
        SyntaxParser parser = new AccumuloSyntaxParser();
        org.apache.lucene.queryparser.flexible.core.nodes.QueryNode node = parser.parse(query, "");
        System.out.println("Query structure: " + query);
        PrintingVisitor.printToStdOut(node);
    }
    
    private String getAsJexl(String query) throws ParseException {
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        return parser.parse(query).getOriginalQuery();
    }
}
