package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.StandardSyntaxParser;
import org.junit.Test;

import java.util.List;

public class BaseVisitorTest {
    
    @Test
    public void testQuery() throws QueryNodeParseException {
        parseAndPrint("(FIELD:1234 OR 456) AND (BAR:435 OR BAR:54)");
    }
    
    private void parseAndPrint(String query) throws QueryNodeParseException {
        System.out.println("Original query: " + query);
        parseAndPrintStandard(query);
        parseAndVisit(query);
    }
    
    private void parseAndPrintStandard(String query) throws QueryNodeParseException {
        StandardSyntaxParser parser = new StandardSyntaxParser();
        QueryNode node = parser.parse(query, "");
        System.out.println("Structure:");
        printNodeStructure(node, "");
        System.out.println("toString()");
        System.out.println(node.toString());
    }
    
    private void parseAndVisit(String query) throws QueryNodeParseException {
        StandardSyntaxParser parser = new StandardSyntaxParser();
        QueryNode node = parser.parse(query, "");
        BaseVisitor visitor = new BaseVisitor();
        visitor.visit(node, null);
    }
    
    private void printNodeStructure(QueryNode node, String indent) {
        List<QueryNode> children = node.getChildren();
        children = children == null ? List.of() : children;
        System.out.println(node.getClass().getName() + " " + children.size());
        String newIndent = indent + "  ";
        children.forEach((c) -> printNodeStructure(c, newIndent));
    }
}
