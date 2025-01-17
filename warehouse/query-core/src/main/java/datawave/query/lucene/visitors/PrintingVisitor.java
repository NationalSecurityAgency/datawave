package datawave.query.lucene.visitors;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.AnyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BoostQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.DeletedQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchAllDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NoTokenFoundQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OpaqueQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.PathQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.PhraseSlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ProximityQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.AbstractRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.BooleanModifierNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.MultiPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.SynonymQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

import datawave.query.Constants;

public class PrintingVisitor extends BaseVisitor {

    public interface Output {
        void writeLine(String line);
    }

    private static class PrintStreamOutput implements Output {

        private final PrintStream stream;

        private PrintStreamOutput(PrintStream stream) {
            this.stream = stream;
        }

        @Override
        public void writeLine(String line) {
            stream.println(line);
        }
    }

    private static class StringListOutput implements Output {
        private final List<String> lines;

        private StringListOutput() {
            this.lines = new ArrayList<>(32);
        }

        @Override
        public void writeLine(String line) {
            lines.add(line);
        }

        public List<String> getLines() {
            return lines;
        }
    }

    public static List<String> printToList(QueryNode node) {
        return ((StringListOutput) printToOutput(node, new StringListOutput())).getLines();
    }

    public static void printToStdOut(QueryNode node) {
        printToOutput(node, new PrintStreamOutput(System.out));
    }

    public static void printToStream(QueryNode node, PrintStream output) {
        printToOutput(node, new PrintStreamOutput(output));
    }

    public static Output printToOutput(QueryNode node, Output output) {
        PrintingVisitor visitor = new PrintingVisitor(output);
        visitor.visit(node, Constants.EMPTY_STRING);
        return output;
    }

    private static final String PREFIX = "  ";

    private final Output output;

    public PrintingVisitor(Output output) {
        this.output = output;
    }

    @Override
    public Object visit(AndQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(AnyQueryNode node, Object data) {
        String line = formatProperties(node, "field", node.getFieldAsString(), "minimumMatchingElements", node.getMinimumMatchingElements());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(FieldQueryNode node, Object data) {
        String line = formatProperties(node, "begin", node.getBegin(), "end", node.getEnd(), "field", node.getFieldAsString(), "text", node.getTextAsString());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(BooleanQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(BoostQueryNode node, Object data) {
        String line = formatProperties(node, "value", node.getValue());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(DeletedQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(FuzzyQueryNode node, Object data) {
        String line = formatProperties(node, "field", node.getFieldAsString(), "text", node.getTextAsString(), "similarity", node.getSimilarity());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(GroupQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(MatchAllDocsQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(MatchNoDocsQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(ModifierQueryNode node, Object data) {
        String line = formatProperties(node, "modifier", node.getModifier());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(NoTokenFoundQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(OpaqueQueryNode node, Object data) {
        String line = formatProperties(node, "schema", node.getSchema(), "value", node.getValue());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(OrQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(PathQueryNode node, Object data) {
        List<PathQueryNode.QueryText> pathElements = node.getPathElements();
        Integer begin = null;
        int end = 0;
        String path = null;
        for (PathQueryNode.QueryText element : pathElements) {
            if (begin == null) {
                begin = element.getBegin();
            }
            end = element.getEnd();
            path = Constants.FORWARD_SLASH + element.getValue();
        }

        String line = formatProperties(node, "begin", begin, "end", end, "path", path);
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(PhraseSlopQueryNode node, Object data) {
        String line = formatProperties(node, "value", node.getValue());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(ProximityQueryNode node, Object data) {
        String line = formatProperties(node, "distance", node.getDistance(), "field", node.getField(), "inorder", node.isInOrder(), "type",
                        node.getProximityType());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(QuotedFieldQueryNode node, Object data) {
        String line = formatProperties(node, "begin", node.getBegin(), "end", node.getEnd(), "field", node.getFieldAsString(), "text", node.getTextAsString());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(SlopQueryNode node, Object data) {
        String line = formatProperties(node, "value", node.getValue());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(TokenizedPhraseQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object visit(AbstractRangeQueryNode node, Object data) {
        String line = formatProperties(node, "lowerInclusive", node.isLowerInclusive(), "upperInclusive", node.isUpperInclusive());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(BooleanModifierNode node, Object data) {
        String line = formatProperties(node, "modifier", node.getModifier());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(MultiPhraseQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(PointQueryNode node, Object data) {
        String line = formatProperties(node, "field", node.getField(), "number", node.getNumberFormat().format(node.getValue()));
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(PointRangeQueryNode node, Object data) {
        String line = formatProperties(node, "lowerInclusive", node.isLowerInclusive(), "upperInclusive", node.isUpperInclusive());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(PrefixWildcardQueryNode node, Object data) {
        String line = formatProperties(node, "begin", node.getBegin(), "end", node.getEnd(), "field", node.getFieldAsString(), "text", node.getTextAsString());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(RegexpQueryNode node, Object data) {
        String line = formatProperties(node, "field", node.getField(), "text", node.getText());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(SynonymQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    @Override
    public Object visit(TermRangeQueryNode node, Object data) {
        String line = formatProperties(node, "lowerInclusive", node.isLowerInclusive(), "upperInclusive", node.isUpperInclusive());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(WildcardQueryNode node, Object data) {
        String line = formatProperties(node, "begin", node.getBegin(), "end", node.getEnd(), "field", node.getFieldAsString(), "text", node.getTextAsString());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(FunctionQueryNode node, Object data) {
        String line = formatProperties(node, "begin", node.getBegin(), "end", node.getEnd(), "function", node.getFunction(), "parameters",
                        node.getParameterList());
        return writeLineAndVisitChildren(node, data, line);
    }

    @Override
    public Object visit(NotBooleanQueryNode node, Object data) {
        return writeNameAndVisitChildren(node, data);
    }

    private Object writeNameAndVisitChildren(QueryNode node, Object data) {
        return writeLineAndVisitChildren(node, data, node.getClass().getSimpleName());
    }

    private Object writeLineAndVisitChildren(QueryNode node, Object data, String line) {
        String prefix = (String) data;
        output.writeLine(prefix + line);
        prefix = prefix + PREFIX;
        visitChildren(node, prefix);
        return null;
    }

    private String formatProperties(QueryNode node, Object... properties) {
        int arrLength = properties.length;
        if (arrLength % 2 == 1) {
            throw new IllegalArgumentException("Properties array must consist of property name followed by property value");
        }
        StringBuilder sb = new StringBuilder();
        sb.append(node.getClass().getSimpleName());
        for (int i = 0; i < arrLength; i += 2) {
            sb.append(Constants.COMMA).append(Constants.SPACE).append(properties[i]).append(Constants.EQUALS).append(properties[(i + 1)]);
        }
        return sb.toString();
    }
}
