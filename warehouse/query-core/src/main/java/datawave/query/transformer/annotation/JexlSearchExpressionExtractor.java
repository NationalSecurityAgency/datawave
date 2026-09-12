package datawave.query.transformer.annotation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;

import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.ContentFunctions;
import datawave.query.jexl.visitors.PushdownNegationVisitor;
import datawave.query.parser.JavaRegexAnalyzer;

/** Extracts the annotation-relevant, positive expressions from an original JEXL tree. */
public class JexlSearchExpressionExtractor implements QueryExpressionExtractor, Serializable {
    private static final long serialVersionUID = 1L;
    private final Set<String> fields;
    private final Normalizer<String> normalizer;

    public JexlSearchExpressionExtractor(Set<String> fields) {
        this(fields, new LcNoDiacriticsNormalizer());
    }

    public JexlSearchExpressionExtractor(Set<String> fields, Normalizer<String> normalizer) {
        this.fields = fields == null ? null : upper(fields);
        this.normalizer = normalizer == null ? new LcNoDiacriticsNormalizer() : normalizer;
    }

    private static Set<String> upper(Set<String> input) {
        Set<String> result = new HashSet<>();
        for (String field : input) {
            if (field != null)
                result.add(field.toUpperCase(Locale.ROOT));
        }
        return result;
    }

    @Override
    public SearchExpressions extract(String query) {
        try {
            return extractChecked(query, normalizer);
        } catch (ParseException | JavaRegexAnalyzer.JavaRegexParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public SearchExpressions extractChecked(String query, Normalizer<String> valueNormalizer) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        JexlNode root = PushdownNegationVisitor.pushdownNegations(JexlASTHelper.parseJexlQuery(query));
        List<SearchExpression> result = new ArrayList<>();
        walk(root, result, valueNormalizer);
        return new SearchExpressions(result);
    }

    /** Shared standalone extraction retained for the legacy TermExtractor API. */
    public static Set<String> extractStandalone(String query, Set<String> fields, Normalizer<String> normalizer)
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        JexlNode root = PushdownNegationVisitor.pushdownNegations(JexlASTHelper.parseJexlQuery(query));
        Set<String> result = new HashSet<>();
        walkStandalone(root, result, fields == null ? null : upper(fields), normalizer);
        return result;
    }

    private void walk(JexlNode node, List<SearchExpression> result, Normalizer<String> valueNormalizer) throws JavaRegexAnalyzer.JavaRegexParseException {
        if (node instanceof ASTEQNode && acceptedLeaf(node)) {
            Object value = JexlASTHelper.getLiteralValueSafely(node);
            if (value != null)
                result.add(new StandalonePatternExpression(valueNormalizer.normalize(value.toString())));
        } else if (node instanceof ASTERNode && acceptedLeaf(node)) {
            Object value = JexlASTHelper.getLiteralValueSafely(node);
            if (value != null)
                result.add(new StandalonePatternExpression(valueNormalizer.normalizeRegex(value.toString())));
        } else if (node instanceof ASTFunctionNode && acceptedFunction(node)) {
            SearchExpression expression = function((ASTFunctionNode) node, valueNormalizer);
            if (expression != null)
                result.add(expression);
        }
        for (int i = 0; i < node.jjtGetNumChildren(); i++)
            walk(node.jjtGetChild(i), result, valueNormalizer);
    }

    private static void walkStandalone(JexlNode node, Set<String> result, Set<String> fields, Normalizer<String> normalizer)
                    throws JavaRegexAnalyzer.JavaRegexParseException {
        if (node instanceof ASTEQNode && acceptedLeaf(node, fields)) {
            Object value = JexlASTHelper.getLiteralValueSafely(node);
            if (value != null)
                result.add(normalizer.normalize(value.toString()));
        } else if (node instanceof ASTERNode && acceptedLeaf(node, fields)) {
            Object value = JexlASTHelper.getLiteralValueSafely(node);
            if (value != null)
                result.add(normalizer.normalizeRegex(value.toString()));
        }
        for (int i = 0; i < node.jjtGetNumChildren(); i++)
            walkStandalone(node.jjtGetChild(i), result, fields, normalizer);
    }

    private boolean acceptedLeaf(JexlNode node) {
        return acceptedLeaf(node, fields);
    }

    private static boolean acceptedLeaf(JexlNode node, Set<String> fields) {
        for (JexlNode parent = node.jjtGetParent(); parent != null; parent = parent.jjtGetParent())
            if (parent instanceof ASTNotNode)
                return false;
        String identifier = JexlASTHelper.getIdentifier(node);
        return identifier != null && (fields == null || fields.contains(identifier.toUpperCase(Locale.ROOT)));
    }

    private boolean acceptedFunction(JexlNode node) {
        for (JexlNode parent = node.jjtGetParent(); parent != null; parent = parent.jjtGetParent())
            if (parent instanceof ASTNotNode)
                return false;
        return true;
    }

    private SearchExpression function(ASTFunctionNode node, Normalizer<String> valueNormalizer) {
        ASTNamespaceIdentifier namespace = (ASTNamespaceIdentifier) node.jjtGetChild(0);
        String name = namespace.getName();
        if (!ContentFunctions.CONTENT_FUNCTION_NAMESPACE.equals(namespace.getNamespace()) || ContentFunctions.CONTENT_SCORED_PHRASE_FUNCTION_NAME.equals(name))
            return null;
        ASTArguments arguments = (ASTArguments) node.jjtGetChild(1);
        List<JexlNode> args = new ArrayList<>();
        for (int i = 0; i < arguments.jjtGetNumChildren(); i++)
            args.add(arguments.jjtGetChild(i));

        int termStart;
        int distance;
        boolean ordered = ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME.equals(name);
        if (ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME.equals(name) || ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME.equals(name)) {
            termStart = isMap(args, 0) ? 1 : 2;
            distance = ordered ? 1 : -1;
        } else if (ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME.equals(name)) {
            boolean hasZone = !isNumber(args, 0);
            int distanceIndex = hasZone ? 1 : 0;
            termStart = distanceIndex + 2;
            distance = number(args, distanceIndex);
        } else {
            return null;
        }
        if (termStart > args.size() || (termStart > 0 && !isMap(args, termStart - 1)))
            throw new IllegalArgumentException("Malformed content function: " + name);
        boolean fielded = termStart == 3 || (!ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME.equals(name) && termStart == 2 && !isMap(args, 0));
        if (fielded && !eligibleZone(args.get(0)))
            return null;
        List<StandalonePatternExpression> terms = new ArrayList<>();
        for (int i = termStart; i < args.size(); i++) {
            Object literal = JexlASTHelper.getLiteralValueSafely(args.get(i));
            if (literal == null || !(literal instanceof String))
                throw new IllegalArgumentException("Content function terms must be string literals");
            String source = valueNormalizer.normalize(literal.toString());
            terms.add(new StandalonePatternExpression(source));
        }
        if (terms.isEmpty())
            throw new IllegalArgumentException("Content function has no terms");
        if (!ordered && distance < 0)
            distance = terms.size() - 1;
        return new ProximityExpression(ordered, terms, distance);
    }

    private boolean eligibleZone(JexlNode zone) {
        if (fields == null || fields.contains(Constants.ANY_FIELD))
            return true;
        Set<String> names = new HashSet<>();
        Object literal = JexlASTHelper.getLiteralValueSafely(zone);
        if (literal != null)
            names.add(literal.toString());
        names.addAll(JexlASTHelper.getIdentifierNames(zone));
        for (String name : names)
            if (fields.contains(name.toUpperCase(Locale.ROOT)))
                return true;
        return false;
    }

    private static boolean isMap(List<JexlNode> args, int index) {
        if (index >= args.size() || !(args.get(index) instanceof ASTIdentifier))
            return false;
        return "termOffsetMap".equals(JexlASTHelper.getIdentifier(args.get(index)));
    }

    private static boolean isNumber(List<JexlNode> args, int index) {
        if (index >= args.size())
            return false;
        Object value = JexlASTHelper.getLiteralValueSafely(args.get(index));
        return value instanceof Number;
    }

    private static int number(List<JexlNode> args, int index) {
        if (!isNumber(args, index))
            throw new IllegalArgumentException("Content function distance must be numeric");
        return ((Number) JexlASTHelper.getLiteralValueSafely(args.get(index))).intValue();
    }
}
