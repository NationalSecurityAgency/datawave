package datawave.query.transformer.annotation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.PhraseSlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ProximityQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.query.language.parser.lucene.LuceneSyntaxQueryParser;

/**
 * Extracts annotation expressions from the raw Lucene syntax tree.
 *
 * <p>
 * This deliberately consumes the tree returned by the supplied parser, rather than parsing the string a second time. Consequently parser options (and
 * parser-specific node processing) remain part of the extraction contract.
 * </p>
 */
public class LuceneSearchExpressionExtractor implements Serializable {
    private static final long serialVersionUID = 1L;
    private final LuceneSyntaxQueryParser parser;
    private final Set<String> fields;
    private final Normalizer<String> normalizer;
    private final QueryExpressionExtractor generatedJexlExtractor;

    public LuceneSearchExpressionExtractor(LuceneSyntaxQueryParser parser, Set<String> fields) {
        this(parser, fields, new LcNoDiacriticsNormalizer());
    }

    public LuceneSearchExpressionExtractor(LuceneSyntaxQueryParser parser, Set<String> fields, Normalizer<String> normalizer) {
        if (parser == null)
            throw new IllegalArgumentException("A configured Lucene parser is required");
        this.parser = parser;
        this.fields = fields == null ? null : upper(fields);
        this.normalizer = normalizer == null ? new LcNoDiacriticsNormalizer() : normalizer;
        this.generatedJexlExtractor = new JexlSearchExpressionExtractor(fields, this.normalizer);
    }

    /** Extract raw Lucene expressions, plus proximity alternatives from generated JEXL. */
    public SearchExpressions extract(String macroExpandedLucene, String generatedOriginalJexl) {
        List<SearchExpression> expressions = new ArrayList<>();
        List<ProximityExpression> generated = new ArrayList<>();
        if (generatedOriginalJexl != null && !generatedOriginalJexl.trim().isEmpty()) {
            for (SearchExpression expression : generatedJexlExtractor.extract(generatedOriginalJexl)) {
                // Generated equality nodes are analyzer implementation details, not
                // explicit Lucene terms. Proximity functions preserve useful provenance.
                if (expression instanceof ProximityExpression) {
                    generated.add((ProximityExpression) expression);
                    expressions.add(expression);
                }
            }
        }
        try {
            collect(parser.parseToLuceneQueryNode(macroExpandedLucene), expressions, false, generated);
        } catch (QueryNodeParseException e) {
            throw new IllegalArgumentException(e);
        }
        return new SearchExpressions(expressions);
    }

    public SearchExpressions extract(String macroExpandedLucene) {
        return extract(macroExpandedLucene, null);
    }

    private void collect(QueryNode node, List<SearchExpression> out, boolean negated, List<ProximityExpression> generated) {
        boolean negative = negated;
        if (node instanceof ModifierQueryNode && ((ModifierQueryNode) node).getModifier() == ModifierQueryNode.Modifier.MOD_NOT)
            negative = !negative;

        if (node instanceof PhraseSlopQueryNode || node instanceof SlopQueryNode) {
            QueryNode child = node instanceof PhraseSlopQueryNode ? ((PhraseSlopQueryNode) node).getChild() : ((SlopQueryNode) node).getChild();
            if (!negative && child != null)
                addPhrase(child, node instanceof PhraseSlopQueryNode ? ((PhraseSlopQueryNode) node).getValue() : ((SlopQueryNode) node).getValue(), out,
                                generated);
            return;
        }
        if (node instanceof QuotedFieldQueryNode) {
            if (!negative)
                addPhrase(node, 1, out, generated);
            return;
        }
        if (node instanceof TokenizedPhraseQueryNode || node instanceof ProximityQueryNode) {
            if (!negative && node instanceof ProximityQueryNode) {
                ProximityQueryNode proximity = (ProximityQueryNode) node;
                List<StandalonePatternExpression> terms = terms(node.getChildren());
                if (!terms.isEmpty() && eligible(proximity.getFieldAsString()))
                    out.add(new ProximityExpression(proximity.isInOrder(), terms, proximity.getDistance() < 0 ? terms.size() - 1 : proximity.getDistance()));
            } else if (!negative) {
                List<StandalonePatternExpression> terms = terms(node.getChildren());
                if (!terms.isEmpty())
                    out.add(new ProximityExpression(true, terms, 1));
            }
            return;
        }
        if (!negative && node instanceof FieldQueryNode) {
            FieldQueryNode fieldNode = (FieldQueryNode) node;
            if (eligible(fieldNode.getFieldAsString()))
                out.add(new StandalonePatternExpression(normalize(fieldNode.getTextAsString(), fieldNode instanceof WildcardQueryNode)));
            return;
        }
        List<QueryNode> children = node.getChildren();
        if (children != null)
            for (QueryNode child : children)
                collect(child, out, negative, generated);
    }

    private void addPhrase(QueryNode node, int rawDistance, List<SearchExpression> out, List<ProximityExpression> generated) {
        String field = node instanceof FieldQueryNode ? ((FieldQueryNode) node).getFieldAsString() : null;
        if (!eligible(field))
            return;
        List<StandalonePatternExpression> terms;
        if (node instanceof FieldQueryNode) {
            terms = components(((FieldQueryNode) node).getTextAsString());
        } else {
            terms = terms(node.getChildren());
        }
        if (terms.size() > 1) {
            int distance = rawDistance <= 0 ? 1 : rawDistance;
            for (ProximityExpression alternative : generated) {
                if (sameTerms(terms, alternative.getComponents())) {
                    distance = alternative.getDistance();
                    break;
                }
            }
            out.add(new ProximityExpression(true, terms, distance));
        }
    }

    private List<StandalonePatternExpression> terms(List<QueryNode> nodes) {
        List<StandalonePatternExpression> result = new ArrayList<>();
        if (nodes != null)
            for (QueryNode child : nodes) {
                if (child instanceof FieldQueryNode) {
                    FieldQueryNode field = (FieldQueryNode) child;
                    result.add(new StandalonePatternExpression(normalize(field.getTextAsString(), field instanceof WildcardQueryNode)));
                }
            }
        return result;
    }

    private List<StandalonePatternExpression> components(String text) {
        List<StandalonePatternExpression> result = new ArrayList<>();
        for (String part : text.trim().split("\\s+"))
            if (!part.isEmpty())
                result.add(new StandalonePatternExpression(normalize(part, part.indexOf('*') >= 0 || part.indexOf('?') >= 0)));
        return result;
    }

    private boolean sameTerms(List<StandalonePatternExpression> left, List<StandalonePatternExpression> right) {
        if (left.size() != right.size())
            return false;
        for (int i = 0; i < left.size(); i++)
            if (!left.get(i).equals(right.get(i)))
                return false;
        return true;
    }

    private String normalize(String value, boolean regex) {
        return regex ? normalizer.normalizeRegex(value) : normalizer.normalize(value);
    }

    private boolean eligible(String field) {
        if (field == null || field.isEmpty())
            return true;
        if (fields == null || fields.contains("_ANYFIELD_"))
            return true;
        return fields.contains(field.toUpperCase(Locale.ROOT));
    }

    private static Set<String> upper(Set<String> input) {
        Set<String> result = new HashSet<>();
        for (String field : input)
            if (field != null)
                result.add(field.toUpperCase(Locale.ROOT));
        return result;
    }
}
