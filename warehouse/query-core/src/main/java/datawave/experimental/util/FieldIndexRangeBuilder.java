package datawave.experimental.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.parser.JavaRegexAnalyzer;

/**
 * Builds the correct field index scan range given a term
 */
public class FieldIndexRangeBuilder {

    public Range rangeFromTerm(String shard, JexlNode node) {
        return rangeFromTerm(shard, node, null, null);
    }

    public Range rangeFromTerm(String shard, JexlNode node, String firstDtUid) {
        return rangeFromTerm(shard, node, firstDtUid, null);
    }

    public Range rangeFromTerm(String shard, JexlNode node, String firstDtUid, String lastDtUid) {
        if (node instanceof ASTAndNode) {
            // probably have a bounded range
            if (QueryPropertyMarker.findInstance(node).isType(BoundedRange.class)) {
                return buildBoundedRange(shard, node);
            }
            throw new IllegalStateException("Expected a BoundedRange but was: " + JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        } else if (node instanceof ASTERNode || node instanceof ASTNRNode) {
            return buildRegexRange(shard, node);
        } else {
            return buildNormalRange(shard, node, firstDtUid, lastDtUid);
        }
    }

    // TODO -- pick this up
    private Range buildBoundedRange(String shard, JexlNode node) {
        JexlASTHelper.RangeFinder rangeFinder = new JexlASTHelper.RangeFinder();
        LiteralRange<?> literalRange = rangeFinder.getRange(node);

        String field = JexlASTHelper.getIdentifiers(node).get(0).image;
        Key startKey = new Key(shard, "fi\0" + field, literalRange.getLower().toString() + "\0");
        Key endKey = new Key(shard, "fi\0" + field, literalRange.getUpper().toString() + "\1");
        return new Range(startKey, literalRange.isLowerInclusive(), endKey, literalRange.isUpperInclusive());
    }

    /**
     *
     *
     * @param shard
     *            the shard
     * @param node
     *            a JexlNode representing a single query term
     * @return a range for a regex term
     */
    private Range buildRegexRange(String shard, JexlNode node) {
        try {
            String field = JexlASTHelper.getIdentifier(node);
            Object literal = JexlASTHelper.getLiteralValue(node);

            // todo -- normalize by type
            JavaRegexAnalyzer regex = new JavaRegexAnalyzer((String) literal);

            String queryTerm;
            if (regex.isLeadingLiteral()) {
                queryTerm = regex.getLeadingLiteral();
            } else {
                throw new IllegalStateException("We haven't implemented reverse regexes for the field index");
            }

            Key startKey = new Key(shard, "fi\0" + field, queryTerm);
            Key endKey = new Key(shard, "fi\0" + field, queryTerm + Constants.MAX_UNICODE_STRING);
            return new Range(startKey, false, endKey, false);

        } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Failed to parse regex for term: " + JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        }
    }

    /**
     * Build a standard range used for a normal equality node like {@code FOO == 'bar'}
     *
     * @param shard
     *            the shard
     * @param node
     *            a JexlNode representing the field and value
     * @return a range used to scan the field index
     */
    private Range buildNormalRange(String shard, JexlNode node, String firstDtUid, String lastDtUid) {
        String field = JexlASTHelper.getIdentifier(node);
        String value = (String) JexlASTHelper.getLiteralValue(node);

        Key startKey;
        if (firstDtUid == null) {
            startKey = new Key(shard, "fi\0" + field, value + "\0");
        } else {
            startKey = new Key(shard, "fi\0" + field, value + "\0" + firstDtUid);
        }

        Key endKey;
        if (lastDtUid == null) {
            endKey = new Key(shard, "fi\0" + field, value + "\1");
        } else {
            endKey = new Key(shard, "fi\0" + field, value + "\0" + lastDtUid); // in TLD this needs to be max value
        }
        return new Range(startKey, true, endKey, false);
    }
}
