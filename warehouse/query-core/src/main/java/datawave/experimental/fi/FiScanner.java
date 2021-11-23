package datawave.experimental.fi;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Serially scans the field index for a set of query terms.
 */
public class FiScanner implements FiScannerStrategy {
    
    private static final Logger log = Logger.getLogger(FiScanner.class);
    
    protected String scanId;
    protected Connector conn;
    
    protected String tableName;
    protected Authorizations auths;
    protected MetadataHelper helper;
    
    public FiScanner(String scanId, Connector conn, String tableName, Authorizations auths, MetadataHelper helper) {
        this.scanId = scanId;
        this.conn = conn;
        this.tableName = tableName;
        this.auths = auths;
        this.helper = helper;
    }
    
    /**
     * Scans the field index serially for the set of query terms
     *
     * @param shard
     *            the shard
     * @param terms
     *            the query terms
     * @param indexedFields
     *            the set of indexed fields, used to limit the lookups
     * @return a map of JexlNode to uids
     */
    @Override
    public Map<String,Set<String>> scanFieldIndexForTerms(String shard, Set<JexlNode> terms, Set<String> indexedFields) {
        log.info("scanning field index for " + terms.size() + " terms");
        long elapsed = System.currentTimeMillis();
        int count = 0;
        Map<String,Set<String>> nodesToUids = new HashMap<>();
        for (JexlNode term : terms) {
            String field;
            if (term instanceof ASTAndNode) {
                // handle the bounded range case
                field = JexlASTHelper.getIdentifiers(term).get(0).image;
            } else {
                field = JexlASTHelper.getIdentifier(term);
                
                // FIELD == null is handled at evaluation time, no field index work required
                try {
                    String value = (String) JexlASTHelper.getLiteralValue(term);
                    if (value == null) {
                        continue;
                    }
                } catch (NoSuchElementException e) {
                    continue;
                }
            }
            
            if (indexedFields.contains(field)) {
                count++;
                Set<String> uids = scanFieldIndexForTerm(shard, field, term);
                String key = JexlStringBuildingVisitor.buildQueryWithoutParse(term);
                nodesToUids.put(key, uids);
            } else {
                log.info("Field " + field + " is not indexed, do not look for it in the field index");
            }
        }
        elapsed = System.currentTimeMillis() - elapsed;
        log.info("scanned field index for " + count + "/" + terms.size() + " indexed terms in " + elapsed + " ms");
        return nodesToUids;
    }
    
    /**
     * Scan the field index for the provided term
     *
     * @param shard
     *            the shard
     * @param field
     *            the field
     * @param term
     *            a JexlNode representing the field and value
     * @return a set of uids
     */
    protected Set<String> scanFieldIndexForTerm(String shard, String field, JexlNode term) {
        Set<String> uids = new HashSet<>();
        Range range = rangeFromTerm(shard, term);
        
        try (Scanner scanner = conn.createScanner(tableName, auths)) {
            scanner.setRange(range);
            if (!field.equals(Constants.ANY_FIELD)) {
                scanner.fetchColumnFamily(new Text("fi\0" + field));
            } else {
                throw new IllegalStateException("Should never have _ANYFIELD_ in this stage.");
            }
            
            for (Map.Entry<Key,Value> entry : scanner) {
                uids.add(dtUidFromKey(entry.getKey()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return uids;
    }
    
    /**
     * Given a JexlNode representing a query term, build a range so the field index can be scanned
     *
     * @param shard
     *            the shard
     * @param node
     *            a JexlNode representing a single query term
     * @return a range built for the field index
     */
    private Range rangeFromTerm(String shard, JexlNode node) {
        if (node instanceof ASTAndNode) {
            // probably have a bounded range
            if (QueryPropertyMarker.findInstance(node).isType(BoundedRange.class)) {
                return buildBoundedRange(shard, node);
            }
            throw new IllegalStateException("Expected a BoundedRange but was: " + JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        } else if (node instanceof ASTERNode || node instanceof ASTNRNode) {
            return buildRegexRange(shard, node);
        } else {
            return buildNormalRange(shard, node);
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
     * @param node
     *            a JexlNode representing a single query term
     * @return
     */
    private Range buildRegexRange(String shard, JexlNode node) {
        try {
            String field = JexlASTHelper.getIdentifier(node);
            Object literal = JexlASTHelper.getLiteralValue(node);
            
            // todo -- normalize by type
            JavaRegexAnalyzer regex = new JavaRegexAnalyzer((String) literal);
            
            String queryTerm = null;
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
    private Range buildNormalRange(String shard, JexlNode node) {
        String field = JexlASTHelper.getIdentifier(node);
        String value = (String) JexlASTHelper.getLiteralValue(node);
        
        Key startKey = new Key(shard, "fi\0" + field, value + "\0");
        Key endKey = new Key(shard, "fi\0" + field, value + "\1");
        return new Range(startKey, true, endKey, false);
    }
    
    /**
     * Extract the datatype and uid from the field index key
     *
     * @param key
     *            a field index key
     * @return the datatype and uid
     */
    private String dtUidFromKey(Key key) {
        String cq = key.getColumnQualifier().toString();
        int index = cq.indexOf('\u0000');
        return cq.substring(index + 1);
    }
}
