package datawave.experimental.fi;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.experimental.intersect.UidIntersection;
import datawave.experimental.intersect.UidIntersectionStrategy;
import datawave.experimental.visitor.QueryTermVisitor;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Serially scans the field index for a set of query terms.
 */
public class SerialUidScanner extends AbstractUidScanner {
    private static final Logger log = Logger.getLogger(SerialUidScanner.class);

    public SerialUidScanner(AccumuloClient client, Authorizations auth, String tableName, String scanId) {
        super(client, auth, tableName, scanId);
    }

    /**
     * Scans the field index serially for the set of query terms
     */
    @Override
    public Set<String> scan(ASTJexlScript script, String row, Set<String> indexedFields) {
        Set<JexlNode> terms = QueryTermVisitor.parse(script);
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
                Set<String> uids = scanFieldIndexForTerm(row, field, term);
                String key = JexlStringBuildingVisitor.buildQueryWithoutParse(term);
                nodesToUids.put(key, uids);
            } else {
                log.info("Field " + field + " is not indexed, do not look for it in the field index");
            }
        }

        if (logStats) {
            elapsed = System.currentTimeMillis() - elapsed;
            log.info("scanned field index for " + count + "/" + terms.size() + " indexed terms in " + elapsed + " ms");
        }

        UidIntersectionStrategy intersector = new UidIntersection();
        return intersector.intersect(script, nodesToUids);
    }

    @Override
    public void setLogStats(boolean logStats) {
        this.logStats = logStats;
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
        Range range = rangeBuilder.rangeFromTerm(shard, term);

        try (Scanner scanner = client.createScanner(tableName, auths)) {
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
            log.error(e);
        }
        return uids;
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
