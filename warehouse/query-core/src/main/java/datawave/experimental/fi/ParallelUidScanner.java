package datawave.experimental.fi;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import datawave.experimental.QueryTermVisitor;
import datawave.experimental.intersect.UidIntersection;
import datawave.experimental.intersect.UidIntersectionStrategy;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Scan terms in parallel
 */
public class ParallelUidScanner extends SerialUidScanner {

    private static final Logger log = Logger.getLogger(ParallelUidScanner.class);

    private final ExecutorService uidThreadPool;

    public ParallelUidScanner(ExecutorService uidThreadPool, AccumuloClient client, Authorizations auths, String tableName, String scanId) {
        super(client, auths, tableName, scanId);
        this.uidThreadPool = uidThreadPool;
    }

    @Override
    public Set<String> scan(ASTJexlScript script, String row, Set<String> indexedFields) {
        Set<JexlNode> terms = QueryTermVisitor.parse(script);
        log.info("scanning uids for " + terms.size() + " terms");
        long elapsed = System.currentTimeMillis();
        Map<String,Set<String>> nodesToUids = new HashMap<>();
        Set<Future<?>> submitted = new HashSet<>();
        for (JexlNode term : terms) {
            String field;
            if (term instanceof ASTAndNode) {
                // handle the bounded range case
                field = JexlASTHelper.getIdentifiers(term).get(0).image;
            } else {
                field = JexlASTHelper.getIdentifier(term);
            }

            if (indexedFields.contains(field)) {
                Future<?> f = uidThreadPool.submit(() -> {
                    String key = JexlStringBuildingVisitor.buildQueryWithoutParse(term);
                    Thread.currentThread().setName(scanId + " fi lookup " + key);
                    Set<String> uids = scanFieldIndexForTerm(row, field, term);
                    synchronized (nodesToUids) {
                        nodesToUids.put(key, uids);
                    }
                });
                submitted.add(f);
            }
        }

        int max = submitted.size();
        int count = 0;
        while (count < max) {
            count = 0;
            for (Future<?> f : submitted) {
                if (f.isDone() || f.isCancelled()) {
                    count++;
                }
            }

            if (count == max)
                break;

            // TODO -- need to handle the case where a critical node
            // in the query tree returns zero hits and we can cancel
            // all other executing futures

            // TODO -- can also handle the case where terms return
            // and we discover the query will never find a hit.
        }

        submitted.clear();

        elapsed = System.currentTimeMillis() - elapsed;
        log.info("scanned field index for " + max + "/" + terms.size() + " indexed fields in " + elapsed + " ms");
        UidIntersectionStrategy intersector = new UidIntersection();
        return intersector.intersect(script, nodesToUids);
    }
}
