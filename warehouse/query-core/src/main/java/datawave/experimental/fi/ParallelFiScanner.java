package datawave.experimental.fi;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Scan terms in parallel
 */
public class ParallelFiScanner extends FiScanner {
    
    private static final Logger log = Logger.getLogger(ParallelFiScanner.class);
    
    private final ExecutorService fieldIndexPool;
    
    public ParallelFiScanner(ExecutorService fieldIndexPool, String scanId, Connector conn, String tableName, Authorizations auths, MetadataHelper helper) {
        super(scanId, conn, tableName, auths, helper);
        this.fieldIndexPool = fieldIndexPool;
    }
    
    @Override
    public Map<String,Set<String>> scanFieldIndexForTerms(String shard, Set<JexlNode> terms, Set<String> indexedFields) {
        log.info("scanning field index for " + terms.size() + " terms");
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
                Future<?> f = fieldIndexPool.submit(() -> {
                    String key = JexlStringBuildingVisitor.buildQueryWithoutParse(term);
                    Thread.currentThread().setName(scanId + " fi lookup " + key);
                    Set<String> uids = scanFieldIndexForTerm(shard, field, term);
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
        return nodesToUids;
    }
}
