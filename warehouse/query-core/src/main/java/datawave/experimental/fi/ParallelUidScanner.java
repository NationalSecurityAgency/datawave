package datawave.experimental.fi;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import datawave.experimental.intersect.UidIntersection;
import datawave.experimental.intersect.UidIntersectionStrategy;
import datawave.experimental.visitor.QueryTermVisitor;
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
    public SortedSet<String> scan(ASTJexlScript script, String row, Set<String> indexedFields) {
        Set<JexlNode> terms = QueryTermVisitor.parse(script);
        long elapsed = System.currentTimeMillis();
        Map<String,Set<String>> nodesToUids = new HashMap<>();
        Set<Future<?>> submitted = new HashSet<>();
        for (JexlNode term : terms) {
            String field;
            if (term instanceof ASTAndNode) {
                // handle the bounded range case
                field = JexlASTHelper.getIdentifiers(term).get(0).getName();
            } else {
                field = JexlASTHelper.getIdentifier(term);
            }

            if (indexedFields.contains(field)) {
                TermScan scan = new TermScan(this, log, nodesToUids, row, field, term);
                ListenableFuture<TermScan> future = (ListenableFuture<TermScan>) uidThreadPool.submit(scan);
                Futures.addCallback(future, scan, uidThreadPool);
                submitted.add(future);
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

        if (logStats) {
            elapsed = System.currentTimeMillis() - elapsed;
            log.info("scanned field index for " + max + "/" + terms.size() + " indexed fields in " + elapsed + " ms");
        }

        UidIntersectionStrategy intersector = new UidIntersection();
        return intersector.intersect(script, nodesToUids);
    }

    public String getScanId() {
        return scanId;
    }

    static class TermScan implements Runnable, FutureCallback<TermScan> {

        private final ParallelUidScanner scanner;
        private final Logger log;
        private final Map<String,Set<String>> nodesToUids;
        private final String row;
        private final String field;
        private final JexlNode term;

        public TermScan(ParallelUidScanner scanner, Logger log, Map<String,Set<String>> nodesToUids, String row, String field, JexlNode term) {
            this.scanner = scanner;
            this.log = log;
            this.nodesToUids = nodesToUids;
            this.row = row;
            this.field = field;
            this.term = term;
        }

        @Override
        public void run() {
            String key = JexlStringBuildingVisitor.buildQueryWithoutParse(term);
            Thread.currentThread().setName(scanner.getScanId() + " fi lookup " + key);
            Set<String> uids = scanner.scanFieldIndexForTerm(row, field, term);
            synchronized (nodesToUids) {
                nodesToUids.put(key, uids);
            }
        }

        @Override
        public void onSuccess(TermScan result) {
            // log.info("TermScan: success");
        }

        @Override
        public void onFailure(Throwable t) {
            log.info("TermScan: failure");
            Throwables.throwIfUnchecked(t);
        }
    }
}
