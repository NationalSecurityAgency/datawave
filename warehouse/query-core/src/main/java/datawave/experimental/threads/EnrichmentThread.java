package datawave.experimental.threads;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import datawave.experimental.scanner.FieldIndexScanner;
import datawave.experimental.scanner.tf.TermFrequencyScanner;
import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.util.Tuple3;

/**
 * Fetches Index-Only and Term Frequency fields for a document
 * <p>
 * Enriches a document and pushes it to a candidate queue
 */
public class EnrichmentThread implements Runnable {

    private static final Logger log = Logger.getLogger(EnrichmentThread.class);
    private final LinkedBlockingQueue<Document> documentQueue;
    private final LinkedBlockingQueue<Tuple3<Key,Document,DatawaveJexlContext>> candidateQueue;

    private final FieldIndexScanner fiScanner;
    private final TermFrequencyScanner tfScanner;

    // misc variables
    private final String shard;
    private final Set<String> indexOnlyFields;
    private final Set<String> tfFields;
    private final Set<JexlNode> terms;
    private final ASTJexlScript script;
    private final AtomicBoolean scanningEvents;
    private final AtomicBoolean enrichingEvents;
    private final Long start;
    private final boolean logStats;

    public EnrichmentThread(LinkedBlockingQueue<Document> documentQueue, LinkedBlockingQueue<Tuple3<Key,Document,DatawaveJexlContext>> candidateQueue,
                    FieldIndexScanner fiScanner, TermFrequencyScanner tfScanner, String shard, Set<String> indexOnlyFields, Set<String> tfFields,
                    Set<JexlNode> terms, ASTJexlScript script, AtomicBoolean scanningEvents, AtomicBoolean enrichingEvents, Long start, boolean logStats) {
        this.documentQueue = documentQueue;
        this.candidateQueue = candidateQueue;
        this.fiScanner = fiScanner;
        this.tfScanner = tfScanner;
        this.shard = shard;
        this.indexOnlyFields = indexOnlyFields;
        this.tfFields = tfFields;
        this.terms = terms;
        this.script = script;
        this.scanningEvents = scanningEvents;
        this.enrichingEvents = enrichingEvents;
        this.start = start;
        this.logStats = logStats;
    }

    @Override
    public void run() {
        long documentCount = 0L;
        while (!documentQueue.isEmpty() || scanningEvents.get()) {
            try {
                Document d = documentQueue.poll(250, TimeUnit.MICROSECONDS);
                if (d == null) {
                    continue;
                }
                String dtUid = d.get("RECORD_ID").getMetadata().getColumnFamily().toString();

                if (fiScanner != null) {
                    fiScanner.fetchIndexOnlyFields(d, shard, dtUid, indexOnlyFields, terms);
                }

                DatawaveJexlContext context = new DatawaveJexlContext();

                if (tfScanner != null) {
                    tfScanner.setTermFrequencyFields(tfFields);
                    Map<String,Object> offsets = tfScanner.fetchOffsets(script, d, shard, dtUid);
                    d.visit(new HashSet<>(), context);
                    context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, offsets.get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME));
                } else {
                    d.visit(new HashSet<>(), context);
                }

                Tuple3<Key,Document,DatawaveJexlContext> result = new Tuple3<>(new Key(shard, dtUid), d, context);
                offer(result);
                documentCount++;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (logStats && documentCount > 0) {
            long elapsed = System.currentTimeMillis() - start;
            log.info("time to aggregate all documents for shard " + shard + ": " + documentCount + " in " + elapsed + " ms (" + (elapsed / documentCount)
                            + " ms per doc)");
        }

        enrichingEvents.set(false);
    }

    private void offer(Tuple3<Key,Document,DatawaveJexlContext> tuple) {
        boolean accepted = false;
        while (!accepted) {
            try {
                accepted = candidateQueue.offer(tuple, 250, TimeUnit.MICROSECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
