package datawave.experimental.threads;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.util.Tuple3;

/**
 * Evaluates a query against a context and pushes any matching documents to a result queue
 */
public class EvaluationThread implements Runnable {

    private static final Logger log = Logger.getLogger(EvaluationThread.class);

    private final String query;
    private final AtomicBoolean aggregating;
    private final AtomicBoolean evaluating;
    private final LinkedBlockingQueue<Tuple3<Key,Document,DatawaveJexlContext>> candidateQueue;
    private final LinkedBlockingQueue<Entry<Key,Document>> documentQueue;

    /**
     *
     * @param query
     *            the query
     * @param aggregating
     *            boolean that tells if the executor is still aggregating documents
     * @param candidateQueue
     *            the queue of candidate documents plus a context
     * @param documentQueue
     *            the queue of documents for additional post-processing
     */
    //@formatter:off
    public EvaluationThread(String query,
                    AtomicBoolean evaluating,
                    AtomicBoolean aggregating,
                    LinkedBlockingQueue<Tuple3<Key,Document,DatawaveJexlContext>> candidateQueue,
                    LinkedBlockingQueue<Entry<Key,Document>> documentQueue){
        //  @formatter:on
        this.query = query;
        this.evaluating = evaluating;
        this.aggregating = aggregating;
        this.candidateQueue = candidateQueue;
        this.documentQueue = documentQueue;
    }

    @Override
    public void run() {
        log.trace("evaluation thread start");
        long totalApply = 0L;
        long totalOffer = 0L;
        long totalThreadRunTime = System.currentTimeMillis();
        JexlEvaluation evaluation = new JexlEvaluation(query, new HitListArithmetic());
        while (!candidateQueue.isEmpty() || aggregating.get()) {
            try {
                Tuple3<Key,Document,DatawaveJexlContext> tuple = candidateQueue.poll(1, TimeUnit.MILLISECONDS);
                if (tuple == null) {
                    continue;
                }
                long applyDuration = System.currentTimeMillis();
                boolean matched = evaluation.apply(tuple);
                applyDuration = System.currentTimeMillis() - applyDuration;
                log.trace("apply duration: " + applyDuration);
                totalApply += applyDuration;

                if (matched) {
                    Entry<Key,Document> result = new AbstractMap.SimpleEntry<>(tuple.first(), tuple.second());
                    offerResult(result, totalOffer);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        totalThreadRunTime = System.currentTimeMillis() - totalThreadRunTime;

        log.trace("Evaluation Thread runtime[total: " + totalThreadRunTime + ", apply: " + totalApply + ", offer: " + totalOffer + "]");
        evaluating.set(false);
    }

    /**
     * Extracted offer logic because sonar lint doesn't like nested try-catch blocks
     *
     * @param result
     *            a document that passed evaluation
     * @param totalOfferDuration
     *            tracking how long we spend offering
     */
    private void offerResult(Entry<Key,Document> result, long totalOfferDuration) {
        boolean accepted = false;
        long offerDuration = System.currentTimeMillis();
        while (!accepted) {
            try {
                accepted = documentQueue.offer(result, 1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        offerDuration = System.currentTimeMillis() - offerDuration;
        log.trace("offer duration: " + offerDuration);
        totalOfferDuration += offerDuration;
    }
}
