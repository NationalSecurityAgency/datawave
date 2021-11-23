package datawave.experimental.util;

import java.util.concurrent.atomic.AtomicLong;

public class ScanStats {

    // next counts
    private final AtomicLong nextFieldIndex = new AtomicLong();
    private final AtomicLong nextDocumentAggregation = new AtomicLong();
    private final AtomicLong nextTermFrequency = new AtomicLong();

    // the number of candidate uids found in the field index
    private final AtomicLong uidsTotal = new AtomicLong();
    private final AtomicLong documentsEvaluated = new AtomicLong(); // documents seen
    private final AtomicLong documentsReturned = new AtomicLong(); // documents matched

    public ScanStats() {
        // empty constructor
    }

    public ScanStats merge(ScanStats other) {
        this.nextFieldIndex.addAndGet(other.nextFieldIndex.get());
        this.nextDocumentAggregation.addAndGet(other.nextDocumentAggregation.get());
        this.nextTermFrequency.addAndGet(other.nextTermFrequency.get());

        this.uidsTotal.addAndGet(other.uidsTotal.get());
        this.documentsEvaluated.addAndGet(other.documentsEvaluated.get());
        this.documentsReturned.addAndGet(other.documentsReturned.get());

        return this;
    }

    public void incrementNextFieldIndex() {
        nextFieldIndex.getAndIncrement();
    }

    public void incrementNextDocumentAggregation() {
        nextDocumentAggregation.getAndIncrement();
    }

    public void incrementNextTermFrequency() {
        nextTermFrequency.getAndIncrement();
    }

    public void incrementUidsTotal(long delta) {
        uidsTotal.getAndAdd(delta);
    }

    public void incrementDocumentsEvaluated() {
        documentsEvaluated.getAndIncrement();
    }

    public void incrementDocumentsReturned() {
        documentsEvaluated.getAndIncrement();
    }

    public Long getNextTotal() {
        return nextFieldIndex.get() + nextDocumentAggregation.get() + nextTermFrequency.get();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("next.fi: ").append(nextFieldIndex.get()).append('\n');
        sb.append("next.doc: ").append(nextDocumentAggregation.get()).append('\n');
        sb.append("next.tf: ").append(nextTermFrequency.get()).append('\n');
        sb.append("next.total: ").append(getNextTotal()).append('\n');
        sb.append("uids.total: ").append(uidsTotal.get()).append('\n');
        sb.append("docs.evaluated: ").append(documentsEvaluated.get()).append('\n');
        sb.append("docs.returned: ").append(documentsReturned.get()).append('\n');
        return sb.toString();
    }

}
