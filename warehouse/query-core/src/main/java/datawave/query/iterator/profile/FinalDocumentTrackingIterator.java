package datawave.query.iterator.profile;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.hadoop.io.Text;

import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.function.LogTiming;
import datawave.query.iterator.Util;

public class FinalDocumentTrackingIterator implements Iterator<Entry<Key,Document>> {

    private final Iterator<Entry<Key,Document>> itr;
    private boolean itrIsDone;
    private boolean statsEntryReturned;
    private Key lastKey = null;

    public static final Text MARKER_TEXT = new Text("\u2735FinalDocument\u2735");
    public static final ByteSequence MARKER_SEQUENCE = new ArrayByteSequence(MARKER_TEXT.getBytes(), 0, MARKER_TEXT.getLength());

    private final Range seekRange;
    private final QuerySpanCollector querySpanCollector;
    private final QuerySpan querySpan;
    private final YieldCallback yield;

    @Deprecated
    public FinalDocumentTrackingIterator(QuerySpanCollector querySpanCollector, QuerySpan querySpan, Range seekRange, Iterator<Entry<Key,Document>> itr,
                    DocumentSerialization.ReturnType returnType, boolean isReducedResponse, boolean isCompressResults, YieldCallback<Key> yield) {
        this(querySpanCollector, querySpan, seekRange, itr, yield);
    }

    public FinalDocumentTrackingIterator(QuerySpanCollector querySpanCollector, QuerySpan querySpan, Range seekRange, Iterator<Entry<Key,Document>> itr,
                    YieldCallback<Key> yield) {
        this.itr = itr;
        this.seekRange = seekRange;
        this.querySpanCollector = querySpanCollector;
        this.querySpan = querySpan;
        this.yield = yield;

        // check for the special case where we were torn down just after returning the final document
        this.itrIsDone = this.statsEntryReturned = isStatsEntryReturned(seekRange);
    }

    private boolean isStatsEntryReturned(Range r) {
        // first check if this is a rebuild key (post teardown)
        if (!r.isStartKeyInclusive()) {
            // now check if the start key is a final document return
            return isFinalDocumentKey(r.getStartKey());
        }
        return false;
    }

    public static boolean isFinalDocumentKey(Key k) {
        if (k != null && k.getColumnQualifierData() != null) {
            ByteSequence bytes = k.getColumnQualifierData();
            if (bytes.length() >= MARKER_TEXT.getLength()) {
                return (bytes.subSequence(bytes.length() - MARKER_TEXT.getLength(), bytes.length()).compareTo(MARKER_SEQUENCE) == 0);
            }
        }
        return false;
    }

    private Entry<Key,Document> getStatsEntry(Key statsKey) {

        // now add our marker
        statsKey = new Key(statsKey.getRow(), statsKey.getColumnFamily(), Util.appendText(statsKey.getColumnQualifier(), MARKER_TEXT),
                        statsKey.getColumnVisibility(), statsKey.getTimestamp());

        HashMap<Key,Document> documentMap = new HashMap<>();

        QuerySpan combinedQuerySpan = querySpanCollector.getCombinedQuerySpan(this.querySpan);
        if (combinedQuerySpan != null) {
            Document document = new Document();
            LogTiming.addTimingMetadata(document, combinedQuerySpan);
            documentMap.put(statsKey, document);
        }

        return documentMap.entrySet().iterator().next();
    }

    @Override
    public boolean hasNext() {
        if (!itrIsDone && itr.hasNext()) {
            return true;

        } else {
            // if we yielded, then leave gracefully
            // checking again as this is after the itr.hasNext() call above which may cause a yield
            if (yield != null && yield.hasYielded()) {
                return false;
            }

            itrIsDone = true;
            if (!statsEntryReturned) {
                if (this.querySpan.hasEntries() || querySpanCollector.hasEntries()) {
                    return true;
                } else {
                    statsEntryReturned = true;
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public Entry<Key,Document> next() {

        Entry<Key,Document> nextEntry = null;
        if (yield == null || !yield.hasYielded()) {
            if (itrIsDone) {
                if (!statsEntryReturned) {

                    // determine the key to append the stats entry to
                    Key statsKey = lastKey;

                    // if no last key, then use the startkey of the range
                    if (statsKey == null) {
                        statsKey = seekRange.getStartKey();
                        if (!seekRange.isStartKeyInclusive()) {
                            statsKey = statsKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);
                        }
                    }

                    nextEntry = getStatsEntry(statsKey);

                    statsEntryReturned = true;
                }
            } else {
                nextEntry = this.itr.next();
                if (nextEntry != null) {
                    this.lastKey = nextEntry.getKey();
                }
            }
        }
        return nextEntry;
    }

    @Override
    public void remove() {
        this.itr.remove();
    }
}
