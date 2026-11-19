package datawave.query.iterator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.iterator.waitwindow.WaitWindowObserver;

class ScanProgressTest {
    private final WaitWindowObserver observer = new WaitWindowObserver();
    private final Key candidate = new Key("20260101_0", "type\0uid");

    @Test
    void progressRequiresOptInAndAResumePositionPastTheRejectedCandidate() {
        observer.setSeekRange(new Range());
        Key retry = observer.createYieldKey(candidate, true, "retry").getLeft();
        Key advance = observer.createYieldKey(candidate, false, "advance").getLeft();

        observer.recordRejectedCandidate(candidate);
        assertFalse(observer.hasReportableProgress(advance));
        observer.setReportScanProgress(true);
        assertFalse(observer.hasReportableProgress(advance));

        observer.recordRejectedCandidate(candidate);
        assertFalse(observer.hasReportableProgress(retry));
        assertTrue(WaitWindowObserver.isScanProgress(observer.overrunDocument(advance)));

        observer.setSeekRange(new Range());
        assertFalse(observer.hasReportableProgress(advance));
    }

    @Test
    void progressEntryCanPrecedeYieldWithoutTimingCollection() {
        observer.setSeekRange(new Range());
        observer.setReportScanProgress(true);
        observer.recordRejectedCandidate(candidate);
        YieldCallback<Key> callback = new YieldCallback<>();
        observer.setYieldCallback(callback);
        Pair<Key,String> position = observer.createYieldKey(candidate, false, "advance");
        var document = observer.overrunDocument(position.getLeft());
        var wireEntry = new KryoDocumentSerializer().apply(new AbstractMap.SimpleImmutableEntry<>(position.getLeft(), document));
        assertTrue(WaitWindowObserver.isScanProgress(new KryoDocumentDeserializer().apply(wireEntry).getValue()));

        observer.setYieldKey(position);
        observer.yieldOnWaitWindowOverrun();
        observer.yieldOnWaitWindowOverrun();
        assertFalse(callback.hasYielded());
        observer.yieldAfterProgressEntry();
        assertTrue(callback.hasYielded());
        assertTrue(callback.getPositionAndReset().compareTo(position.getLeft()) > 0);
    }
}
