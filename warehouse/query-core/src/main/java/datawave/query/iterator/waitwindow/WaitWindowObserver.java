package datawave.query.iterator.waitwindow;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.attributes.WaitWindowExceededMetadata;
import datawave.query.exceptions.WaitWindowOverrunException;
import datawave.query.iterator.ResultCountingIterator;
import datawave.query.iterator.profile.QuerySpan;

/*
 * This class maintains common state and logic to determine if the QueryIterator's stack of boolean logic
 * should yield to ensure that resources are shared appropriately, to return collected metrics if configured,
 * and to ensure that there is still a client waiting for a response.
 */
public class WaitWindowObserver {

    private static final Logger log = Logger.getLogger(WaitWindowObserver.class);
    public static final String WAIT_WINDOW_OVERRUN = "WAIT_WINDOW_OVERRUN";
    public static final String YIELD_AT_BEGIN_STR = "!YIELD_AT_BEGIN";
    public static final Text YIELD_AT_BEGIN = new Text(YIELD_AT_BEGIN_STR);
    public static final String YIELD_AT_END_STR = "\uffffYIELD_AT_END";
    public static final Text YIELD_AT_END = new Text(YIELD_AT_END_STR);
    private static Timer timer = null;
    private boolean readyToYield = false;

    public static final Comparator<Comparable> keyComparator = (o1, o2) -> {
        if (o1 instanceof Key) {
            return ((Key) o1).compareTo((Key) o2, PartialKey.ROW_COLFAM_COLQUAL);
        }
        return o1.compareTo(o2);
    };

    public static final Comparator<Comparable> keyComparatorReversed = (o1, o2) -> {
        if (o1 instanceof Key) {
            return ((Key) o2).compareTo((Key) o1, PartialKey.ROW_COLFAM_COLQUAL);
        }
        return o2.compareTo(o1);
    };

    protected YieldCallback yieldCallback = null;
    // When the wait window is over. Set during the initial seek
    protected long endOfWaitWindow;
    // Remaining time in the wait window. Updated by the timerTask
    protected AtomicLong remainingTimeMs = new AtomicLong();
    protected TimerTask timerTask = new WaitWindowTimerTask();
    // How often the timerTask gets run
    protected long checkPeriod = 50;
    // Seek range of the QueryIterator. Used to ensure that yieldKey is in the range.
    protected Range seekRange = null;
    // When collectTimingDetails==true, we set the yieldKey, return a WAIT_WINDOW_OVERRUN
    // document (that will also have the timingDetails), and then yield to this key on
    // a subsequent call to QueryIterator.hasTop
    protected Key yieldKey = null;

    // Used for the unsortedUID case to prepend the colFam with an encoded count
    protected ResultCountingIterator resultCountingIterator = null;
    protected boolean sortedUIDs = true;

    // Used to record the yield that happens
    protected QuerySpan trackingSpan = null;

    public WaitWindowObserver() {

    }

    /*
     * Using the WaitWindowTask in a Timer will limit the number of times that System.currentTimeMillis() is called while still decrementing remainingTimeMs as
     * appropriate to enable yields.
     */
    private class WaitWindowTimerTask extends TimerTask {
        @Override
        public void run() {
            long remaining = WaitWindowObserver.this.endOfWaitWindow - System.currentTimeMillis();
            WaitWindowObserver.this.remainingTimeMs.set(remaining);
            // self-cancel this task when the wait window is exhausted
            if (remaining <= 0) {
                this.cancel();
            }
        }
    }

    /*
     * Ensure that we are only creating one Timer object per JVM (tablet server) for scheduling WaitWindowTimerTasks. Use the double null check to limit
     * synchronization and prevent a race condition that overwrites WaitWindowObserver.timer.
     */
    private static Timer getTimer() {
        if (WaitWindowObserver.timer == null) {
            synchronized (WaitWindowObserver.class) {
                if (WaitWindowObserver.timer == null) {
                    WaitWindowObserver.timer = new Timer();
                }
            }
        }
        return WaitWindowObserver.timer;
    }

    /*
     * Set seekRange, remainingTimeMs, endOfWaitWindow and start the Timer
     */
    public void start(Range seekRange, long yieldThresholdMs) {
        this.seekRange = seekRange;
        this.remainingTimeMs.set(yieldThresholdMs);
        this.endOfWaitWindow = yieldThresholdMs + System.currentTimeMillis();
        WaitWindowObserver.getTimer().schedule(this.timerTask, this.checkPeriod, this.checkPeriod);
    }

    /*
     * Ensure that the WaitWindowTimerTask is cancelled. Called from QueryIterator.hasTop.
     */
    public void stop() {
        this.timerTask.cancel();
    }

    /*
     * Called from waitWindowOverrun() and from places that use a timeout for polling or retrieving Future results remainingTimeMs get updated periodically in
     * another thread
     */
    public long remainingTimeMs() {
        if (this.yieldCallback == null) {
            return Long.MAX_VALUE;
        } else {
            return this.remainingTimeMs.get();
        }
    }

    /*
     * remainingTimeMs get updated periodically in another thread
     */
    public boolean waitWindowOverrun() {
        return remainingTimeMs.get() <= 0;
    }

    /*
     * If we have exceeded the wait window, then immediately throw a WaitWindowOverrunException containing a yield key corresponding to the provided key.
     */
    public void checkWaitWindow(Key currentKey, boolean yieldToBeginning) {
        if (this.yieldCallback != null && waitWindowOverrun()) {
            Key currentYieldKey = createYieldKey(currentKey, yieldToBeginning);
            if (this.yieldKey == null) {
                throw new WaitWindowOverrunException(currentYieldKey);
            } else {
                throw new WaitWindowOverrunException(lowestYieldKey(Arrays.asList(this.yieldKey, currentYieldKey)));
            }
        }
    }

    /*
     * There can be many embedded AndIterators, OrIterators, and Ivarators where a WaitWindowOverrunException can be thrown from. As the exception makes its way
     * to the top of the call chain, we need to evaluate the yieldKey at each level.
     */
    public void propagateException(Key key, boolean yieldToBeginning, boolean keepLowest, WaitWindowOverrunException e) {
        Key yieldKey;
        if (key == null) {
            yieldKey = e.getYieldKey();
        } else {
            Collection<Key> keys = Arrays.asList(e.getYieldKey(), createYieldKey(key, yieldToBeginning));
            if (keepLowest) {
                yieldKey = lowestYieldKey(keys);
            } else {
                yieldKey = highestYieldKey(keys);
            }
        }
        throw new WaitWindowOverrunException(yieldKey);
    }

    /*
     * When yieldKey is set and collectTimingDetails=true, then we yield on the second call from QueryIterator.hasTop so that the first call can return the
     * document that contains the WAIT_WINDOW_OVERRUN and TIMING_METADATA attributes. readyToYield will be set to true after the first call to yieldOnOverrun
     */
    public void yieldOnOverrun() {
        if (this.yieldCallback != null && this.yieldKey != null && !this.yieldCallback.hasYielded()) {
            if (readyToYield) {
                if (this.resultCountingIterator != null) {
                    this.yieldKey = this.resultCountingIterator.addKeyCount(this.yieldKey);
                }
                yieldCallback.yield(this.yieldKey);
                if (log.isDebugEnabled()) {
                    log.debug("Yielding at " + this.yieldKey);
                }
            } else {
                readyToYield = true;
            }
        }
    }

    /*
     * Create a yield key with YIELD_AT_BEGIN or YIELD_AT_END marker
     */
    public Key createYieldKey(Key yieldKey, boolean yieldToBeginning) {
        if (isShardKey(yieldKey)) {
            return createShardYieldKey(yieldKey, yieldToBeginning);
        } else {
            return createDocumentYieldKey(yieldKey, yieldToBeginning);
        }
    }

    /*
     * Create a key that sorts either before or after all field keys for this document key. A colQual starting with ! sorts before all keys whose colFam starts
     * with an alphanumeric character. A colQual starting with \uffff sorts after all keys whose colFam starts with an alphanumeric character. We are adding
     * sort-irrelevant marker text after that symbol to easily identify the key
     *
     * YYYYMMDD_NN !YIELD_AT_BEGIN YYYYMMDD_NN \uffffYIELD_AT_END
     *
     * ensureYieldKeyAfterRangeStart may add a \x00 after the colFam if the produced key matches the startKey of a non-inclusive seekRange
     */
    public Key createShardYieldKey(Key key, boolean yieldToBeginning) {
        // if key already contains YIELD_AT_END then we must yield to the end
        Text marker = yieldToBeginning && !hasEndMarker(key) ? YIELD_AT_BEGIN : YIELD_AT_END;
        return ensureYieldKeyAfterRangeStart(new Key(key.getRow(), marker));
    }

    /*
     * Create a key that sorts either before or after all field keys for this document key. A colQual starting with ! sorts before all keys with the same
     * row/colFam and an alphanumeric colQual A colQual starting with \uffff sorts after all keys with the same row/colFam and an alphanumeric colQual Also
     * adding a sort-irrelevant marker text after that symbol to easily identify the key
     *
     * sortedUIDs YYYYMMDD_NN datatype\x00uid:!YIELD_AT_BEGIN YYYYMMDD_NN datatype\x00uid:\uffffYIELD_AT_END
     *
     * !sortedUIDs YYYYMMDD_NN datatype\x00uid:!YIELD_AT_BEGIN\x00field\x00value YYYYMMDD_NN datatype\x00uid:field\x00value\uffffYIELD_AT_END
     *
     * ensureYieldKeyAfterRangeStart may add a \x00 after the colQual if the produced key matches the startKey of a non-inclusive seekRange
     */
    public Key createDocumentYieldKey(Key key, boolean yieldToBeginning) {
        // if key already contains YIELD_AT_END then we must yield to the end
        Text marker = yieldToBeginning && !hasEndMarker(key) ? YIELD_AT_BEGIN : YIELD_AT_END;
        Key newKey;
        if (sortedUIDs) {
            Key documentKey = new DocumentKey(key, true).getDocKey();
            newKey = new Key(documentKey.getRow(), documentKey.getColumnFamily(), marker);
        } else {
            Text colQual;
            if (hasMarker(key.getColumnQualifier())) {
                colQual = key.getColumnQualifier();
            } else {
                String origColQual = key.getColumnQualifier().toString();
                if (origColQual.isEmpty()) {
                    colQual = marker;
                } else if (yieldToBeginning) {
                    colQual = new Text(marker + "\0" + origColQual);
                } else {
                    colQual = new Text(origColQual + marker);
                }
            }
            newKey = new Key(key.getRow(), key.getColumnFamily(), colQual);
        }
        return ensureYieldKeyAfterRangeStart(newKey);
    }

    /*
     * When the current seekRange is non-inclusive, we can not return the startKey of the range as a yield key. Instead, we have to return the following key.
     */
    private Key ensureYieldKeyAfterRangeStart(Key key) {
        if (!this.seekRange.isStartKeyInclusive()) {
            Key seekStartKey = this.seekRange.getStartKey();
            boolean isShardKey = isShardKey(seekStartKey);
            if (isShardKey && key.compareTo(seekStartKey, PartialKey.ROW_COLFAM) <= 0) {
                // shard range
                return seekStartKey.followingKey(PartialKey.ROW_COLFAM);
            }
            if (!isShardKey && key.compareTo(seekStartKey, PartialKey.ROW_COLFAM_COLQUAL) <= 0) {
                // document range
                return seekStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
            }
        }
        return key;
    }

    /*
     * When yieldKey is set and collectTimingDetails=true, then we yield on the second call from QueryIterator.hasTop so that the first call can return the
     * document that contains the WAIT_WINDOW_OVERRUN and TIMING_METADATA attributes. Since we can not yield on the same key that was just returned, we have to
     * yield on a key that follows the yieldKey. For YIELD_AT_END, we add a null char after YIELD_AT_END which is at the end of the colFam or colQual. For
     * YIELD_AT_BEGIN, we add a null character after that marker.
     */
    private Key yieldKeyAfterOverrun(Key key) {
        Text row = key.getRow();
        Text colFam = key.getColumnFamily();
        Text colQual = key.getColumnQualifier();
        if (hasBeginMarker(key)) {
            if (hasBeginMarker(colFam)) {
                colFam = new Text(colFam.toString().replace(YIELD_AT_BEGIN_STR, YIELD_AT_BEGIN_STR + "\0"));
            } else {
                colQual = new Text(colQual.toString().replace(YIELD_AT_BEGIN_STR, YIELD_AT_BEGIN_STR + "\0"));
            }
        } else if (hasEndMarker(key)) {
            if (hasEndMarker(colFam)) {
                colFam = new Text(colFam.toString().replace(YIELD_AT_END_STR, YIELD_AT_END_STR + "\0"));
            } else {
                colQual = new Text(colQual.toString().replace(YIELD_AT_END_STR, YIELD_AT_END_STR + "\0"));
            }
        }
        return new Key(row, colFam, colQual);
    }

    public void setYieldKey(Key yieldKey) {
        // only do this once
        if (this.trackingSpan != null && this.yieldKey == null) {
            this.trackingSpan.yield();
        }
        // we can't return a key and then yield at the same key
        this.yieldKey = yieldKeyAfterOverrun(yieldKey);
    }

    /*
     * When yieldKey is set and collectTimingDetails=true, then we yield on the second call from QueryIterator.hasTop so that the first call can return the
     * document that contains the WAIT_WINDOW_OVERRUN and TIMING_METADATA attributes. readyToYield will be set to true after the first call to yieldOnOverrun
     */
    public boolean isReadyToYield() {
        return readyToYield && yieldKey != null;
    }

    public Key getYieldKey() {
        return yieldKey;
    }

    /*
     * YIELD_AT_BEGIN and YIELD_AT_END markers for Document keys are in the colQual, so if the colFam is empty or contains one of these markers, then it is a
     * shard key
     */
    static public boolean isShardKey(Key key) {
        Text colFam = key.getColumnFamily();
        return colFam.equals(new Text()) || hasBeginMarker(colFam);
    }

    /*
     * Check if YIELD_AT_BEGIN or YIELD_AT_END is in either the colFam or colQual
     */
    static public boolean hasMarker(Key key) {
        return hasBeginMarker(key) || hasEndMarker(key);
    }

    /*
     * Check if YIELD_AT_BEGIN or YIELD_AT_END is in this Text
     */
    static public boolean hasMarker(Text text) {
        return hasBeginMarker(text) || hasEndMarker(text);
    }

    /*
     * Check if YIELD_AT_BEGIN is in either the colFam or colQual
     */
    static public boolean hasBeginMarker(Key key) {
        return hasBeginMarker(key.getColumnFamily()) || hasBeginMarker(key.getColumnQualifier());
    }

    /*
     * YIELD_AT_BEGIN will always be at the beginning of the Text
     */
    static public boolean hasBeginMarker(Text text) {
        return text.toString().startsWith(YIELD_AT_BEGIN_STR);
    }

    /*
     * Check if YIELD_AT_END is in either the colFam or colQual
     */
    static public boolean hasEndMarker(Key key) {
        return hasEndMarker(key.getColumnFamily()) || hasEndMarker(key.getColumnQualifier());
    }

    /*
     * Check if YIELD_AT_END is contained in the Text. There are cases where one or more null characters get added to the end of the colFam or colQual, so we
     * can not check for endsWith
     */
    static public boolean hasEndMarker(Text text) {
        return text.toString().contains(YIELD_AT_END_STR);
    }

    static public Text removeMarkers(Text text) {
        String str = text.toString();
        if (hasBeginMarker(text)) {
            int yieldAtBeginStrLength = YIELD_AT_BEGIN_STR.length();
            if (str.length() > yieldAtBeginStrLength) {
                str = str.substring(yieldAtBeginStrLength + 1);
                // strip null characters after YIELD_AT_BEGIN (now at start of str)
                str = StringUtils.stripStart(str, "\0");
            } else {
                str = "";
            }
            return new Text(str);
        } else if (hasEndMarker(text)) {
            int marker = str.indexOf(WaitWindowObserver.YIELD_AT_END_STR);
            if (marker > 0) {
                str = str.substring(0, marker);
            } else {
                str = "";
            }
            return new Text(str);
        } else {
            return text;
        }
    }

    /*
     * Convenience method to produce a document containing a WAIT_WINDOW_OVERRUN attribute, This document gets returned before a yield when
     * collectTimingDetails=true so that the FinalDocumenTrackingIterator can add timing details and metrics befoer returning the Document.
     */
    static public Document getWaitWindowOverrunDocument() {
        Document document = new Document();
        document.put(WAIT_WINDOW_OVERRUN, new WaitWindowExceededMetadata());
        return document;
    }

    /*
     * Return a yieldKey for the lowest key in a collection while handling the special case of !sortedUIDs
     */
    public Key lowestYieldKey(Collection<Key> keys) {
        Collection<Key> keySet = new HashSet<>(keys);
        Text lowestRow = keySet.stream().sorted(keyComparator).findFirst().get().getRow();
        List<Key> keysInRowSortedIncreasing = keySet.stream().filter(k -> k.getRow().equals(lowestRow)).sorted(keyComparator).collect(Collectors.toList());
        Key lowestKey = keysInRowSortedIncreasing.stream().findFirst().get();
        // if !sortedUIDs, then the yieldKeys could have field names and values in the colQual. If the lowestKey has
        // a non-empty colQual (after removing any marker) then the yieldKey will have to be constructed from the lowest
        // colFam AND the lowest colQual even if those are not from the same original key.
        if (keysInRowSortedIncreasing.size() > 1 && !sortedUIDs) {
            Optional<String> lowestColQual = keysInRowSortedIncreasing.stream().map(k -> WaitWindowObserver.removeMarkers(k.getColumnQualifier()).toString())
                            .filter(Predicate.not(String::isEmpty)).sorted().findFirst();
            if (removeMarkers(lowestKey.getColumnQualifier()).getLength() > 0 && lowestColQual.isPresent()) {
                Key lowestKeyWithLowestColQual = keysInRowSortedIncreasing.stream()
                                .filter(k -> WaitWindowObserver.removeMarkers(k.getColumnQualifier()).toString().equals(lowestColQual.get())).findFirst().get();
                int compare = lowestKeyWithLowestColQual.compareTo(lowestKey, PartialKey.ROW_COLFAM_COLQUAL);
                if (compare <= 0) {
                    lowestKey = lowestKeyWithLowestColQual;
                } else {
                    Text colQual = lowestKeyWithLowestColQual.getColumnQualifier();
                    lowestKey = new Key(lowestKey.getRow(), lowestKey.getColumnFamily(), colQual);
                }
            }
        }
        // default to YIELD_AT_BEGIN unless the key already has a YIELD_AT_END marker
        return createYieldKey(lowestKey, !hasEndMarker(lowestKey));
    }

    /*
     * Return a yieldKey for the lowest key in a collection while handling the special case of !sortedUIDs
     */
    public Key highestYieldKey(Collection<Key> keys) {
        Collection<Key> keySet = new HashSet<>(keys);
        Text higestRow = keySet.stream().sorted(keyComparatorReversed).findFirst().get().getRow();
        List<Key> keysInRowSortedDecreasing = keySet.stream().filter(k -> k.getRow().equals(higestRow)).sorted(keyComparatorReversed)
                        .collect(Collectors.toList());
        Key highestKey = keysInRowSortedDecreasing.stream().findFirst().get();
        // if !sortedUIDs, then the yieldKeys could have field names and values in the colQual. If the highestKey has
        // a non-empty colQual (after removing any marker) then the yieldKey will have to be constructed from the
        // highest colFam AND the highest colQual even if those are not from the same original key.
        if (keysInRowSortedDecreasing.size() > 1 && !sortedUIDs) {
            Optional<String> highestColQual = keysInRowSortedDecreasing.stream().map(k -> WaitWindowObserver.removeMarkers(k.getColumnQualifier()).toString())
                            .filter(Predicate.not(String::isEmpty)).sorted(Comparator.reverseOrder()).findFirst();
            if (highestColQual.isPresent()) {
                Optional<Key> highestKeyWithHighestColQualOpt = keysInRowSortedDecreasing.stream()
                                .filter(k -> WaitWindowObserver.removeMarkers(k.getColumnQualifier()).toString().equals(highestColQual.get())).findFirst();
                if (removeMarkers(highestKey.getColumnQualifier()).getLength() > 0 && highestKeyWithHighestColQualOpt.isPresent()) {
                    Key highestKeyWithHighestColQual = highestKeyWithHighestColQualOpt.get();
                    int compare = highestKeyWithHighestColQual.compareTo(highestKey, PartialKey.ROW_COLFAM_COLQUAL);
                    if (compare <= 0) {
                        highestKey = highestKeyWithHighestColQual;
                    } else {
                        Text colQual = highestKeyWithHighestColQual.getColumnQualifier();
                        highestKey = new Key(highestKey.getRow(), highestKey.getColumnFamily(), colQual);
                    }
                }
            }
        }
        // default to YIELD_AT_BEGIN unless the key already has a YIELD_AT_END marker
        return createYieldKey(highestKey, !hasEndMarker(highestKey));
    }

    public void setResultCountingIterator(ResultCountingIterator resultCountingIterator) {
        this.resultCountingIterator = resultCountingIterator;
    }

    public void setYieldCallback(YieldCallback yieldCallback) {
        this.yieldCallback = yieldCallback;
    }

    public void setSortedUIDs(boolean sortedUIDs) {
        this.sortedUIDs = sortedUIDs;
    }

    public boolean getSortedUIDs() {
        return sortedUIDs;
    }

    public void setTrackingSpan(QuerySpan trackingSpan) {
        this.trackingSpan = trackingSpan;
    }
}
