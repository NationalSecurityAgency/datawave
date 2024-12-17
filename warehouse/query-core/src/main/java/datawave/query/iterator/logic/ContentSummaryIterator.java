package datawave.query.iterator.logic;

import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.getDtUid;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.getDtUidFromEventKey;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.getSortedCFs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.Constants;
import datawave.query.attributes.SummaryOptions;
import datawave.query.table.parser.ContentKeyValueFactory;

/**
 * This iterator is intended to scan the d column for a specified document. The result will be a summary for each document scanned.
 */
public class ContentSummaryIterator implements SortedKeyValueIterator<Key,Value> {
    private static final Logger log = LoggerFactory.getLogger(ContentSummaryIterator.class);
    private static final Collection<ByteSequence> D_COLUMN_FAMILY_BYTE_SEQUENCE = Collections
                    .singleton(new ArrayByteSequence(Constants.D_COLUMN_FAMILY.getBytes()));

    public static final String SUMMARY_SIZE = "summary.size";

    public static final String VIEW_NAMES = "view.names";

    public static final String ONLY_SPECIFIED = "only.specified";

    private static final int MAX_SUMMARY_SIZE = 1500;

    // 100 megabytes
    private static final int MAX_CONTENT_SIZE = 100 * 1024 * 1024;

    /**
     * A list of view names to potentially create a summary for. The closer to the front in the list, the higher the priority to get a summary for that view
     */
    protected final ArrayList<String> viewSummaryOrder = new ArrayList<>();

    /** the size in bytes of the summary to create */
    protected int summarySize;

    /** if we will only look at the view names specified in the query */
    protected boolean onlySpecified;

    /** the underlying source */
    protected SortedKeyValueIterator<Key,Value> source;

    /** The specified dt/uid column families */
    protected SortedSet<String> columnFamilies;

    /** inclusive or exclusive dt/uid column families */
    protected boolean inclusive;

    /** the underlying D column scan range */
    protected Range scanRange;

    /** the top key */
    protected Key tk;

    /** the top value */
    protected Value tv;

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        ContentSummaryIterator it = new ContentSummaryIterator();
        it.source = source.deepCopy(env);
        return it;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;

        viewSummaryOrder.add("CONTENT");

        if (options.containsKey(SUMMARY_SIZE)) {
            this.summarySize = Math.max(1, Math.min(Integer.parseInt(options.get(SUMMARY_SIZE)), MAX_SUMMARY_SIZE));
        } else {
            this.summarySize = SummaryOptions.DEFAULT_SIZE;
        }

        // if "ONLY" we will clear the view names list so that we only use the ones passed in
        if (options.containsKey(ONLY_SPECIFIED)) {
            onlySpecified = Boolean.parseBoolean(options.get(ONLY_SPECIFIED));
            if (onlySpecified) {
                viewSummaryOrder.clear();
            }
        } else {
            onlySpecified = false;
        }

        // add the view names to the list in the order specified
        if (options.containsKey(VIEW_NAMES)) {
            String[] nameList = SummaryOptions.viewNamesListFromString(options.get(VIEW_NAMES));
            for (int i = nameList.length - 1; i >= 0; i--) {
                String name = nameList[i];
                viewSummaryOrder.remove(name);
                viewSummaryOrder.add(0, name);
            }
        }
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("{} seek'ing with requested range {}", this, range);
        }

        // capture the column families and the inclusiveness
        this.columnFamilies = columnFamilies != null ? getSortedCFs(columnFamilies) : Collections.emptySortedSet();
        this.inclusive = inclusive;

        // Determine the start key in the d keys
        Key startKey = null;
        if (range.getStartKey() != null) {
            // get the start document
            String dtAndUid = getDtUidFromEventKey(range.getStartKey(), true, range.isStartKeyInclusive());
            // if no start document
            if (dtAndUid == null) {
                // if no column families or not using these column families inclusively
                if (this.columnFamilies.isEmpty() || !this.inclusive) {
                    // then start at the beginning of the d range
                    startKey = new Key(range.getStartKey().getRow(), Constants.D_COLUMN_FAMILY);
                } else {
                    // otherwise start at the first document specified
                    startKey = new Key(range.getStartKey().getRow(), Constants.D_COLUMN_FAMILY, new Text(this.columnFamilies.first() + Constants.NULL));
                }
            } else {
                // we had a start document specified in the start key, so start there
                startKey = new Key(range.getStartKey().getRow(), Constants.D_COLUMN_FAMILY, new Text(dtAndUid));
            }
        }
        log.debug("{} calling seek to start key: {}", this, startKey);

        // Determine the end key in the d keys
        Key endKey = null;
        if (range.getEndKey() != null) {
            // get the end document
            String dtAndUid = getDtUidFromEventKey(range.getEndKey(), false, range.isEndKeyInclusive());
            // if no end document
            if (dtAndUid == null) {
                // if we do not have column families specified, or they are not inclusive
                if (this.columnFamilies.isEmpty() || !this.inclusive) {
                    // then go to the end of the d keys
                    endKey = new Key(range.getEndKey().getRow(), Constants.D_COLUMN_FAMILY, new Text(Constants.MAX_UNICODE_STRING));
                } else {
                    // otherwise end at the last document specified
                    endKey = new Key(range.getEndKey().getRow(), Constants.D_COLUMN_FAMILY,
                                    new Text(this.columnFamilies.last() + Constants.NULL + Constants.MAX_UNICODE_STRING));
                }
            } else {
                // we had an end document specified in the end key, so end there
                endKey = new Key(range.getStartKey().getRow(), Constants.D_COLUMN_FAMILY, new Text(dtAndUid));
            }
        }
        log.debug("{} seek'ing to end key: {}", this, endKey);

        // if we have actually exhausted our range, then return with no next key
        if (endKey != null && startKey != null && endKey.compareTo(startKey) <= 0) {
            this.scanRange = null;
            this.tk = null;
            this.tv = null;
            return;
        }

        // set our d keys scan range
        this.scanRange = new Range(startKey, false, endKey, false);

        if (log.isDebugEnabled()) {
            log.debug("{} seek'ing to: {} from requested range {}", this, this.scanRange, range);
        }

        // seek the underlying source
        source.seek(this.scanRange, D_COLUMN_FAMILY_BYTE_SEQUENCE, true);

        // get the next key
        next();
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;

        if (log.isTraceEnabled()) {
            log.trace("{} calling next on {}", source.hasTop(), scanRange);
        }

        // find a valid dt/uid (depends on initial column families set in seek call)
        String dtUid = null;
        while (source.hasTop() && dtUid == null) {
            Key top = source.getTopKey();
            String thisDtUid = getDtUidFromDKey(top);
            // if this dt and uid are in the accepted column families...
            if (columnFamilies.contains(thisDtUid) == inclusive) {
                // we can use this document
                dtUid = thisDtUid;
            } else {
                seekToNextUid(top.getRow(), thisDtUid);
            }
        }

        // if no more d keys, then we are done.
        if (!source.hasTop() || dtUid == null) {
            return;
        }

        Key top = source.getTopKey();

        // this is where we will save all the content found for this document
        final Map<String,byte[]> foundContent = new HashMap<>();

        // while we have d keys for the same document
        while (source.hasTop() && dtUid.equals(getDtUidFromDKey(source.getTopKey()))) {
            top = source.getTopKey();

            // get the view name
            String currentViewName = getViewName(top);

            for (String name : viewSummaryOrder) {
                if (name.endsWith("*")) {
                    name = name.substring(0, name.length() - 1);
                    if (currentViewName.startsWith(name)) {
                        addContentToFound(foundContent, currentViewName, source);
                    }
                } else {
                    if (currentViewName.equalsIgnoreCase(name)) {
                        addContentToFound(foundContent, currentViewName, source);
                    }
                }
            }

            // get the next d key
            source.next();
        }

        // create the summary
        String summary = new SummaryCreator(viewSummaryOrder, foundContent, summarySize).createSummary();
        if (summary != null) {
            tk = new Key(top.getRow(), new Text(dtUid), new Text(summary), top.getColumnVisibility());
            tv = new Value();
            return;
        }

        // If we get here, we have not found content to summarize, so return null
        tk = null;
        tv = null;
    }

    private static void addContentToFound(Map<String,byte[]> foundContent, String currentViewName, SortedKeyValueIterator<Key,Value> source) {
        // true for compressed, false for uncompressed
        byte[] content = source.getTopValue().get();
        if (content.length < MAX_CONTENT_SIZE) {
            foundContent.put(currentViewName + Constants.COLON + Boolean.TRUE, content);
        } else {
            content = ContentKeyValueFactory.decodeAndDecompressContent(content);
            // pre-truncate big content to MAX_SUMMARY_SIZE
            content = new String(content).substring(0, MAX_SUMMARY_SIZE).getBytes();
            foundContent.put(currentViewName + Constants.COLON + Boolean.FALSE, content);
        }
    }

    /**
     * Seek to the dt/uid following the one passed in
     *
     * @param row
     *            a row
     * @param dtAndUid
     *            the dt and uid string
     * @throws IOException
     *             for issues with read/write
     */
    private void seekToNextUid(Text row, String dtAndUid) throws IOException {
        Key startKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtAndUid + Constants.ONE_BYTE));
        this.scanRange = new Range(startKey, false, this.scanRange.getEndKey(), this.scanRange.isEndKeyInclusive());
        if (log.isDebugEnabled()) {
            log.debug("{} seek'ing to next document: {}", this, this.scanRange);
        }

        source.seek(this.scanRange, Collections.singleton(new ArrayByteSequence(Constants.D_COLUMN_FAMILY.getBytes())), true);
    }

    /**
     * Get the view name from the end of the column qualifier of the d key
     *
     * @param dKey
     *            the d key
     * @return the view name
     */
    private static String getViewName(Key dKey) {
        String cq = dKey.getColumnQualifier().toString();
        int index = cq.lastIndexOf(Constants.NULL);
        return cq.substring(index + 1);
    }

    /**
     * get the dt and uid from a d key
     *
     * @param dKey
     *            the d key
     * @return the dt\x00uid
     */
    private static String getDtUidFromDKey(Key dKey) {
        return getDtUid(dKey.getColumnQualifier().toString());
    }

    public void setViewNameList(List<String> viewNameList) {
        viewSummaryOrder.clear();
        viewSummaryOrder.addAll(viewNameList);
    }

    @Override
    public String toString() {
        return "DColumnExcerptIterator: " + summarySize + ", " + onlySpecified + ", " + viewSummaryOrder;
    }

}
