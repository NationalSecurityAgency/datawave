package datawave.query.iterator.logic;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.TermWeight;
import datawave.query.Constants;

/**
 * This iterator is intended to scan the term frequencies for a specified document, field, and offset range. The result will be excerpts for the field specified
 * for each document scanned.
 */
public class TermFrequencyExcerptIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    private static final Logger log = Logger.getLogger(TermFrequencyExcerptIterator.class);
    private static final Joiner joiner = Joiner.on(" ").skipNulls();

    // The field name option
    public static final String FIELD_NAME = "field.name";
    // The start offset option
    public static final String START_OFFSET = "start.offset";
    // The end offset option
    public static final String END_OFFSET = "end.offset";
    // The hit term callout option
    public static final String HIT_CALLOUT = "hit.callout";
    // The hit term value(s) to be called out
    public static final String HIT_CALLOUT_TERMS = "hit.callout.terms";

    // the underlying source
    protected SortedKeyValueIterator<Key,Value> source;

    // the field name
    protected String fieldName;
    // the start offset (inclusive)
    protected int startOffset;
    // the end offset (exclusive)
    protected int endOffset;

    // The specified dt/uid column families
    protected SortedSet<String> columnFamilies;
    // inclusive or exclusive dt/uid column families
    protected boolean inclusive;

    // the underlying TF scan range
    protected Range scanRange;

    // the top key
    protected Key tk;
    // the top value
    protected Value tv;

    private boolean hitCallout;

    // the hit term(s) to be called out
    private String hitTerms;

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = new IteratorOptions(TermFrequencyExcerptIterator.class.getSimpleName(),
                        "An iterator that returns excepts from the scanned documents", null, null);
        options.addNamedOption(FIELD_NAME, "The token field name for which to get excerpts (required)");
        options.addNamedOption(START_OFFSET, "The start offset for the excerpt (inclusive) (required)");
        options.addNamedOption(END_OFFSET, "The end offset for the excerpt (exclusive) (required)");
        options.addNamedOption(HIT_CALLOUT, "The option for whether or not to call out hit terms (required)");
        options.addNamedOption(HIT_CALLOUT_TERMS, "The hit term(s) to be called out (not required)");
        return options;
    }

    @Override
    public boolean validateOptions(Map<String,String> map) {
        if (map.containsKey(FIELD_NAME)) {
            if (map.get(FIELD_NAME).isEmpty()) {
                throw new IllegalArgumentException("Empty field name property: " + FIELD_NAME);
            }
        } else {
            throw new IllegalArgumentException("Missing field name property: " + FIELD_NAME);
        }

        int startOffset;
        if (map.containsKey(START_OFFSET)) {
            try {
                startOffset = Integer.parseInt(map.get(START_OFFSET));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse start offset as integer", e);
            }
        } else {
            throw new IllegalArgumentException("Missing start offset property: " + START_OFFSET);
        }

        int endOffset;
        if (map.containsKey(END_OFFSET)) {
            try {
                endOffset = Integer.parseInt(map.get(END_OFFSET));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse end offset as integer", e);
            }
            if (endOffset <= startOffset) {
                throw new IllegalArgumentException("End offset must be greater than start offset");
            }
        } else {
            throw new IllegalArgumentException("Missing end offset property: " + END_OFFSET);
        }

        if (map.containsKey(HIT_CALLOUT)) {
            if (map.get(HIT_CALLOUT).isEmpty()) {
                throw new IllegalArgumentException("Empty hit callout property: " + HIT_CALLOUT);
            }
        } else {
            throw new IllegalArgumentException("Missing hit callout property: " + HIT_CALLOUT);
        }

        return true;
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        TermFrequencyExcerptIterator it = new TermFrequencyExcerptIterator();
        it.startOffset = startOffset;
        it.endOffset = endOffset;
        it.fieldName = fieldName;
        it.hitCallout = hitCallout;
        it.hitTerms = hitTerms;
        it.source = source.deepCopy(env);
        return it;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.startOffset = Integer.parseInt(options.get(START_OFFSET));
        this.endOffset = Integer.parseInt(options.get(END_OFFSET));
        this.fieldName = options.get(FIELD_NAME);
        this.hitCallout = Boolean.parseBoolean(options.get(HIT_CALLOUT));
        this.hitTerms = options.get(HIT_CALLOUT_TERMS);
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
            log.debug(this + " seek'ing with requested range " + range);
        }

        // capture the column families and the inclusiveness
        if (columnFamilies != null) {
            this.columnFamilies = getSortedCFs(columnFamilies);
        } else {
            this.columnFamilies = Collections.emptySortedSet();
        }
        this.inclusive = inclusive;

        // Determine the start key in the term frequencies
        Key startKey = null;
        if (range.getStartKey() != null) {
            // get the start document
            String dtAndUid = getDtUidFromEventKey(range.getStartKey(), true, range.isStartKeyInclusive());
            // if no start document
            if (dtAndUid == null) {
                // if no column families or not using these column families inclusively
                if (this.columnFamilies.isEmpty() || !this.inclusive) {
                    // then start at the beginning of the tf range
                    startKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY);
                } else {
                    // otherwise start at the first document specified
                    startKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY,
                                    new Text(this.columnFamilies.first() + Constants.NULL));
                }
            } else {
                // we had a start document specified in the start key, so start there
                startKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(dtAndUid));
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(this + " seek'ing to start key: " + startKey);
        }

        // Determine the end key in the term frequencies
        Key endKey = null;
        if (range.getEndKey() != null) {
            // get the end document
            String dtAndUid = getDtUidFromEventKey(range.getEndKey(), false, range.isEndKeyInclusive());
            // if no end document
            if (dtAndUid == null) {
                // if we do not have column families specified or they are not inclusive
                if (this.columnFamilies.isEmpty() || !this.inclusive) {
                    // then go to the end of the TFs
                    endKey = new Key(range.getEndKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(Constants.MAX_UNICODE_STRING));
                } else {
                    // othersize end at the last document specified
                    endKey = new Key(range.getEndKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY,
                                    new Text(this.columnFamilies.last() + Constants.NULL + Constants.MAX_UNICODE_STRING));
                }
            } else {
                // we had an end document specified in the end key, so end there
                endKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(dtAndUid));
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(this + " seek'ing to end key: " + endKey);
        }

        // if we have actually exhausted our range, then return with no next key
        if (endKey != null && startKey != null && endKey.compareTo(startKey) <= 0) {
            this.scanRange = null;
            this.tk = null;
            this.tv = null;
            return;
        }

        // set our term frequency scan range
        this.scanRange = new Range(startKey, false, endKey, false);

        if (log.isDebugEnabled()) {
            log.debug(this + " seek'ing to: " + this.scanRange + " from requested range " + range);
        }

        // seek the underlying source
        source.seek(this.scanRange, Collections.singleton(new ArrayByteSequence(Constants.TERM_FREQUENCY_COLUMN_FAMILY.getBytes())), true);

        // get the next key
        next();
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;

        if (log.isTraceEnabled()) {
            log.trace(source.hasTop() + " nexting on " + scanRange);
        }

        // find a valid dt/uid (depends on initial column families set in seek call)
        String dtUid = null;
        while (source.hasTop() && dtUid == null) {
            Key top = source.getTopKey();
            String thisDtUid = getDtUidFromTfKey(top);
            if (isUsableDocument(thisDtUid)) {
                dtUid = thisDtUid;
            } else {
                seekToNextUid(top.getRow(), thisDtUid);
            }
        }

        // if no more term frequencies, then we are done.
        if (!source.hasTop()) {
            return;
        }

        // get the pieces from the top key that will be returned
        Key top = source.getTopKey();
        Text cv = top.getColumnVisibility();
        long ts = top.getTimestamp();
        Text row = top.getRow();
        List<String>[] terms = new List[endOffset - startOffset];

        // while we have term frequencies for the same document
        while (source.hasTop() && dtUid.equals(getDtUidFromTfKey(source.getTopKey()))) {
            top = source.getTopKey();

            // get the field and value
            String[] fieldAndValue = getFieldAndValue(top);

            // if this is for the field we are summarizing
            if (fieldName.equals(fieldAndValue[0])) {
                try {
                    // parse the offsets from the value
                    TermWeight.Info info = TermWeight.Info.parseFrom(source.getTopValue().get());

                    // for each offset, gather all the terms in our range
                    for (int i = 0; i < info.getTermOffsetCount(); i++) {
                        int offset = info.getTermOffset(i);
                        // if the offset is within our range
                        if (offset >= startOffset && offset < endOffset) {
                            // calculate the index in our value list
                            int index = offset - startOffset;
                            // if the value is larger than the value for this offset thus far
                            if (terms[index] == null) {
                                terms[index] = new ArrayList<>();
                            }
                            // use this value
                            terms[index].add(fieldAndValue[1]);
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error("Value found in tf column was not of type TermWeight.Info, skipping", e);
                }
            }

            // get the next term frequency
            source.next();
        }

        // generate the return key and value
        tk = new Key(row, new Text(dtUid), new Text(fieldName + Constants.NULL + generatePhrase(terms)), cv, ts);
        tv = new Value();
    }

    /**
     * Generate a phrase from the given list of terms
     *
     * @param terms
     *            the terms to create a phrase from
     * @return the phrase
     */
    protected String generatePhrase(List<String>[] terms) {
        String[] largestTerms = new String[terms.length];
        for (int i = 0; i < terms.length; i++) {
            largestTerms[i] = getLongestTerm(terms[i], hitCallout, hitTerms);
        }

        return joiner.join(largestTerms);
    }

    /**
     * Get the longest term from a list of terms;
     *
     * @param terms
     *            the terms to create a phrase
     * @return the longest term (null if empty or null list)
     */
    protected String getLongestTerm(List<String> terms, boolean hitCallout, String hitTerms) {
        if (terms == null || terms.isEmpty()) {
            return null;
        } else {
            String term = terms.stream().max(Comparator.comparingInt(String::length)).get();

            return hitCallout && term.equals(hitTerms) ? "[" + term + "]" : term;
        }
    }

    /**
     * Determine if this dt and uid are in the accepted column families
     *
     * @param dtAndUid
     *            the dt and uid string
     * @return true if we can use it, false if not
     */
    private boolean isUsableDocument(String dtAndUid) {
        return columnFamilies.contains(dtAndUid) == inclusive;
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
        Key startKey = new Key(row, Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(dtAndUid + '.'));
        this.scanRange = new Range(startKey, false, this.scanRange.getEndKey(), this.scanRange.isEndKeyInclusive());
        if (log.isDebugEnabled()) {
            log.debug(this + " seek'ing to next document: " + this.scanRange);
        }

        source.seek(this.scanRange, Collections.singleton(new ArrayByteSequence(Constants.TERM_FREQUENCY_COLUMN_FAMILY.getBytes())), true);
    }

    /**
     * Turn a set of column families into a sorted string set
     *
     * @param columnFamilies
     *            the column families
     * @return a sorted set of column families as Strings
     */
    private SortedSet<String> getSortedCFs(Collection<ByteSequence> columnFamilies) {
        return columnFamilies.stream().map(m -> {
            try {
                return Text.decode(m.getBackingArray(), m.offset(), m.length());
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Get the field and value from the end of the column qualifier of the tf key
     *
     * @param tfKey
     *            the term freq key
     * @return the field name
     */
    private String[] getFieldAndValue(Key tfKey) {
        String cq = tfKey.getColumnQualifier().toString();
        int index = cq.lastIndexOf(Constants.NULL);
        String fieldname = cq.substring(index + 1);
        int index2 = cq.lastIndexOf(Constants.NULL, index - 1);
        String fieldvalue = cq.substring(index2 + 1, index);
        return new String[] {fieldname, fieldvalue};
    }

    /**
     * get the dt and uid from a tf key
     *
     * @param tfKey
     *            the term freq key
     * @return the dt\x00uid
     */
    private String getDtUidFromTfKey(Key tfKey) {
        return getDtUid(tfKey.getColumnQualifier().toString());
    }

    /**
     * Get the dt and uid start or end given an event key
     *
     * @param eventKey
     *            an event key
     * @param startKey
     *            a start key
     * @param inclusive
     *            inclusive boolean flag
     * @return the start or end document (cq) for our tf scan range. Null if dt,uid does not exist in the event key
     */
    private String getDtUidFromEventKey(Key eventKey, boolean startKey, boolean inclusive) {
        // if an infinite end range, or unspecified end document, then no cdocument to specify
        if (eventKey == null || eventKey.getColumnFamily() == null || eventKey.getColumnFamily().getLength() == 0) {
            return null;
        }

        // get the dt/uid from the cf
        String cf = eventKey.getColumnFamily().toString();
        String dtAndUid = getDtUid(cf);

        // if calculating a start cq
        if (startKey) {
            // if the start dt/uid is inclusive and the cf is only the dt and uid, then include this document
            if (inclusive && cf.equals(dtAndUid)) {
                return dtAndUid + Constants.NULL;
            }
            // otherwise start at the next document
            else {
                return dtAndUid + Constants.ONE_BYTE;
            }
        }
        // if calculating an end cq
        else {
            // if the end dt/uid is inclusive or the cf was not only the dt and uid
            if (inclusive || !cf.equals(dtAndUid)) {
                // then include this document
                return dtAndUid + Constants.NULL + Constants.MAX_UNICODE_STRING;
            }
            // otherwise stop before this document
            else {
                return dtAndUid + Constants.NULL;
            }
        }
    }

    // get the dt/uid from the beginning of a given string
    private String getDtUid(String str) {
        int index = str.indexOf(Constants.NULL);
        index = str.indexOf(Constants.NULL, index + 1);
        if (index == -1) {
            return str;
        } else {
            return str.substring(0, index);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TermFrequencyExcerptIterator: ");
        sb.append(this.fieldName);
        sb.append(", ");
        sb.append(this.startOffset);
        sb.append(", ");
        sb.append(this.endOffset);

        return sb.toString();
    }

}
