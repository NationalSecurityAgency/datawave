package datawave.query.iterator.logic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.iterator.NestedIterator;
import datawave.table.constants.QueryConstants;

/**
 * A context required iterator that determines if a field has any value in the field index for a candidate document. It is used to evaluate null checks against
 * index-only fields at the field index:
 * <ul>
 * <li>As an include, it satisfies {@code FIELD != null} and {@code !(FIELD == null)}.</li>
 * <li>As an exclude, it satisfies {@code FIELD == null}.</li>
 * </ul>
 * This iterator never generates candidates. It is moved to a candidate supplied by an anchor term and returns at most one key per candidate: the candidate
 * itself if the field is present, otherwise a key that sorts immediately after the candidate.
 * <p>
 * With TLD matching enabled via {@link #withTldMatching(boolean)}, the candidate is a top level document, and entries for any descendant document
 * ({@code uid.*}) also count as present.
 * <p>
 * Field Index key structure
 * <p>
 * <code>row fi\x00FIELD : value\x00datatype\x00uid</code>
 */
public class IndexOnlyFieldPresenceFilter implements NestedIterator<Key> {

    private static final Logger log = LoggerFactory.getLogger(IndexOnlyFieldPresenceFilter.class);

    /**
     * Sorts after any datatype for a given value.
     */
    private static final byte[] MAX_BYTES = QueryConstants.MAX_UNICODE_STR.getBytes(StandardCharsets.UTF_8);

    /**
     * The prefix of all FI columns before the field portion.
     */
    private static final String FI_CF_PREFIX = "fi" + QueryConstants.NULL_BYTE_STR;

    private final SortedKeyValueIterator<Key,Value> source;
    private final String field;
    private final Text columnFamily;
    private final Collection<ByteSequence> columnFamiliesToSeek;

    private Predicate<Key> timeFilter;
    private boolean tldMatching = false;
    private AttributeFactory attributeFactory = null;
    private Document document = new Document();

    /**
     * Return a new {@link IndexOnlyFieldPresenceFilter} that will check for the presence of the given field in the elements the given source iterator.
     *
     * @param field
     *            the field
     * @param source
     *            the source data
     * @throws IllegalArgumentException
     *             if the field is null or blank
     * @throws NullPointerException
     *             if the source iterator is null
     */
    public IndexOnlyFieldPresenceFilter(String field, SortedKeyValueIterator<Key,Value> source) {
        Preconditions.checkArgument(field != null && !field.isBlank(), "field must not be null or blank");
        Preconditions.checkNotNull(source, "source must not be null");

        this.field = field;
        this.source = source;
        this.columnFamily = new Text(FI_CF_PREFIX + field);
        this.columnFamiliesToSeek = Collections.singleton(new ArrayByteSequence(this.columnFamily.copyBytes()));
    }

    /**
     * Return the field this iterator checks for the presence of.
     *
     * @return the field
     */
    public String getField() {
        return field;
    }

    /**
     * Enable top level document matching: the candidate is a top level document, and an entry for the candidate or any of its descendants counts as present.
     *
     * @param tldMatching
     *            true to count entries for descendant documents
     * @return this iterator
     */
    public IndexOnlyFieldPresenceFilter withTldMatching(boolean tldMatching) {
        this.tldMatching = tldMatching;
        return this;
    }

    /**
     * Set the attribute factory for this iterator.
     *
     * @param attributeFactory
     *            the factory
     * @return this iterator
     */
    public IndexOnlyFieldPresenceFilter withAttributeFactory(AttributeFactory attributeFactory) {
        this.attributeFactory = attributeFactory;
        return this;
    }

    /**
     * Set the time filter for this iterator.
     *
     * @param timeFilter
     *            the time filter
     * @return this iterator
     */
    public IndexOnlyFieldPresenceFilter withTimeFilter(Predicate<Key> timeFilter) {
        this.timeFilter = timeFilter;
        return this;
    }

    /**
     * When an attribute factory is provided and the field is present for the last candidate, the document contains a single attribute for the field that is
     * used for evaluation only (it is not returned). Otherwise, the document is empty.
     *
     * @return a document
     */
    @Override
    public Document document() {
        return this.document;
    }

    /**
     * Determine if the field is present for the candidate document
     *
     * @param minimum
     *            a candidate document key in the form <code>row datatype\0uid</code>
     * @return the candidate if the field is present, otherwise the candidate's column family plus a null byte
     */
    @Override
    public Key move(Key minimum) {
        Text row = minimum.getRow();
        byte[] targetCF = minimum.getColumnFamilyData().toArray();

        // If TLD matching is enabled, entries for descendants of the target (datatype\0uid.*) also count.
        byte[] descendantPrefix = tldMatching ? append(targetCF, (byte) '.') : null;

        this.document = new Document();

        Key start = new Key(row, this.columnFamily);
        Range range = new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM), false);
        advanceTo(range);

        boolean found = false;
        // If we have not yet found a match for the field.
        while (!found && source.hasTop()) {
            Key topKey = source.getTopKey();
            ByteSequence topCQ = topKey.getColumnQualifierData();

            int secondNullPos = lastIndexOfNull(topCQ, topCQ.length());
            int firstNullPos = secondNullPos > 0 ? lastIndexOfNull(topCQ, secondNullPos) : -1;
            if (firstNullPos < 0) {
                log.warn("Skipping malformed field index key: {}", topKey);
                advanceToNext();
                continue;
            }

            // Compare the datatype and uid portion of a column qualifier against the target as unsigned bytes.
            int cmp = compare(topCQ, firstNullPos + 1, targetCF);
            if (cmp < 0) {
                // Same value, seek forward to the target datatype and uid.
                Text cq = columnQualifier(topCQ, firstNullPos, targetCF);
                advanceTo(row, cq, range.getEndKey());
            } else if (cmp > 0 && descendantPrefix != null && compare(topCQ, firstNullPos + 1, descendantPrefix) < 0) {
                // A different uid that sorts between the target and its descendants, seek forward to the descendants.
                Text cq = columnQualifier(topCQ, firstNullPos, descendantPrefix);
                advanceTo(row, cq, range.getEndKey());
            } else if (cmp > 0 && !(descendantPrefix != null && startsWith(topCQ, firstNullPos + 1, descendantPrefix))) {
                // The target is not under this value, seek to the next value.
                Text cq = columnQualifier(topCQ, firstNullPos, MAX_BYTES);
                advanceTo(row, cq, range.getEndKey());
            } else if (timeFilter == null || timeFilter.apply(topKey)) {
                found = true;
                if (attributeFactory != null) {
                    // Record the presence of the field for evaluation only, it is not returned.
                    String value = new String(topCQ.getBackingArray(), topCQ.offset(), firstNullPos, StandardCharsets.UTF_8);
                    document.put(field, attributeFactory.create(field, value, topKey, false));
                }
            } else {
                // Matched everything except the time filter.
                advanceToNext();
            }
        }

        if (found) {
            return minimum;
        }

        byte[] miss = new byte[targetCF.length + 1];
        System.arraycopy(targetCF, 0, miss, 0, targetCF.length);
        miss[targetCF.length] = QueryConstants.NULL_BYTE;
        return new Key(row, new Text(miss));
    }

    /**
     * Build a column qualifier from the value portion of an existing column qualifier and a suffix.
     *
     * @param cq
     *            an existing column qualifier
     * @param firstNull
     *            the offset of the null byte that terminates the value
     * @param suffix
     *            the bytes to append after the value and null byte
     * @return a new column qualifier
     */
    private static Text columnQualifier(ByteSequence cq, int firstNull, byte[] suffix) {
        byte[] bytes = new byte[firstNull + 1 + suffix.length];
        System.arraycopy(cq.getBackingArray(), cq.offset(), bytes, 0, firstNull + 1);
        System.arraycopy(suffix, 0, bytes, firstNull + 1, suffix.length);
        return new Text(bytes);
    }

    /**
     * Return whether the given sequence starts with the given prefix at the given offset.
     *
     * @param sequence
     *            the sequence
     * @param offset
     *            the offset position
     * @param prefix
     *            the prefix
     * @return true if the sequence starts with prefix at the offset or false otherwise
     */
    private static boolean startsWith(ByteSequence sequence, int offset, byte[] prefix) {
        return sequence.length() - offset >= prefix.length && compare(sequence, offset, prefix, prefix.length) == 0;
    }

    /**
     * Compare the bytes of the given byte sequence, starting at {@code offset}, against the target as unsigned bytes.
     *
     * @param seq
     *            the byte sequence
     * @param offset
     *            the offset
     * @param target
     *            the target to compare against
     * @return the comparison
     */
    private static int compare(ByteSequence seq, int offset, byte[] target) {
        return compare(seq, offset, target, seq.length() - offset);
    }

    /**
     * Compare at most {@code length} bytes of the given byte sequence, starting at {@code offset}, against the target as unsigned bytes.
     *
     * @param seq
     *            the byte sequence
     * @param offset
     *            the offset
     * @param target
     *            the target to compare against
     * @param length
     *            the number of bytes to compare
     * @return the comparison
     */
    private static int compare(ByteSequence seq, int offset, byte[] target, int length) {
        int n = Math.min(length, target.length);
        for (int i = 0; i < n; i++) {
            int diff = (seq.byteAt(offset + i) & 0xFF) - (target[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return length - target.length;
    }

    /**
     * Create and return a new byte array with the contents of the given byte array and the given suffix byte in the last element.
     *
     * @param bytes
     *            the bytes
     * @param suffix
     *            the suffix
     * @return the new byte array
     */
    private static byte[] append(byte[] bytes, byte suffix) {
        byte[] appended = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, appended, 0, bytes.length);
        appended[bytes.length] = suffix;
        return appended;
    }

    /**
     * Return the last index of the null byte in the given byte sequence, starting at element {@code end} and walking backwards through the sequence.
     *
     * @param seq
     *            the byte sequence
     * @param end
     *            the end position to start at
     * @return the last index of the null byte, or -1 if it was not found
     */
    private static int lastIndexOfNull(ByteSequence seq, int end) {
        for (int i = end - 1; i >= 0; i--) {
            if (seq.byteAt(i) == QueryConstants.NULL_BYTE) {
                return i;
            }
        }
        return -1;
    }

    /**
     * See to the first {@link Key} with the given row and column qualifier, with the target field in the column qualifier.
     *
     * @param row
     *            the row
     * @param columnQualifier
     *            the column qualifier
     * @param endKey
     *            the end key
     */
    private void advanceTo(Text row, Text columnQualifier, Key endKey) {
        Key next = new Key(row, this.columnFamily, columnQualifier);
        Range range = new Range(next, true, endKey, false);
        advanceTo(range);
    }

    /**
     * Seek to the first {@link Key} in the given range in with the target field in the column family.
     *
     * @param range
     *            the range to seek to
     */
    private void advanceTo(Range range) {
        try {
            this.source.seek(range, this.columnFamiliesToSeek, true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to seek to range " + range, e);
        }
    }

    /**
     * Advance to the next {@link Key}.
     */
    private void advanceToNext() {
        try {
            this.source.next();
        } catch (IOException e) {
            throw new RuntimeException("Error thrown by source.next()", e);
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        // No-op, this iterator is only positioned via move.
    }

    @Override
    public void initialize() {
        // No-op.
    }

    @Override
    public Collection<NestedIterator<Key>> leaves() {
        return List.of();
    }

    @Override
    public Collection<NestedIterator<Key>> children() {
        return List.of();
    }

    @Override
    public boolean isContextRequired() {
        return true;
    }

    @Override
    public void setContext(Key context) {
        // No-op, context is always supplied via move.
    }

    @Override
    public boolean isNonEventField() {
        return false;
    }

    @Override
    public boolean hasNext() {
        // A context required iterator always has a potential next value.
        return true;
    }

    @Override
    public Key next() {
        // This iterator only advances via move.
        return null;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", IndexOnlyFieldPresenceFilter.class.getSimpleName() + "[", "]").add("field='" + field + "'")
                        .add("tldMatching=" + tldMatching).toString();
    }
}
