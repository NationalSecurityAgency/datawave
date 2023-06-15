package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import datawave.query.function.DocumentRangeProvider;
import datawave.query.function.Equality;
import datawave.query.function.PrefixEquality;
import datawave.query.function.RangeProvider;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.attributes.Document;
import datawave.query.function.KeyToDocumentData;
import datawave.query.iterator.aggregation.DocumentData;
import datawave.query.iterator.filter.KeyIdentity;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

public class DocumentDataIterator implements Iterator<DocumentData> {
    private static final Logger log = Logger.getLogger(DocumentDataIterator.class);

    private static final Collection<ByteSequence> columnFamilies = Lists.<ByteSequence> newArrayList(new ArrayByteSequence("tf"), new ArrayByteSequence("d"));
    private static final boolean inclusive = false;

    protected SortedKeyValueIterator<Key,Value> source;
    protected Range totalRange;

    private Entry<DocumentData,Document> documentData = null;

    private RangeProvider rangeProvider;

    private final Predicate<Key> dataTypeFilter;

    private final KeyToDocumentData documentMapper;

    private final Text holder1 = new Text(), holder2 = new Text();

    public DocumentDataIterator(SortedKeyValueIterator<Key,Value> source, Range totalRange, boolean includeChildCount, boolean includeParent) {
        this(source, totalRange, KeyIdentity.Function, new PrefixEquality(PartialKey.ROW_COLFAM), includeChildCount, includeParent);
    }

    public DocumentDataIterator(SortedKeyValueIterator<Key,Value> source, Range totalRange, Predicate<Key> dataTypeFilter, Equality eq,
                    boolean includeChildCount, boolean includeParent) {
        this(source, totalRange, dataTypeFilter, eq, null, null, includeChildCount, includeParent);
    }

    public DocumentDataIterator(SortedKeyValueIterator<Key,Value> source, Range totalRange, Predicate<Key> dataTypeFilter, Equality eq,
                    EventDataQueryFilter evaluationFilter, RangeProvider rangeProvider, boolean includeChildCount, boolean includeParent) {
        this(source, null, null, totalRange, dataTypeFilter, eq, evaluationFilter, rangeProvider, includeChildCount, includeParent);
    }

    public DocumentDataIterator(SortedKeyValueIterator<Key,Value> source, final IteratorEnvironment env, final Map<String,String> options, Range totalRange,
                    Predicate<Key> dataTypeFilter, Equality eq, EventDataQueryFilter evaluationFilter, RangeProvider rangeProvider, boolean includeChildCount,
                    boolean includeParent) {
        this.source = source;
        this.totalRange = totalRange;

        try {
            this.source.seek(totalRange, columnFamilies, inclusive);
        } catch (IOException e) {
            throw new RuntimeException("Could not seek in constructor", e);
        }

        this.dataTypeFilter = dataTypeFilter;
        this.rangeProvider = rangeProvider;

        this.documentMapper = new KeyToDocumentData(source, env, options, eq, evaluationFilter, includeChildCount, includeParent)
                        .withRangeProvider(getRangeProvider());

        findNextDocument();
    }

    @Override
    public boolean hasNext() {
        return documentData != null;
    }

    @Override
    public DocumentData next() {
        DocumentData returnDocumentData = documentData.getKey();

        findNextDocument();

        return returnDocumentData;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not implemented");
    }

    protected void findNextDocument() {
        documentData = null;

        try {
            Text cf = new Text();

            /*
             * Given that we are already at a document key, this method will continue to advance the underlying source until it is either exhausted (hasTop()
             * returns false), the returned key is not in the totalRange, and the current top key shares the same row and column family as the source's next
             * key.
             */
            while (documentData == null && source.hasTop()) {
                Key k = source.getTopKey();
                if (log.isTraceEnabled())
                    log.trace("Sought to " + k);
                k.getColumnFamily(cf);

                if (!isEventKey(k)) {
                    if (cf.find("fi\0") == 0) {
                        if (log.isDebugEnabled()) {
                            log.debug("Seeking over 'fi')");
                        }
                        // Try to do an optimized jump over the field index
                        cf.set("fi\1");
                        source.seek(new Range(new Key(source.getTopKey().getRow(), cf), false, totalRange.getEndKey(), totalRange.isEndKeyInclusive()),
                                        columnFamilies, inclusive);
                    } else if (cf.getLength() == 1 && cf.charAt(0) == 'd') {
                        if (log.isDebugEnabled()) {
                            log.debug("Seeking over 'd'");
                        }
                        // Try to do an optimized jump over the raw documents
                        cf.set("d\0");
                        source.seek(new Range(new Key(source.getTopKey().getRow(), cf), false, totalRange.getEndKey(), totalRange.isEndKeyInclusive()),
                                        columnFamilies, inclusive);
                    } else if (cf.getLength() == 2 && cf.charAt(0) == 't' && cf.charAt(1) == 'f') {
                        if (log.isDebugEnabled()) {
                            log.debug("Seeking over 'tf'");
                        }
                        // Try to do an optimized jump over the term frequencies
                        cf.set("tf\0");
                        source.seek(new Range(new Key(source.getTopKey().getRow(), cf), false, totalRange.getEndKey(), totalRange.isEndKeyInclusive()),
                                        columnFamilies, inclusive);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Next()'ing over the current key");
                        }
                        source.next();
                    }
                } else {
                    Key pointer = source.getTopKey();
                    if (dataTypeFilter.apply(pointer)) {
                        this.documentData = this.documentMapper.apply(Maps.immutableEntry(pointer, new Document()));
                    }
                    // now bounce to the next document as the documentMapper may have moved the source considerably
                    Key nextDocKey = this.rangeProvider != null ? this.rangeProvider.getStopKey(pointer) : pointer.followingKey(PartialKey.ROW_COLFAM);
                    if (totalRange.contains(nextDocKey)) {
                        Range nextCF = new Range(nextDocKey, true, totalRange.getEndKey(), totalRange.isEndKeyInclusive());
                        source.seek(nextCF, columnFamilies, inclusive);
                    } else {
                        // skip to the end
                        Range nextCF = new Range(totalRange.getEndKey(), false,
                                        totalRange.getEndKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL), false);
                        source.seek(nextCF, columnFamilies, inclusive);

                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not seek in findNextDocument", e);
        }
    }

    protected boolean isEventKey(Key k) {
        Text cf = k.getColumnFamily();
        return cf.getLength() > 0 && cf.find("\u0000") != -1 && !((cf.charAt(0) == 'f' && cf.charAt(1) == 'i' && cf.charAt(2) == 0)
                        || (cf.getLength() == 1 && cf.charAt(0) == 'd') || (cf.getLength() == 2 && cf.charAt(0) == 't' && cf.charAt(1) == 'f'));
    }

    protected RangeProvider getRangeProvider() {
        if (rangeProvider == null) {
            rangeProvider = new DocumentRangeProvider();
        }
        return rangeProvider;
    }
}
