package datawave.query.iterator.builder;

import java.io.IOException;
import java.util.SortedSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.core.iterators.DatawaveFieldIndexRangeIteratorJexl;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.DocumentAggregatingIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import datawave.query.jexl.LiteralRange;

/**
 * A convenience class that aggregates a field, range, source iterator, normalizer mappings, index only fields, data type filter and key transformer when
 * traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 *
 */
public class IndexRangeIteratorBuilder extends IvaratorBuilder implements IteratorBuilder {
    private static Logger log = Logger.getLogger(IndexRangeIteratorBuilder.class);

    protected LiteralRange range;
    protected SortedSet<Range> subRanges;

    public LiteralRange getRange() {
        return range;
    }

    public void setRange(LiteralRange range) {
        this.range = range;
        setField(range.getFieldName());
        StringBuilder builder = new StringBuilder();
        builder.append(range.getLower()).append("-").append(range.getUpper());
        setValue(builder.toString());
    }

    public SortedSet<Range> getSubRanges() {
        return subRanges;
    }

    public void setSubRanges(SortedSet<Range> subRanges) {
        this.subRanges = subRanges;
    }

    @SuppressWarnings("unchecked")
    @Override
    public NestedIterator<Key> build() {
        if (notNull(range, source, datatypeFilter, keyTform, timeFilter, ivaratorCacheDirs, getField(), getNode())) {
            if (log.isTraceEnabled()) {
                log.trace("Generating ivarator (caching field index iterator) for " + range);
            }

            // we can't build an ivarator if no ivarator directories have been defined
            if (ivaratorCacheDirs.isEmpty())
                throw new IllegalStateException("No ivarator cache dirs defined");

            // ensure that we are able to create the first ivarator cache dir (the control dir)
            validateIvaratorControlDir(ivaratorCacheDirs.get(0));

            DocumentIterator docIterator = null;
            try {
                // create a field index caching ivarator
                // @formatter:off
                DatawaveFieldIndexRangeIteratorJexl rangeIterator = DatawaveFieldIndexRangeIteratorJexl.builder()
                        .withFieldName(new Text(range.getFieldName()))
                        .withLowerBound(range.getLower().toString())
                        .lowerInclusive(range.isLowerInclusive())
                        .withUpperBound(range.getUpper().toString())
                        .upperInclusive(range.isUpperInclusive())
                        .withTimeFilter(this.timeFilter)
                        .withDatatypeFilter(this.datatypeFilter)
                        .negated(false)
                        .withScanThreshold(ivaratorCacheScanPersistThreshold)
                        .withScanTimeout(ivaratorCacheScanTimeout)
                        .withHdfsBackedSetBufferSize(ivaratorCacheBufferSize)
                        .withMaxRangeSplit(maxRangeSplit)
                        .withMaxOpenFiles(ivaratorMaxOpenFiles)
                        .withIvaratorCacheDirs(ivaratorCacheDirs)
                        .withNumRetries(ivaratorNumRetries)
                        .withPersistOptions(ivaratorPersistOptions)
                        .withMaxResults(maxIvaratorResults)
                        .withQueryLock(queryLock)
                        .allowDirResuse(true)
                        .withReturnKeyType(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)
                        .withSortedUUIDs(sortedUIDs)
                        .withCompositeMetadata(compositeMetadata)
                        .withCompositeSeekThreshold(compositeSeekThreshold)
                        .withTypeMetadata(typeMetadata)
                        .withSubRanges(subRanges)
                        .withIteratorEnv(env)
                        .withIvaratorSourcePool(ivaratorSourcePool)
                        .build();
                // @formatter:on

                if (collectTimingDetails) {
                    rangeIterator.setCollectTimingDetails(true);
                    rangeIterator.setQuerySpanCollector(this.querySpanCollector);
                }
                rangeIterator.init(source, null, env);
                log.debug("Created a DatawaveFieldIndexRangeIteratorJexl: " + rangeIterator);

                // Add an iterator to aggregate documents. This is needed for index only fields.
                DocumentAggregatingIterator aggregatingIterator = new DocumentAggregatingIterator(buildDocument, this.typeMetadata, keyTform);
                aggregatingIterator.init(rangeIterator, null, null);

                docIterator = aggregatingIterator;

            } catch (IOException e) {
                throw new IllegalStateException("Unable to initialize regex iterator stack", e);
            }

            IndexIteratorBridge itr = new IndexIteratorBridge(docIterator, getNode(), getField());
            range = null;
            source = null;
            timeFilter = null;
            datatypeFilter = null;
            keyTform = null;
            timeFilter = null;
            ivaratorCacheDirs = null;
            node = null;
            field = null;
            return itr;
        } else {
            StringBuilder msg = new StringBuilder(256);
            msg.append("Cannot build iterator-- a field was null!\n");
            if (range == null) {
                msg.append("\tRange was null!\n");
            }
            if (source == null) {
                msg.append("\tSource was null!\n");
            }
            msg.setLength(msg.length() - 1);
            throw new IllegalStateException(msg.toString());
        }
    }
}
