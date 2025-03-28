package datawave.query.iterator.builder;

import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.util.fst.FST;

import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.DocumentAggregatingIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;

/**
 * A convenience class that aggregates a field, a list of values, source iterator, normalizer mappings, index only fields, data type filter and key transformer
 * when traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 *
 */
public class IndexListIteratorBuilder extends IvaratorBuilder implements IteratorBuilder {
    private static Logger log = Logger.getLogger(IndexListIteratorBuilder.class);

    protected Boolean negated;
    protected Set<String> values;
    protected FST fst;

    public boolean isNegated() {
        return negated;
    }

    public void setNegated(boolean negated) {
        this.negated = negated;
    }

    public Set<String> getValues() {
        return values;
    }

    public void setValues(Set<String> values) {
        StringBuilder builder = new StringBuilder();
        for (String value : values) {
            builder.append(value);
        }
        setValue(builder.toString());
        this.values = values;
    }

    public FST getFst() {
        return fst;
    }

    public void setFst(FST fst) {
        this.fst = fst;
    }

    @SuppressWarnings("unchecked")
    @Override
    public NestedIterator<Key> build() {
        if (notNull(field, (values != null ? values : fst), negated, source, datatypeFilter, timeFilter, keyTform, ivaratorCacheDirs, getField(), getNode())) {
            if (log.isTraceEnabled()) {
                log.trace("Generating ivarator (caching field index iterator) for " + field + (negated ? "!~" : "=~") + value);
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
                DatawaveFieldIndexListIteratorJexl.Builder builder = DatawaveFieldIndexListIteratorJexl.builder()
                        .withFieldName(new Text(field))
                        .withTimeFilter(timeFilter)
                        .withDatatypeFilter(datatypeFilter)
                        .negated(negated)
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
                        .withIvaratorSourcePool(ivaratorSourcePool)
                        .withIteratorEnv(env);
                // @formatter:on
                if (values != null) {
                    builder = builder.withValues(values);
                } else {
                    builder = builder.withFST(fst);
                }
                DatawaveFieldIndexListIteratorJexl listIterator = builder.build();

                if (collectTimingDetails) {
                    listIterator.setCollectTimingDetails(true);
                    listIterator.setQuerySpanCollector(this.querySpanCollector);
                }
                listIterator.init(source, null, env);
                log.debug("Created a DatawaveFieldIndexListIteratorJexl: " + listIterator);

                // Add an interator to aggregate documents. This is needed for index only fields.
                DocumentAggregatingIterator aggregatingIterator = new DocumentAggregatingIterator(buildDocument, this.typeMetadata, keyTform);
                aggregatingIterator.init(listIterator, null, null);

                docIterator = aggregatingIterator;

            } catch (IOException e) {
                throw new IllegalStateException("Unable to initialize list iterator stack", e);
            }

            IndexIteratorBridge itr = new IndexIteratorBridge(docIterator, getNode(), getField());
            field = null;
            value = null;
            negated = null;
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
            if (field == null) {
                msg.append("\tField was null!\n");
            }
            if (value == null) {
                msg.append("\tValue was null!\n");
            }
            if (source == null) {
                msg.append("\tSource was null!\n");
            }
            msg.setLength(msg.length() - 1);
            throw new IllegalStateException(msg.toString());
        }
    }

}
