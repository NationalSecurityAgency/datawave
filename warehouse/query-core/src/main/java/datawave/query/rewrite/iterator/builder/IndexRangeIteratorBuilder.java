package datawave.query.rewrite.iterator.builder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import datawave.core.iterators.DatawaveFieldIndexRangeIteratorJexl;
import datawave.query.rewrite.iterator.DocumentIterator;
import datawave.query.rewrite.iterator.NestedIterator;
import datawave.query.rewrite.iterator.logic.DocumentAggregatingIterator;
import datawave.query.rewrite.iterator.logic.IndexIteratorBridge;
import datawave.query.rewrite.jexl.LiteralRange;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * A convenience class that aggregates a field, range, source iterator, normalizer mappings, index only fields, data type filter and key transformer when
 * traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 * 
 */
public class IndexRangeIteratorBuilder extends IvaratorBuilder implements IteratorBuilder {
    private static Logger log = Logger.getLogger(IndexRangeIteratorBuilder.class);
    
    protected LiteralRange range;
    
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
    
    @SuppressWarnings("unchecked")
    @Override
    public NestedIterator<Key> build() {
        if (notNull(range, source, datatypeFilter, keyTform, timeFilter, ivaratorCacheDirURI, hdfsFileSystem)) {
            if (log.isTraceEnabled()) {
                log.trace("Generating ivarator (caching field index iterator) for " + range);
            }
            // get the hadoop file system and a temporary directory
            URI hdfsCacheURI = null;
            try {
                hdfsCacheURI = new URI(ivaratorCacheDirURI);
                hdfsFileSystem.mkdirs(new Path(hdfsCacheURI));
            } catch (MalformedURLException e) {
                throw new IllegalStateException("Unable to load hadoop configuration", e);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to create hadoop file system", e);
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Invalid hdfs cache dir URI: " + ivaratorCacheDirURI, e);
            }
            
            DocumentIterator docIterator = null;
            try {
                // create a field index caching ivarator
                DatawaveFieldIndexRangeIteratorJexl rangeIterator = new DatawaveFieldIndexRangeIteratorJexl(new Text(range.getFieldName()), new Text(range
                                .getLower().toString()), range.isLowerInclusive(), new Text(range.getUpper().toString()), range.isUpperInclusive(),
                                this.timeFilter, this.datatypeFilter, false, ivaratorCacheScanPersistThreshold, ivaratorCacheScanTimeout,
                                ivaratorCacheBufferSize, maxRangeSplit, ivaratorMaxOpenFiles, hdfsFileSystem, new Path(hdfsCacheURI), queryLock, true,
                                PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME, sortedUIDs);
                if (collectTimingDetails) {
                    rangeIterator.setCollectTimingDetails(true);
                    rangeIterator.setQuerySpanCollector(this.querySpanCollector);
                }
                rangeIterator.init(source, null, null);
                log.debug("Created a DatawaveFieldIndexRangeIteratorJexl: " + rangeIterator);
                
                boolean canBuildDocument = this.indexOnlyFields == null ? false : this.indexOnlyFields.contains(field);
                if (forceDocumentBuild) {
                    canBuildDocument = true;
                }
                
                // Add an interator to aggregate documents. This is needed for index only fields.
                DocumentAggregatingIterator aggregatingIterator = new DocumentAggregatingIterator(canBuildDocument, this.typeMetadata, keyTform);
                aggregatingIterator.init(rangeIterator, null, null);
                
                docIterator = aggregatingIterator;
                
            } catch (IOException e) {
                throw new IllegalStateException("Unable to initialize regex iterator stack", e);
            }
            
            IndexIteratorBridge itr = new IndexIteratorBridge(docIterator);
            range = null;
            source = null;
            timeFilter = null;
            datatypeFilter = null;
            keyTform = null;
            timeFilter = null;
            hdfsFileSystem = null;
            ivaratorCacheDirURI = null;
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
