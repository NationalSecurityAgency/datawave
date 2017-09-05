package datawave.query.rewrite.iterator.builder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.query.rewrite.iterator.DocumentIterator;
import datawave.query.rewrite.iterator.NestedIterator;
import datawave.query.rewrite.iterator.logic.DocumentAggregatingIterator;
import datawave.query.rewrite.iterator.logic.IndexIteratorBridge;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.util.fst.FST;

/**
 * A convenience class that aggregates a field, a list of values, source iterator, normalizer mappings, index only fields, data type filter and key transformer
 * when traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 * 
 */
public class IndexListIteratorBuilder extends IvaratorBuilder implements IteratorBuilder {
    private static Logger log = Logger.getLogger(IndexListIteratorBuilder.class);
    
    protected FileSystem fstHdfsFileSystem;
    protected Boolean negated;
    protected Set<String> values;
    protected URI fstURI;
    
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
    
    public FileSystem getFstHdfsFileSystem() {
        return fstHdfsFileSystem;
    }
    
    public void setFstHdfsFileSystem(FileSystem fstHdfsFileSystem) {
        this.fstHdfsFileSystem = fstHdfsFileSystem;
    }
    
    public URI getFstURI() {
        return fstURI;
    }
    
    public void setFstURI(URI fstURI) {
        this.fstURI = fstURI;
        if (null != fstURI)
            setValue(fstURI.toASCIIString());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public NestedIterator<Key> build() {
        if (notNull(field, (values != null ? values : fstURI), negated, source, datatypeFilter, timeFilter, keyTform, ivaratorCacheDirURI, hdfsFileSystem)) {
            if (log.isTraceEnabled()) {
                log.trace("Generating ivarator (caching field index iterator) for " + field + (negated ? "!~" : "=~") + value);
            }
            // get the hadoop file system and a temporary directory
            final URI hdfsCacheURI;
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
                DatawaveFieldIndexListIteratorJexl listIterator = null;
                if (values != null) {
                    listIterator = new DatawaveFieldIndexListIteratorJexl(new Text(field), values, this.timeFilter, this.datatypeFilter, negated,
                                    ivaratorCacheScanPersistThreshold, ivaratorCacheScanTimeout, ivaratorCacheBufferSize, maxRangeSplit, ivaratorMaxOpenFiles,
                                    hdfsFileSystem, new Path(hdfsCacheURI), queryLock, true, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME, sortedUIDs);
                } else {
                    FST fst = DatawaveFieldIndexListIteratorJexl.FSTManager.get(new Path(fstURI), hdfsFileCompressionCodec, fstHdfsFileSystem);
                    listIterator = new DatawaveFieldIndexListIteratorJexl(new Text(field), fst, this.timeFilter, this.datatypeFilter, negated,
                                    ivaratorCacheScanPersistThreshold, ivaratorCacheScanTimeout, ivaratorCacheBufferSize, maxRangeSplit, ivaratorMaxOpenFiles,
                                    hdfsFileSystem, new Path(hdfsCacheURI), queryLock, true, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME, sortedUIDs);
                }
                if (collectTimingDetails) {
                    listIterator.setCollectTimingDetails(true);
                    listIterator.setQuerySpanCollector(this.querySpanCollector);
                }
                listIterator.init(source, null, null);
                log.debug("Created a DatawaveFieldIndexListIteratorJexl: " + listIterator);
                
                boolean canBuildDocument = this.fieldsToAggregate == null ? false : this.fieldsToAggregate.contains(field);
                if (forceDocumentBuild) {
                    canBuildDocument = true;
                }
                
                // Add an interator to aggregate documents. This is needed for index only fields.
                DocumentAggregatingIterator aggregatingIterator = new DocumentAggregatingIterator(canBuildDocument, this.typeMetadata, keyTform);
                aggregatingIterator.init(listIterator, null, null);
                
                docIterator = aggregatingIterator;
                
            } catch (IOException e) {
                throw new IllegalStateException("Unable to initialize list iterator stack", e);
            }
            
            IndexIteratorBridge itr = new IndexIteratorBridge(docIterator);
            field = null;
            value = null;
            negated = null;
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
