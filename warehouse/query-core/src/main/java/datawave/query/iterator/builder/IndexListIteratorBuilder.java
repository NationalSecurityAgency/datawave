package datawave.query.iterator.builder;

import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.DocumentAggregatingIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

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
        if (notNull(field, (values != null ? values : fst), negated, source, datatypeFilter, timeFilter, keyTform, ivaratorCacheDirURI, hdfsFileSystem)) {
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
                        .withFileSystem(hdfsFileSystem)
                        .withMaxResults(maxIvaratorResults)
                        .withUniqueDir(new Path(hdfsCacheURI))
                        .withQueryLock(queryLock)
                        .allowDirResuse(true)
                        .withReturnKeyType(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)
                        .withSortedUUIDs(sortedUIDs)
                        .withCompositeMetadata(compositeMetadata)
                        .withCompositeSeekThreshold(compositeSeekThreshold)
                        .withTypeMetadata(typeMetadata)
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
