package datawave.query.iterator.builder;

import datawave.core.iterators.DatawaveFieldIndexRegexIteratorJexl;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.DocumentAggregatingIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A convenience class that aggregates a field, regex, source iterator, normalizer mappings, index only fields, data type filter and key transformer when
 * traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 * 
 */
public class IndexRegexIteratorBuilder extends IvaratorBuilder implements IteratorBuilder {
    private static Logger log = Logger.getLogger(IndexRegexIteratorBuilder.class);
    
    protected Boolean negated;
    
    public boolean isNegated() {
        return negated;
    }
    
    public void setNegated(boolean negated) {
        this.negated = negated;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public NestedIterator<Key> build() {
        if (notNull(field, value, negated, source, datatypeFilter, timeFilter, keyTform, ivaratorCacheDirURI, hdfsFileSystem)) {
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
                DatawaveFieldIndexRegexIteratorJexl regexIterator = DatawaveFieldIndexRegexIteratorJexl.builder()
                        .withFieldName(new Text(field))
                        .withFieldValue(new Text(value))
                        .withTimeFilter(timeFilter)
                        .withDatatypeFilter(datatypeFilter)
                        .negated(negated)
                        .withScanThreshold(ivaratorCacheScanPersistThreshold)
                        .withScanTimeout(ivaratorCacheScanTimeout)
                        .withHdfsBackedSetBufferSize(ivaratorCacheBufferSize)
                        .withMaxRangeSplit(maxRangeSplit)
                        .withMaxOpenFiles(ivaratorMaxOpenFiles)
                        .withMaxResults(maxIvaratorResults)
                        .withFileSystem(hdfsFileSystem)
                        .withUniqueDir(new Path(hdfsCacheURI))
                        .withQueryLock(queryLock)
                        .allowDirResuse(true)
                        .withReturnKeyType(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)
                        .withSortedUUIDs(sortedUIDs)
                        .withCompositeMetadata(compositeMetadata)
                        .withCompositeSeekThreshold(compositeSeekThreshold)
                        .withTypeMetadata(typeMetadata)
                        .withIteratorEnv(env)
                        .build();
                // @formatter:on
                
                if (collectTimingDetails) {
                    regexIterator.setCollectTimingDetails(true);
                    regexIterator.setQuerySpanCollector(this.querySpanCollector);
                }
                regexIterator.init(source, null, env);
                log.debug("Created a DatawaveFieldIndexRegexIteratorJexl: " + regexIterator);
                
                boolean canBuildDocument = this.fieldsToAggregate == null ? false : this.fieldsToAggregate.contains(field);
                if (forceDocumentBuild) {
                    canBuildDocument = true;
                }
                // Add an interator to aggregate documents. This is needed for index only fields.
                DocumentAggregatingIterator aggregatingIterator = new DocumentAggregatingIterator(canBuildDocument, this.typeMetadata, keyTform);
                aggregatingIterator.init(regexIterator, null, null);
                
                docIterator = aggregatingIterator;
                
            } catch (IOException e) {
                throw new IllegalStateException("Unable to initialize regex iterator stack", e);
                // } catch (JavaRegexParseException e) {
                // throw new IllegalStateException("Unable to parse regex " + value, e);
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
