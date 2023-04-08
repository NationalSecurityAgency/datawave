package datawave.query.planner;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.query.CloseableIterable;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.index.lookup.CreateUidsIterator;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.UidIntersector;
import datawave.query.planner.pushdown.PushDownPlanner;
import datawave.query.tables.ScannerFactory;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public abstract class QueryPlanner implements PushDownPlanner {
    
    protected Class<? extends SortedKeyValueIterator<Key,Value>> createUidsIteratorClass = CreateUidsIterator.class;
    
    protected UidIntersector uidIntersector = new IndexInfo();
    
    /**
     * Process the {@code query} with the provided {@code config} to generate an {@link Iterable}&lt;QueryData&gt; to apply each to a BatchScanner.
     *
     * @param config
     *            the query configuration config
     * @param query
     *            the query string
     * @param settings
     *            the query settings
     * @param scannerFactory
     *            the scanner factory
     * @return query data
     * @throws DatawaveQueryException
     *             for issues with the datawave query
     */
    public abstract CloseableIterable<QueryData> process(GenericQueryConfiguration config, String query, Query settings, ScannerFactory scannerFactory)
                    throws DatawaveQueryException;
    
    public abstract long maxRangesPerQueryPiece();
    
    public abstract void close(GenericQueryConfiguration config, Query settings);
    
    public abstract void setQueryIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> clazz);
    
    public abstract Class<? extends SortedKeyValueIterator<Key,Value>> getQueryIteratorClass();
    
    public abstract String getPlannedScript();
    
    public abstract QueryPlanner clone();
    
    public Class<? extends SortedKeyValueIterator<Key,Value>> getCreateUidsIteratorClass() {
        return createUidsIteratorClass;
    }
    
    public void setCreateUidsIteratorClass(Class<? extends SortedKeyValueIterator<Key,Value>> createUidsIteratorClass) {
        this.createUidsIteratorClass = createUidsIteratorClass;
    }
    
    public UidIntersector getUidIntersector() {
        return uidIntersector;
    }
    
    public void setUidIntersector(UidIntersector uidIntersector) {
        this.uidIntersector = uidIntersector;
    }
    
}
