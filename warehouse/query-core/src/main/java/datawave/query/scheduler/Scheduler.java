package datawave.query.scheduler;

import com.google.common.collect.Lists;
import datawave.microservice.common.storage.QueryCheckpoint;
import datawave.microservice.common.storage.QueryKey;
import datawave.microservice.query.configuration.QueryData;
import datawave.microservice.query.configuration.Result;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.stats.ScanSessionStats;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.util.Collection;
import java.util.List;

/**
 * 
 */
public abstract class Scheduler implements CloseableIterable<Result> {
    
    protected Collection<IteratorSetting> settings = Lists.newArrayList();
    
    public abstract BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException;
    
    /**
     * Returns the scan session stats provided by this scheduler
     * 
     * @return
     */
    public abstract ScanSessionStats getSchedulerStats();
    
    public void addSetting(IteratorSetting customSetting) {
        settings.add(customSetting);
    }
    
    public abstract List<QueryCheckpoint> checkpoint(QueryKey queryKey);
}
