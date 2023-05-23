package datawave.query.scheduler;

import com.google.common.collect.Lists;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.webservice.query.configuration.QueryData;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.util.Collection;

/**
 * 
 */
public abstract class Scheduler<T> implements CloseableIterable<T> {
    
    protected Collection<IteratorSetting> settings = Lists.newArrayList();
    
    public abstract BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException;
    
    /**
     * Returns the scan session stats provided by this scheduler
     * 
     * @return the scan session stats
     */
    public abstract ScanSessionStats getSchedulerStats();
    
    public void addSetting(IteratorSetting customSetting) {
        settings.add(customSetting);
    }
    
}
