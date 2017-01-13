package nsa.datawave.query.rewrite.scheduler;

import java.util.Collection;
import java.util.Map.Entry;

import nsa.datawave.query.rewrite.CloseableIterable;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.tables.stats.ScanSessionStats;
import nsa.datawave.webservice.query.configuration.QueryData;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Lists;

/**
 * 
 */
public abstract class Scheduler implements CloseableIterable<Entry<Key,Value>> {
    
    protected Collection<IteratorSetting> settings = Lists.newArrayList();
    
    public abstract BatchScanner createBatchScanner(RefactoredShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd)
                    throws TableNotFoundException;
    
    /**
     * Returns the scan session stats provided by this scheduler
     * 
     * @return
     */
    public abstract ScanSessionStats getSchedulerStats();
    
    public void addSetting(IteratorSetting customSetting) {
        settings.add(customSetting);
    }
    
}
