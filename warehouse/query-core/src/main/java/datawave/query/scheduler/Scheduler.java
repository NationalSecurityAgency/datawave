package datawave.query.scheduler;

import java.util.Collection;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Lists;

import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.webservice.query.configuration.QueryData;

public abstract class Scheduler implements CloseableIterable<Entry<Key,Value>> {

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
