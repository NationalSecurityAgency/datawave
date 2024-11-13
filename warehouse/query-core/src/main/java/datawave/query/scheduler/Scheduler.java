package datawave.query.scheduler;

import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.google.common.collect.Lists;

import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.stats.ScanSessionStats;

public abstract class Scheduler implements CloseableIterable<Result> {
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

    public abstract List<QueryCheckpoint> checkpoint(QueryKey queryKey);
}
