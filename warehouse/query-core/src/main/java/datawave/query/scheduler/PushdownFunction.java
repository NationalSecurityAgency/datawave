package datawave.query.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import datawave.core.query.configuration.QueryData;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.QueryOptions;
import datawave.query.tables.BatchScannerSession;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.ScannerChunk;

/**
 * The PushdownFunction transforms the QueryData coming off the range stream into ScannerChunks which are used by the {@link BatchScannerSession}
 * <p>
 * The single most important thing this function does is update the {@link IteratorSetting} with the query that was (potentially) pruned by the range stream
 * (global index lookup).
 */
public class PushdownFunction implements Function<QueryData,List<ScannerChunk>> {

    private static final Logger log = Logger.getLogger(PushdownFunction.class);

    /**
     * The shard query config
     */
    private final ShardQueryConfiguration config;
    // any custom settings
    protected Collection<IteratorSetting> customSettings;
    // table id, used to apply execution hints
    protected TableId tableId;

    @Deprecated
    public PushdownFunction(TabletLocator tabletLocator, ShardQueryConfiguration config, Collection<IteratorSetting> settings, TableId tableId) {
        this(config, settings, tableId);
    }

    /**
     * Preferred constructor
     *
     * @param config
     *            the config
     * @param settings
     *            the iterator settings
     * @param tableId
     *            the table id
     */
    public PushdownFunction(ShardQueryConfiguration config, Collection<IteratorSetting> settings, TableId tableId) {
        this.config = config;
        this.customSettings = settings;
        this.tableId = tableId;
    }

    /**
     * Transforms a {@link QueryData} into a list of {@link ScannerChunk}s. Most of the time this will be a singleton list.
     *
     * @param qd
     *            the query data
     * @return a list of scanner chunks
     */
    public List<ScannerChunk> apply(QueryData qd) {
        List<ScannerChunk> chunks = Lists.newArrayList();
        for (Range range : qd.getRanges()) {

            SessionOptions options = new SessionOptions();
            options.setQueryConfig(this.config);

            // fetch column families
            for (String cf : qd.getColumnFamilies()) {
                options.fetchColumnFamily(new Text(cf));
            }

            // apply custom scan iterators
            for (IteratorSetting setting : customSettings) {
                options.addScanIterator(setting);
            }

            // apply execution hints
            String tableName = tableId.canonical();
            options.applyExecutionHints(tableName, config.getTableHints());
            options.applyConsistencyLevel(tableName, config.getTableConsistencyLevels());

            // copy over all iterator settings
            List<IteratorSetting> newSettings = new ArrayList<>();
            for (IteratorSetting setting : qd.getSettings()) {
                IteratorSetting newSetting = new IteratorSetting(setting.getPriority(), setting.getName(), setting.getIteratorClass());
                newSetting.addOptions(setting.getOptions());

                // very important: update the session options QUERY with the potentially pruned query
                if (newSetting.getOptions().containsKey(QueryOptions.QUERY)) {
                    newSetting.addOption(QueryOptions.QUERY, qd.getQuery());
                }
                newSettings.add(newSetting);
            }

            // now update the sessions options with the freshly copied settings
            for (IteratorSetting setting : newSettings) {
                options.addScanIterator(setting);
            }

            chunks.add(new ScannerChunk(options, Collections.singleton(range), qd, ""));
        }
        return chunks;
    }
}
