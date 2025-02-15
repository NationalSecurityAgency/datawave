package datawave.next;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;

import datawave.core.query.configuration.QueryData;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.QueryOptions;
import datawave.query.scheduler.PushdownFunction;
import datawave.query.scheduler.PushdownScheduler;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.util.MetadataHelperFactory;

public class CountScheduler extends PushdownScheduler {

    public CountScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory helperFactory) {
        super(config, scannerFactory, helperFactory);
    }

    @Override
    public PushdownFunction getPushdownFunction() {
        return new CountPushdownFunction(config, settings, tableId);
    }

    public class CountPushdownFunction extends PushdownFunction {

        public CountPushdownFunction(ShardQueryConfiguration config, Collection<IteratorSetting> settings, TableId tableId) {
            super(config, settings, tableId);
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
                options.setQueryConfig(getConfig());

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

                // in this case we construct entirely new iterator settings

                IteratorSetting existing = qd.getSettings().get(0);

                IteratorSetting count = new IteratorSetting(existing.getPriority(), "CountIterator", CountQueryIterator.class);
                count.addOption(QueryOptions.QUERY, qd.getQuery()); // always use the (potentially) pruned query
                if (existing.getOptions().containsKey(QueryOptions.DATATYPE_FILTER)) {
                    count.addOption(QueryOptions.DATATYPE_FILTER, existing.getOptions().get(QueryOptions.DATATYPE_FILTER));
                }
                count.addOption(QueryOptions.START_TIME, existing.getOptions().get(QueryOptions.START_TIME));
                count.addOption(QueryOptions.END_TIME, existing.getOptions().get(QueryOptions.END_TIME));
                count.addOption(QueryOptions.INDEXED_FIELDS, existing.getOptions().get(QueryOptions.INDEXED_FIELDS));

                // now update the sessions options with the new settings
                options.addScanIterator(count);

                chunks.add(new ScannerChunk(options, Collections.singleton(range), qd, ""));
            }
            return chunks;
        }
    }
}
