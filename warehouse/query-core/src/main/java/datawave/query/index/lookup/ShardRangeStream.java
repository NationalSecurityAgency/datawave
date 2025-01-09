package datawave.query.index.lookup;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import datawave.data.type.Type;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.lookup.IndexStream.StreamContext;
import datawave.query.iterator.FieldIndexOnlyQueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.errors.ErrorKey;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.util.time.DateHelper;

public class ShardRangeStream extends RangeStream {

    public ShardRangeStream(ShardQueryConfiguration config, ScannerFactory scanners, MetadataHelper helper) {
        super(config, scanners, helper);
    }

    @Override
    public CloseableIterable<QueryPlan> streamPlans(JexlNode node) {
        try {
            String queryString = JexlStringBuildingVisitor.buildQuery(node);

            int stackStart = config.getBaseIteratorPriority() + 40;
            BatchScanner scanner = scanners.newScanner(config.getShardTableName(), config.getAuthorizations(), config.getNumQueryThreads(), config.getQuery(),
                            true);

            IteratorSetting cfg = new IteratorSetting(stackStart++, "query", FieldIndexOnlyQueryIterator.class);

            DefaultQueryPlanner.addOption(cfg, QueryOptions.QUERY_ID, config.getQuery().getId().toString(), false);
            DefaultQueryPlanner.addOption(cfg, QueryOptions.QUERY, queryString, false);

            try {
                DefaultQueryPlanner.addOption(cfg, QueryOptions.INDEX_ONLY_FIELDS,
                                QueryOptions.buildFieldStringFromSet(metadataHelper.getIndexOnlyFields(config.getDatatypeFilter())), true);
                DefaultQueryPlanner.addOption(cfg, QueryOptions.INDEXED_FIELDS,
                                QueryOptions.buildFieldStringFromSet(metadataHelper.getIndexedFields(config.getDatatypeFilter())), true);
            } catch (TableNotFoundException e) {
                throw new RuntimeException(e);
            }

            DefaultQueryPlanner.addOption(cfg, QueryOptions.START_TIME, Long.toString(config.getBeginDate().getTime()), false);
            DefaultQueryPlanner.addOption(cfg, QueryOptions.DATATYPE_FILTER, config.getDatatypeFilterAsString(), false);
            DefaultQueryPlanner.addOption(cfg, QueryOptions.END_TIME, Long.toString(config.getEndDate().getTime()), false);

            configureTypeMappings(config, cfg, metadataHelper);

            scanner.setRanges(Collections.singleton(rangeForTerm(null, null, config)));

            scanner.addScanIterator(cfg);

            Iterator<Entry<Key,Value>> kvIter = scanner.iterator();

            itr = Collections.emptyIterator();

            if (kvIter.hasNext()) {
                PeekingIterator<Entry<Key,Value>> peeking = new PeekingIterator<>(kvIter);
                Entry<Key,Value> peekKey = peeking.peek();
                ErrorKey errorKey = ErrorKey.getErrorKey(peekKey.getKey());
                if (errorKey != null) {
                    switch (errorKey.getErrorType()) {
                        case UNINDEXED_FIELD:
                            this.context = StreamContext.UNINDEXED;
                            break;
                        case UNKNOWN:
                            this.context = StreamContext.ABSENT;
                    }
                } else {
                    itr = Iterators.transform(peeking, new FieldIndexParser(node));
                    this.context = StreamContext.PRESENT;
                }

            } else {
                this.context = StreamContext.ABSENT;

            }

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            // shut down the executor as all threads have completed
            shutdownThreads();
        }

        return this;
    }

    @Override
    public Range rangeForTerm(String term, String field, Date start, Date end) {
        Key startKey = new Key(DateHelper.format(start) + "_");
        Key endKey = new Key(DateHelper.format(end) + "_" + '\uffff');
        return new Range(startKey, false, endKey, false);
    }

    public class FieldIndexParser implements Function<Entry<Key,Value>,QueryPlan> {
        protected JexlNode node;

        public FieldIndexParser(JexlNode node) {
            this.node = node;
        }

        public QueryPlan apply(Entry<Key,Value> entry) {
            Key key = entry.getKey();
            Key start = new Key(key.getRow(), key.getColumnFamily());
            Key end = start.followingKey(PartialKey.ROW_COLFAM);
            Range range = new Range(start, true, end, false);
            //  @formatter:off
            return new QueryPlan()
                            .withTableName(config.getShardTableName())
                            .withQueryTree(node)
                            .withRanges(Collections.singleton(range));
            //  @formatter:on
        }
    }

    /**
     * Lift and shift from DefaultQueryPlanner to avoid reliance on static methods
     */
    private void configureTypeMappings(ShardQueryConfiguration config, IteratorSetting cfg, MetadataHelper metadataHelper) {
        DefaultQueryPlanner.addOption(cfg, QueryOptions.QUERY_MAPPING_COMPRESS, Boolean.toString(true), false);

        Multimap<String,Type<?>> nonIndexedQueryFieldsDatatypes = HashMultimap.create(config.getQueryFieldsDatatypes());
        nonIndexedQueryFieldsDatatypes.keySet().removeAll(config.getIndexedFields());
        String nonIndexedTypes = QueryOptions.buildFieldNormalizerString(nonIndexedQueryFieldsDatatypes);
        DefaultQueryPlanner.addOption(cfg, QueryOptions.NON_INDEXED_DATATYPES, nonIndexedTypes, false);

        try {
            String serializedTypeMetadata = metadataHelper.getTypeMetadata(config.getDatatypeFilter()).toString();
            DefaultQueryPlanner.addOption(cfg, QueryOptions.TYPE_METADATA, serializedTypeMetadata, false);

            String requiredAuthsString = metadataHelper.getUsersMetadataAuthorizationSubset();
            requiredAuthsString = QueryOptions.compressOption(requiredAuthsString, QueryOptions.UTF8);
            DefaultQueryPlanner.addOption(cfg, QueryOptions.TYPE_METADATA_AUTHS, requiredAuthsString, false);
        } catch (TableNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }

        DefaultQueryPlanner.addOption(cfg, QueryOptions.METADATA_TABLE_NAME, config.getMetadataTableName(), false);
    }
}
