package datawave.query.tables;

import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.lookup.DataTypeFilter;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.TruncatedIndexIterator;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.Tuple2;
import datawave.util.time.DateHelper;

/**
 * Wraps an {@link TruncatedIndexIterator} with the context of a query's date range. Transforms the aggregated bitset that denotes shard offsets to present a
 * shard-specific view of the world to the caller.
 */
public class TruncatedIndexScanner implements Iterator<Tuple2<String,IndexInfo>> {

    private static final Logger log = LoggerFactory.getLogger(TruncatedIndexScanner.class);

    private String value;
    private String field;
    private Set<String> datatypes = null;

    private final AccumuloClient client;
    private Authorizations auths;
    private String tableName;

    private Text columnFamily;
    private String currentDay;
    private int basePriority = 30;

    private Tuple2<String,IndexInfo> top = null;

    private final DateIterator dateIterator;
    private final BitSetIterator bitsetIterator;

    public TruncatedIndexScanner(ShardQueryConfiguration config) {
        this(config.getClient(), DateHelper.format(config.getBeginDate()), DateHelper.format(config.getEndDate()));
    }

    public TruncatedIndexScanner(AccumuloClient client, String startDate, String endDate) {
        this.client = client;
        this.dateIterator = new DateIterator(startDate, endDate);
        this.bitsetIterator = new BitSetIterator(null);
    }

    public void setFieldValue(String field, String value) {
        this.field = field;
        this.value = value;
        this.columnFamily = new Text(field);
    }

    public void setAuths(Authorizations auths) {
        this.auths = auths;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setDatatypes(Set<String> datatypes) {
        this.datatypes = datatypes;
    }

    public void setBasePriority(int basePriority) {
        this.basePriority = basePriority;
    }

    @Override
    public boolean hasNext() {
        while (top == null) {

            if (currentDay == null) {
                currentDay = dateIterator.next();

                BitSet bitset = scanNextDay(currentDay);
                if (bitset == null || bitset.cardinality() == 0) {
                    currentDay = null; // no data found, advance to next day
                } else {
                    bitsetIterator.reset(bitset);
                }
            }

            if (bitsetIterator.hasNext()) {
                int index = bitsetIterator.next();
                if (currentDay != null && index != -1) {
                    String shard = currentDay + "_" + index;
                    top = createIndexInfo(shard);
                }
            } else {
                currentDay = null;
            }

            if (currentDay == null && !dateIterator.hasNext() && !bitsetIterator.hasNext()) {
                break;
            }
        }
        return top != null;
    }

    /**
     * Create a new Tuple using the provided shard. A new object must be created for each top value.
     *
     * @param shard
     *            the current shard
     * @return a new Tuple of shard and index info
     */
    private Tuple2<String,IndexInfo> createIndexInfo(String shard) {
        IndexInfo info = new IndexInfo(-1);
        info.applyNode(JexlNodeFactory.buildEQNode(field, value));
        return new Tuple2<>(shard, info);
    }

    @Override
    public Tuple2<String,IndexInfo> next() {
        Tuple2<String,IndexInfo> next = top;
        top = null;
        return next;
    }

    private BitSet scanNextDay(String date) {
        Objects.requireNonNull(tableName, "must set the index table name");
        Objects.requireNonNull(auths, "authorizations must be set");
        try (var scanner = client.createScanner(tableName, auths)) {

            Range range = createScanRange(value, field, date);
            scanner.setRange(range);
            scanner.fetchColumnFamily(columnFamily);

            if (datatypes != null && !datatypes.isEmpty()) {
                scanner.addScanIterator(createDatatypeFilter(basePriority + 1));
            }
            scanner.addScanIterator(createScanIterator(basePriority + 2));

            BitSet bitset = null;
            int count = 0;
            for (Map.Entry<Key,Value> entry : scanner) {
                count++;
                if (bitset == null) {
                    bitset = BitSet.valueOf(entry.getValue().get());
                } else {
                    BitSet result = BitSet.valueOf(entry.getValue().get());
                    bitset.or(result);
                }
            }

            if (count > 1) {
                // the TruncatedIndexIterator should never return more than one entry returned
                log.error("found more than one entry: {}", count);
            }

            return bitset;
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private Range createScanRange(String value, String field, String date) {
        Key start = new Key(value, field, date + "\u0000");
        Key stop = new Key(value, field, date + "\u0000\uFFFF");
        return new Range(start, true, stop, true);
    }

    private IteratorSetting createDatatypeFilter(int priority) {
        IteratorSetting setting = new IteratorSetting(priority, DataTypeFilter.class);
        setting.addOption(DataTypeFilter.TYPES, Joiner.on(',').join(datatypes));
        return setting;
    }

    private IteratorSetting createScanIterator(int priority) {
        return new IteratorSetting(priority, TruncatedIndexIterator.class.getSimpleName(), TruncatedIndexIterator.class);
    }

}
