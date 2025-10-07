package datawave.query.index.stats;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import datawave.core.iterators.filter.CsvKeyFilter;
import datawave.iterators.IteratorSettingHelper;
import datawave.query.Constants;
import datawave.util.TableName;

/**
 * API for getting stats for field names and data types.
 *
 */
public class IndexStatsClient {
    public static final String DEFAULT_STRING = "default";
    public static final Double DEFAULT_VALUE = Double.MAX_VALUE;
    public static final ImmutableMap<String,Double> EMPTY_STATS = ImmutableMap.of(DEFAULT_STRING, DEFAULT_VALUE);

    private static final Logger log = Logger.getLogger(IndexStatsClient.class);

    private AccumuloClient client;
    private String table;
    private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public IndexStatsClient(AccumuloClient client) {
        this(client, TableName.INDEX_STATS);
    }

    public IndexStatsClient(AccumuloClient client, String tableName) {
        this.client = client;
        table = tableName;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setDateFormatter(SimpleDateFormat f) {
        this.dateFormat = new SimpleDateFormat(f.toPattern());
    }

    /**
     * If getStat throws any exceptions, return the default EMPTY_STATS instead of throwing the exception.
     *
     * @param fields
     *            set of fields
     * @param dataTypes
     *            set of datatypes
     * @param start
     *            startdate
     * @param end
     *            enddate
     * @return mapping of stats
     */
    public Map<String,Double> safeGetStat(Set<String> fields, Set<String> dataTypes, Date start, Date end) {
        try {
            return getStat(fields, dataTypes, start, end);
        } catch (Exception e) {
            if (log.isDebugEnabled())
                log.debug("Caught exception running getStat, Returning EMPTY_STATS");
            return EMPTY_STATS;
        }
    }

    public Map<String,Double> getStat(Set<String> fields, Set<String> dataTypes, Date start, Date end) throws IOException {
        if (client.tableOperations().exists(this.table)) {
            TreeSet<String> dates = new TreeSet<>();
            dates.add(dateFormat.format(start));
            dates.add(dateFormat.format(end));
            return getStat(fields, dataTypes, dates);
        } else {
            // If the index stats table doesn't exist, just return the default EMPTY_STATS
            if (log.isDebugEnabled())
                log.debug("Stats table " + this.table + " does not exist.  Returning EMPTY_STATS");
            return EMPTY_STATS;
        }
    }

    public Map<String,Double> getStat(Set<String> fields, Set<String> dataTypes, SortedSet<String> dates) throws IOException {
        final ScannerBase scanner;
        try {
            Authorizations auths = client.securityOperations().getUserAuthorizations(client.whoami());
            if (fields.isEmpty()) {
                scanner = client.createScanner(table, auths);
            } else {
                BatchScanner bScanner = client.createBatchScanner(table, auths, fields.size());
                bScanner.setRanges(buildRanges(fields));
                scanner = bScanner;
            }
        } catch (Exception e) {
            log.error(e);
            throw new IOException(e);
        }

        configureScanIterators(scanner, dataTypes, dates);

        Map<String,Double> results = scanResults(scanner);

        if (scanner instanceof BatchScanner) {
            scanner.close();
        }

        return results;
    }

    public void configureScanIterators(ScannerBase scanner, Collection<String> dataTypes, SortedSet<String> dates) throws IOException {

        if (!dates.isEmpty()) {
            // Filters out sub sections of the column families for me
            IteratorSetting cfg = new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 30, MinMaxIterator.class);
            cfg.addOption(MinMaxIterator.MIN_OPT, dates.first());
            cfg.addOption(MinMaxIterator.MAX_OPT, dates.last());
            scanner.addScanIterator(cfg);
        }

        // only want these data types
        if (!dataTypes.isEmpty()) {
            IteratorSetting cfg = new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 31, CsvKeyFilter.class);
            String dtypesCsv = StringUtils.join(dataTypes, ',');
            log.debug("Filtering on data types: " + (dtypesCsv.isEmpty() ? "none" : dtypesCsv));
            cfg.addOption(CsvKeyFilter.ALLOWED_OPT, dtypesCsv);
            cfg.addOption(CsvKeyFilter.KEY_PART_OPT, "colq");
            scanner.addScanIterator(cfg);
        }

        /*
         * considers the date ranges and datatypes when calculating a weight for a given field
         */
        scanner.addScanIterator(new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 32, IndexStatsCombiningIterator.class));
    }

    public HashMap<String,Double> scanResults(Iterable<Entry<Key,Value>> data) {
        HashMap<String,Double> fieldWeights = new HashMap<>();
        final DoubleWritable vWeight = new DoubleWritable();
        for (Entry<Key,Value> kv : data) {
            if (log.isDebugEnabled()) {
                log.debug("Received key " + kv.getKey().toStringNoTime());
            }
            Text field = kv.getKey().getRow();
            Double weight = null;
            try {
                vWeight.readFields(new DataInputStream(new ByteArrayInputStream(kv.getValue().get())));
                weight = vWeight.get();
            } catch (IOException e) {
                log.error("Could not parse value for " + field, e);
                continue;
            }
            fieldWeights.put(field.toString(), weight);
        }
        return fieldWeights;
    }

    public SortedSet<Range> buildRanges(Collection<String> fields) {
        TreeSet<Range> ranges = new TreeSet<>();
        for (String field : fields) {
            ranges.add(new Range(field, field + Constants.NULL_BYTE_STRING));
        }
        return ranges;
    }
}
