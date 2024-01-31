package datawave.query.tables.shard;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import datawave.core.iterators.filter.CsvKeyFilter;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.stats.IndexStatsRecord;
import datawave.query.index.stats.IndexStatsSummingIterator;
import datawave.query.index.stats.MinMaxIterator;
import datawave.security.util.ScannerHelper;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.AbstractQueryLogicTransformer;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.result.istat.FieldStat;
import datawave.webservice.query.result.istat.IndexStatsResponse;
import datawave.webservice.result.BaseQueryResponse;

public class IndexStatsQueryLogic extends BaseQueryLogic<FieldStat> {
    private static final Logger log = Logger.getLogger(IndexStatsQueryLogic.class);

    private AccumuloClient client;

    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }

    /**
     * The QL iterator returns FieldStats, so we don't have to transform anything
     */
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new AbstractQueryLogicTransformer<Object,Object>() {

            @Override
            public Object transform(Object input) {
                return input;
            }

            @Override
            public BaseQueryResponse createResponse(List<Object> resultList) {
                IndexStatsResponse resp = new IndexStatsResponse();

                for (Object o : resultList) {
                    resp.addFieldStat((FieldStat) o);
                }

                return resp;
            }

        };
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query query, Set<Authorizations> auths) throws Exception {
        this.client = client;

        ShardQueryConfiguration config = new ShardQueryConfiguration();

        // Get the datatype set if specified
        String typeList = query.findParameter(QueryParameters.DATATYPE_FILTER_SET).getParameterValue();
        HashSet<String> typeFilter = null;

        if (null != typeList && !typeList.isEmpty()) {
            typeFilter = new HashSet<>();
            typeFilter.addAll(Arrays.asList(StringUtils.split(typeList, Constants.PARAM_VALUE_SEP)));

            if (!typeFilter.isEmpty()) {
                config.setDatatypeFilter(typeFilter);

                if (log.isDebugEnabled()) {
                    log.debug("Type Filter: " + typeFilter);
                }
            }
        }

        config.setBeginDate(query.getBeginDate());
        config.setEndDate(query.getEndDate());
        config.setQueryString(query.getQuery());
        config.setAuthorizations(auths);

        return config;
    }

    @Override
    public void setupQuery(GenericQueryConfiguration config) throws Exception {
        ShardQueryConfiguration qConf = (ShardQueryConfiguration) config;

        HashSet<String> fields = new HashSet<>();
        Collections.addAll(fields, config.getQueryString().split(" "));

        StatsMonkey monkey = new StatsMonkey();
        monkey.client = client;
        monkey.table = TableName.INDEX_STATS;
        List<FieldStat> stats = monkey.getStat(fields, qConf.getDatatypeFilter(), qConf.getBeginDate(), qConf.getEndDate());
        this.iterator = stats.iterator();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new IndexStatsQueryLogic();
    }

    private static class StatsMonkey {
        AccumuloClient client;
        String table;

        public List<FieldStat> getStat(Set<String> fields, Set<String> dataTypes, Date start, Date end) throws IOException {
            TreeSet<String> dates = new TreeSet<>();
            dates.add(DateHelper.format(start));
            dates.add(DateHelper.format(end));
            return getStat(fields, dataTypes, dates);
        }

        public List<FieldStat> getStat(Set<String> fields, Set<String> dataTypes, SortedSet<String> dates) throws IOException {
            // To allow "fields" to be empty and configure the scanners the same,
            // I have to have to references to the Scanner implementation because
            // ScannerBase does not implement Iterable<Entry<Key, Value>>
            final ScannerBase scanner;
            final Iterable<Entry<Key,Value>> dataSource;
            try {
                Set<Authorizations> auths = Collections.singleton(client.securityOperations().getUserAuthorizations(client.whoami()));
                if (fields.isEmpty()) {
                    Scanner simpleScanner = ScannerHelper.createScanner(client, table, auths);
                    dataSource = simpleScanner;
                    scanner = simpleScanner;
                } else {
                    BatchScanner bScanner = ScannerHelper.createBatchScanner(client, table, auths, fields.size());
                    bScanner.setRanges(buildRanges(fields));
                    scanner = bScanner;
                    dataSource = bScanner;
                }
            } catch (Exception e) {
                log.error(e);
                throw new IOException(e);
            }

            configureScanIterators(scanner, dataTypes, dates);

            List<FieldStat> results = scanResults(dataSource);

            if (scanner instanceof BatchScanner) {
                scanner.close();
            }

            return results;
        }

        public void configureScanIterators(ScannerBase scanner, Collection<String> dataTypes, SortedSet<String> dates) throws IOException {

            if (!dates.isEmpty()) {
                // Filters out sub sections of the column families for me
                IteratorSetting cfg = new IteratorSetting(30, "mmi", MinMaxIterator.class);
                cfg.addOption(MinMaxIterator.MIN_OPT, dates.first());
                cfg.addOption(MinMaxIterator.MAX_OPT, dates.last());
                scanner.addScanIterator(cfg);
            }

            // only want these data types
            if (!dataTypes.isEmpty()) {
                String dtypesCsv = StringUtils.join(dataTypes, ',');
                log.debug("Filtering on data types: " + (dtypesCsv.isEmpty() ? "none" : dtypesCsv));
                IteratorSetting cfg = new IteratorSetting(31, "fi", CsvKeyFilter.class);
                cfg.addOption(CsvKeyFilter.ALLOWED_OPT, dtypesCsv);
                cfg.addOption(CsvKeyFilter.KEY_PART_OPT, "colq");
                scanner.addScanIterator(cfg);
            }

            /*
             * considers the date ranges and datatypes when calculating a weight for a given field
             */
            scanner.addScanIterator(new IteratorSetting(32, "issi", IndexStatsSummingIterator.class));
        }

        public LinkedList<FieldStat> scanResults(Iterable<Entry<Key,Value>> data) {
            LinkedList<FieldStat> stats = new LinkedList<>();
            IndexStatsRecord tuple = new IndexStatsRecord();
            for (Entry<Key,Value> kv : data) {
                if (log.isDebugEnabled()) {
                    log.debug("Received key " + kv.getKey().toStringNoTime());
                }
                String field = kv.getKey().getRow().toString();
                long unique, total;
                double selectivity;
                try {
                    tuple.readFields(new DataInputStream(new ByteArrayInputStream(kv.getValue().get())));
                } catch (IOException e) {
                    log.error("Could not parse value for " + field, e);
                    continue;
                }
                unique = tuple.getNumberOfUniqueWords().get();
                total = tuple.getWordCount().get();
                selectivity = ((double) unique) / ((double) total);

                FieldStat fs = new FieldStat();
                fs.field = field;
                fs.unique = unique;
                fs.observed = total;
                fs.selectivity = selectivity;
                stats.add(fs);
            }
            return stats;
        }

        public SortedSet<Range> buildRanges(Collection<String> fields) {
            TreeSet<Range> ranges = new TreeSet<>();
            for (String field : fields) {
                ranges.add(new Range(field, field + Constants.NULL_BYTE_STRING));
            }
            return ranges;
        }
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.add(QueryParameters.DATATYPE_FILTER_SET);
        return params;
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }

}
