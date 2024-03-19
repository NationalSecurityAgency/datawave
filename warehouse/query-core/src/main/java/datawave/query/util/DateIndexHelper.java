package datawave.query.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;

import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.security.util.ScannerHelper;
import datawave.util.StringUtils;

/**
 * <p>
 * Helper class to fetch data from the date index.
 * </p>
 * <table border="1">
 * <caption>DateIndex</caption>
 * <tr>
 * <th>Schema Type</th>
 * <th>Use</th>
 * <th>Row</th>
 * <th>Column Family</th>
 * <th>Column Qualifier</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>date index</td>
 * <td>mapping date to event date/time or shard</td>
 * <td>date (yyyyMMdd)</td>
 * <td>type (e.g. ACTIVITY)</td>
 * <td>date\0datatype\0field (yyyyMMdd event time \0 datatype \0 field name)</td>
 * <td>shard bit string (see java.util.BitSet)</td>
 * </tr>
 * </table>
 *
 *
 *
 */
@Configuration
@EnableCaching
@Component("dateIndexHelper")
public class DateIndexHelper implements ApplicationContextAware {
    private static final Logger log = Logger.getLogger(DateIndexHelper.class);

    public static final String NULL_BYTE = "\0";

    protected AccumuloClient client;

    protected String dateIndexTableName;
    protected Set<Authorizations> auths;
    protected int numQueryThreads;

    protected float collapseDatePercentThreshold;
    protected boolean timeTravel = false;

    protected ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        log.warn("applicationcontextaware setting of applicationContext:" + applicationContext);
    }

    public void setTimeTravel(boolean timeTravel) {
        this.timeTravel = timeTravel;
    }

    public String getDateIndexTableName() {
        return dateIndexTableName;
    }

    public AccumuloClient getClient() {
        return client;
    }

    public Set<Authorizations> getAuths() {
        return auths;
    }

    public int getNumQueryThreads() {
        return numQueryThreads;
    }

    public float getCollapseDatePercentThreshold() {
        return collapseDatePercentThreshold;
    }

    public boolean isTimeTravel() {
        return timeTravel;
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    protected DateIndexHelper() {}

    public static DateIndexHelper getInstance() {
        log.warn("DateIndexHelper created outside of dependency-injection context. This is fine for unit testing, but this is an error in production code");
        if (log.isDebugEnabled())
            log.debug("DateIndexHelper created outside of dependency-injection context. This is fine for unit testing, but this is an error in production code",
                            new Exception("exception for debug purposes"));
        return new DateIndexHelper();
    }

    /**
     * Initializes the instance with a provided update interval.
     *
     * @param client
     *            A client connection to Accumulo
     * @param dateIndexTableName
     *            The name of the date index table
     * @param auths
     *            Any {@link Authorizations} to use
     * @param numQueryThreads
     *            number of query threads
     * @param collapseDatePercentThreshold
     *            the date percent threshold
     * @return the instance
     */
    public DateIndexHelper initialize(AccumuloClient client, String dateIndexTableName, Set<Authorizations> auths, int numQueryThreads,
                    float collapseDatePercentThreshold) {
        if (dateIndexTableName == null || dateIndexTableName.isEmpty()) {
            log.warn("Attempting to create a date index helper however the date index table name is empty");
            return null;
        }

        this.client = client;
        this.dateIndexTableName = dateIndexTableName;
        this.auths = auths;
        this.numQueryThreads = numQueryThreads;
        this.collapseDatePercentThreshold = collapseDatePercentThreshold;

        if (log.isTraceEnabled()) {
            log.trace("Constructor  connector: " + (client != null ? client.getClass().getCanonicalName() : client) + " with auths: " + auths
                            + " and date index table name: " + dateIndexTableName + "; " + numQueryThreads + " threads and " + collapseDatePercentThreshold
                            + " collapse date percent threshold");
        }
        return this;
    }

    public static class DateTypeDescription {
        final Set<String> fields = new HashSet<>();
        final String[] dateRange = new String[2];

        public Set<String> getFields() {
            return fields;
        }

        public String[] getDateRange() {
            return dateRange;
        }

        public Date getBeginDate() {
            try {
                return DateIndexUtil.getBeginDate(dateRange[0]);
            } catch (ParseException pe) {
                log.error("Malformed date in date index table, expected yyyyMMdd: " + dateRange[0], pe);
                throw new IllegalStateException("Malformed date in date index table, expected yyyyMMdd: " + dateRange[0], pe);
            }
        }

        public Date getEndDate() {
            try {
                return DateIndexUtil.getEndDate(dateRange[1]);
            } catch (ParseException pe) {
                log.error("Malformed date in date index table, expected yyyyMMdd: " + dateRange[1], pe);
                throw new IllegalStateException("Malformed date in date index table, expected yyyyMMdd: " + dateRange[1], pe);
            }
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("field: ").append(fields);
            builder.append(", dateRange").append(Arrays.asList(dateRange));
            return builder.toString();
        }
    }

    /**
     * Get the date type description which includes the fields and the mapped date range.
     *
     * @param dateType
     *            date type
     * @param begin
     *            begin date
     * @param end
     *            end date
     * @param datatypeFilter
     *            data type filter
     * @return the date type description
     * @throws TableNotFoundException
     *             if the table is not found
     */
    @Cacheable(value = "getTypeDescription", key = "{#root.target.dateIndexTableName,#root.target.auths,#dateType,#begin,#end,#datatypeFilter}",
                    cacheManager = "dateIndexHelperCacheManager")
    public DateTypeDescription getTypeDescription(String dateType, Date begin, Date end, Set<String> datatypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getTypeDescription(" + dateIndexTableName + ", " + auths + ", " + dateType + ", " + begin + ", " + end + ", "
                        + datatypeFilter + ")");
        if (log.isTraceEnabled()) {
            this.showMeDaCache("before getTypeDescription");
        }
        long startTime = System.currentTimeMillis();

        DateTypeDescription desc = new DateTypeDescription();

        BatchScanner bs = ScannerHelper.createBatchScanner(client, dateIndexTableName, auths, numQueryThreads);
        try {

            // scan from begin to end
            bs.setRanges(Arrays.asList(new Range(DateIndexUtil.format(begin), DateIndexUtil.format(end) + '~')));

            // restrict to our date type
            bs.fetchColumnFamily(new Text(dateType));

            Iterator<Entry<Key,Value>> iterator = bs.iterator();

            while (iterator.hasNext()) {
                Entry<Key,Value> entry = iterator.next();
                Key k = entry.getKey();

                String[] parts = StringUtils.split(k.getColumnQualifier().toString(), '\0');
                if (datatypeFilter == null || datatypeFilter.isEmpty() || datatypeFilter.contains(parts[1])) {
                    desc.fields.add(parts[2]);
                    String date = parts[0];
                    if (desc.dateRange[0] == null) {
                        desc.dateRange[0] = date;
                        desc.dateRange[1] = date;
                    } else {
                        if (date.compareTo(desc.dateRange[0]) < 0) {
                            desc.dateRange[0] = date;
                        }
                        if (date.compareTo(desc.dateRange[1]) > 0) {
                            desc.dateRange[1] = date;
                        }
                    }
                }
            }
        } finally {
            bs.close();
        }

        // if the dates are still empty, then default to the incoming dates
        if (desc.dateRange[0] == null) {
            desc.dateRange[0] = DateIndexUtil.format(begin);
            desc.dateRange[1] = DateIndexUtil.format(end);
        }

        if (log.isDebugEnabled()) {
            long endTime = System.currentTimeMillis();
            log.debug("getTypeDescription from table: " + dateIndexTableName + ", " + auths + ", " + dateType + ", " + begin + ", " + end + ", "
                            + datatypeFilter + " returned " + desc + " in " + (endTime - startTime) + "ms");
        }

        return desc;
    }

    /**
     * Get a comma delimited set of shards and days to be used as the SHARDS_AND_DAYS hint to support to the RangeStream.
     *
     * @param field
     *            Note this is not the datetype, but the specific field
     * @param begin
     *            The begin date for this field
     * @param end
     *            The end date for this field
     * @param rangeBegin
     *            The miniminum shard to search
     * @param rangeEnd
     *            The maximum shard to search
     * @param datatypeFilter
     *            The data type filter
     * @return A string of comma delimited days and shards, order unspecified
     * @throws TableNotFoundException
     *             if the table is not found
     */
    @Cacheable(value = "getShardsAndDaysHint",
                    key = "{#root.target.dateIndexTableName,#root.target.auths,#root.target.collapseDatePercentThreshold,#field,#begin,#end,#rangeBegin,#rangeEnd,#datatypeFilter}",
                    cacheManager = "dateIndexHelperCacheManager")
    public String getShardsAndDaysHint(String field, Date begin, Date end, Date rangeBegin, Date rangeEnd, Set<String> datatypeFilter)
                    throws TableNotFoundException {
        log.debug("cache fault for getShardsAndDaysHint(" + dateIndexTableName + ", " + auths + ", " + collapseDatePercentThreshold + ", " + field + ", "
                        + begin + ", " + end + ", " + rangeBegin + ", " + rangeEnd + ", " + datatypeFilter + ")");
        if (log.isTraceEnabled()) {
            this.showMeDaCache("before getShardsAndDaysHint");
        }
        long startTime = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug("timeTravel is " + this.timeTravel);
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String minShard = format.format(rangeBegin);
        String maxShard = format.format(rangeEnd);

        TreeMap<String,BitSet> bitsets = new TreeMap<>();

        BatchScanner bs = ScannerHelper.createBatchScanner(client, dateIndexTableName, auths, numQueryThreads);

        try {
            bs.setRanges(Arrays.asList(new Range(DateIndexUtil.format(begin), DateIndexUtil.format(end) + '~')));

            Iterator<Entry<Key,Value>> iterator = bs.iterator();

            while (iterator.hasNext()) {
                Entry<Key,Value> entry = iterator.next();
                Key k = entry.getKey();
                String[] parts = StringUtils.split(k.getColumnQualifier().toString(), '\0');
                String date = parts[0];

                // If the event date is more than one day before the event actually happened,
                // then skip it, unless time-travel has been enabled.
                String[] columnFamilyParts = StringUtils.split(k.getColumnFamily().toString(), '\0');
                if (timeTravel == false && columnFamilyParts.length > 0 && columnFamilyParts[0].equals("ACTIVITY")) {
                    try {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                        String rowDateString = k.getRow().toString();
                        // the row may be sharded, i need to get the date part
                        if (rowDateString.contains("_")) {
                            // strip off the shard number part of the rowDate string
                            rowDateString = rowDateString.substring(0, rowDateString.indexOf("_"));
                        }
                        LocalDate rowDate = LocalDate.parse(rowDateString, formatter);
                        LocalDate shardDate = LocalDate.parse(date, formatter);
                        if (shardDate.isBefore(rowDate.minusDays(1))) {
                            continue;
                        }
                    } catch (Exception ex) {
                        // likely a problem parsing the dates, Move on....
                        log.info("problem during delorean check", ex);
                    }
                }

                // If the date is outside of the min and max shard, then continue to the next entry
                if (date.compareTo(minShard) < 0) {
                    continue;
                }
                if (date.compareTo(maxShard) > 0) {
                    continue;
                }

                if (parts[2].equals(field)) {
                    if (datatypeFilter == null || datatypeFilter.isEmpty() || datatypeFilter.contains(parts[1])) {
                        BitSet bits = BitSet.valueOf(entry.getValue().get());
                        BitSet mappedBits = bitsets.get(date);
                        if (mappedBits == null) {
                            bitsets.put(date, bits);
                        } else {
                            mappedBits.or(bits);
                        }
                    }
                }
            }

        } finally {
            bs.close();
        }

        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String,BitSet> entry : bitsets.entrySet()) {
            appendToShardsAndDays(builder, entry.getValue(), entry.getKey());
        }

        String shardsAndDays = builder.toString();

        if (log.isDebugEnabled()) {
            long endTime = System.currentTimeMillis();
            log.debug("getShardsAndDaysHint from table: " + dateIndexTableName + ", " + auths + ", " + collapseDatePercentThreshold + ", " + field + ", "
                            + begin + ", " + end + ", " + rangeBegin + ", " + rangeEnd + ", " + datatypeFilter + " returned " + shardsAndDays + " in "
                            + (endTime - startTime) + "ms");
        }

        return shardsAndDays;
    }

    private void appendToShardsAndDays(StringBuilder builder, BitSet bits, String date) {
        // If the shard density is near 100% (99% or more), then lets assume the entire day.
        log.debug("bits.length():" + bits.length() + ", bits.cardinality():" + bits.cardinality());
        if ((bits.length() > 1) && ((float) (bits.cardinality()) >= (collapseDatePercentThreshold * bits.length()))) {
            if (log.isDebugEnabled()) {
                log.debug("Collapsing shards for " + date + " down to just the date with " + bits.cardinality() + " of " + bits.length() + " shards marked");
            }
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(date);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Appending " + bits.cardinality() + " of " + bits.length() + " shards for " + date);
            }
            StringBuilder shard = new StringBuilder();
            shard.append(date).append('_');
            int baseShardLen = shard.length();

            for (int i = 0; i < bits.length(); i++) {
                if (bits.get(i)) {
                    shard.setLength(baseShardLen);
                    shard.append(i);
                    String shardString = shard.toString();
                    if (builder.length() > 0) {
                        builder.append(',');
                    }
                    builder.append(shardString);
                }
            }
        }
    }

    private void showMeDaCache(String when) {
        log.trace("from applicationContext:" + applicationContext);
        if (this.applicationContext != null) {
            CacheManager cacheManager = applicationContext.getBean("dateIndexHelperCacheManager", CacheManager.class);
            log.trace("beans are " + Arrays.toString(applicationContext.getBeanDefinitionNames()));
            if (cacheManager != null) {
                for (String cacheName : cacheManager.getCacheNames()) {
                    log.trace(when + " got " + cacheName);
                    Object nativeCache = cacheManager.getCache(cacheName).getNativeCache();
                    log.trace("nativeCache is a " + nativeCache);
                    Cache cache = (Cache) nativeCache;
                    Map map = cache.asMap();
                    log.trace("cache map is " + map);
                    log.trace("cache map size is " + map.size());
                    for (Object key : map.keySet()) {
                        log.trace("value for " + key + " is :" + map.get(key));
                    }
                }
            } else {
                log.trace(when + "CacheManager is " + cacheManager);
            }
        }
    }

}
