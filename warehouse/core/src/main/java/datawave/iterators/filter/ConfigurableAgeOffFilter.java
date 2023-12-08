package datawave.iterators.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import datawave.ingest.util.cache.ReloadableCacheBuilder;
import datawave.ingest.util.cache.watch.FileRuleWatcher;
import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterRule;
import datawave.util.CompositeTimestamp;

/**
 * This class provides a subclass of the {@code org.apache.accumulo.core.iterators.Filter} class and implements the {@code Option Describer} interface. It
 * allows one to configure a {@code Iterator} to apply many filtering rules, each with it own specific "age off" value. Records {@code (Key, Value)} pairs are
 * aged off if their age is older than the "time to live" ("ttl") specified in with the filtering rules or in the default time to live.
 *
 * <p>
 * The default time to live and its "units" are specified in the options {@code Map<String, String>} object that is passed to the {@code init} method. The
 * default time to live is stored under the key defined by {@code AgeOffConfigParams.TTL}. The units for the default time to live are stored under the key
 * defined by {@code AgeOffConfigParams.TTL_UNITS}, and lastly the configuration filename is stored under the key defined by
 * {@code AgeOffConfigParams.FILTER_CONFIG} {@code AgeOffConfigParams.TTL_SHORT_CIRCUIT} can be optionally used to short circuit invoking the filters and will
 * allow all records younger thatn that interval to be passed through. The units definition is used for both {@code AgeOffConfigParams.TTL} and
 * {@code AgeOffConfigParams.TTL_SHORT_CIRCUIT}.
 *
 *
 * <p>
 * The filtering rules are stored in a configuration file, which may be stored in the local file system, or in HDFS. If it is stored in the local filesystem,
 * then it must be available on all of the tablet servers' filesystems. The configuration file should be specified as a full URL such as
 * {@code file:///opt/accumulo/config/configFilter.xml} or {@code hdfs://config/filters/configFilter.xml}.
 *
 * <p>
 * The TTL Units may be the following values:
 * <ul>
 * <li>{@code ms} - milliseconds
 * <li>{@code s} - seconds
 * <li>{@code m} - minutes
 * <li>{@code h} - hours
 * <li>{@code d} - days
 * </ul>
 *
 * <p>
 * Sample Configuration File:
 *
 * <p>
 *
 * <pre>
 * &lt;ageoffConfiguration&gt;
 *  &lt;rules&gt;
 *      &lt;rule&gt;
 *          &lt;filterClass&gt;
 *          datawave.iterators.filter.ColumnQualifierRegexFilter
 *          &lt;/filterClass&gt;
 *          &lt;ttl units="ms"&gt;
 *          30000
 *          &lt;/ttl&gt;
 *          &lt;matchPattern&gt;
 *          BAZ
 *          &lt;/matchPattern&gt;
 *      &lt;/rule&gt;
 *      &lt;rule&gt;
 *          &lt;filterClass&gt;
 *          datawave.iterators.filter.ColumnQualifierRegexFilter
 *          &lt;/filterClass&gt;
 *          &lt;ttl units="ms"&gt;
 *          60000
 *          &lt;/ttl&gt;
 *          &lt;matchPattern&gt;
 *          ^edge*
 *          &lt;/matchPattern&gt;
 *      &lt;/rule&gt;
 *  &lt;/rules&gt;
 * &lt;/ageoffConfiguration&gt;
 * </pre>
 */
public class ConfigurableAgeOffFilter extends Filter implements OptionDescriber {

    private static final Logger log = Logger.getLogger(ConfigurableAgeOffFilter.class);

    private static final ScheduledThreadPoolExecutor SIMPLE_TIMER = ThreadPools.getServerThreadPools().createScheduledExecutorService(1,
                    ConfigurableAgeOffFilter.class.getSimpleName() + "-ruleCache-refresh", false);

    public static final String UPDATE_INTERVAL_MS_PROP = "tserver.datawave.ageoff.cache.update.interval.ms";
    protected static final long DEFAULT_UPDATE_INTERVAL_MS = 5;
    protected static long UPDATE_INTERVAL_MS = DEFAULT_UPDATE_INTERVAL_MS;

    public static final String EXPIRATION_INTERVAL_MS_PROP = "tserver.datawave.ageoff.cache.expiration.interval.ms";
    protected static final long DEFAULT_EXPIRATION_INTERVAL_MS = 60 * 60 * 1000L; // default 1 hour
    protected static long EXPIRATION_INTERVAL_MS = DEFAULT_EXPIRATION_INTERVAL_MS;

    /**
     * Changed filter list to use FilterRule
     */

    protected static LoadingCache<FileRuleWatcher,Collection<FilterRule>> ruleCache = null;

    protected Collection<AppliedRule> filterList;

    protected long cutOffDateMillis;
    protected long scanStart;

    // this is a time after which we do not need to check any filters.
    protected long shortCircuitDateMillis;

    protected String filename;

    protected static FileSystem fs = null;

    protected IteratorEnvironment myEnv;

    private PluginEnvironment pluginEnv;

    // Adding the ability to disable the filter checks in the case of a system-initialized major compaction for example.
    // The thought is that we force compactions where we want the data to aged off.
    // The system-initialized compactions are on data just imported in which case they are not expected to remove much.
    protected boolean disabled = false;

    public ConfigurableAgeOffFilter() {

    }

    public ConfigurableAgeOffFilter(ConfigurableAgeOffFilter other, IteratorEnvironment env) {

        initialize(other);
    }

    /**
     * This method returns a {@code boolean} value indicating whether or not to allow the {@code (Key, Value)} pair through the filter. A value of {@code true}
     * indicates that he pair should be passed onward through the {@code Iterator} stack, and {@code false} indicates that the {@code (Key, Value)} pair should
     * not be passed on.
     *
     * @param k
     *            {@code Key} object containing the row, column family, and column qualifier.
     * @param v
     *            {@code Value} object containing the value corresponding to the {@code Key: k}
     * @return {@code boolean} value indicating whether or not to allow the {@code Key, Value} through the {@code Filter}.
     */
    @Override
    public boolean accept(Key k, Value v) {

        // if disabled, simple pass through
        if (this.disabled)
            return true;

        // short circuit check
        long timeStamp = CompositeTimestamp.getAgeOffDate(k.getTimestamp());
        if (timeStamp > this.shortCircuitDateMillis)
            return true;

        boolean acceptFlag = false;
        boolean filterRuleApplied = false;

        Iterator<AppliedRule> iter = this.filterList.iterator();

        while ((!filterRuleApplied) && iter.hasNext()) {
            AppliedRule filter = iter.next();
            acceptFlag = filter.accept(k, v);
            filterRuleApplied = filter.isFilterRuleApplied();
        }

        // We went through all of the defined filter rules
        // and none were used, let's apply the default TTL
        if (!filterRuleApplied) {
            acceptFlag = timeStamp > this.cutOffDateMillis;
        }

        return acceptFlag;

    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {

        myEnv = env;
        pluginEnv = env == null ? null : env.getPluginEnv();
        return ((ConfigurableAgeOffFilter) super.deepCopy(env)).initialize(this);
    }

    /**
     * initialize the object via some other configurable age off filter.
     */
    protected ConfigurableAgeOffFilter initialize(ConfigurableAgeOffFilter other) {

        this.disabled = other.disabled;

        this.filterList = Lists.newArrayList(other.filterList);

        this.scanStart = other.scanStart;

        this.cutOffDateMillis = other.cutOffDateMillis;

        this.shortCircuitDateMillis = other.shortCircuitDateMillis;

        this.filename = other.filename;

        return this;
    }

    /**
     * Initialize this object with a set of string parameters representing the configuration options for this iterator.
     *
     * @param ttl
     * @param ttlUnits
     * @param scanStart
     * @param fileName
     * @throws IOException
     * @throws IllegalArgumentException
     */
    protected void initialize(final String ttl, final String ttlUnits, final String ttlShortCircuitStr, final long scanStart, final String fileName)
                    throws IllegalArgumentException, IOException {

        Preconditions.checkNotNull(ttl, "ttl must be set for ConfigurableAgeOffFilter");
        Preconditions.checkNotNull(ttlUnits, "ttlUnits must be set for ConfigurableAgeOffFilter");

        /**
         * initialize filter list.
         */
        this.filterList = Lists.newArrayList();

        /**
         * deal with TTL and TTL Units
         *
         */
        long ttlUnitsFactor = (1000 * 60 * 60 * 24); // (ms per day) default to "days" as the unit.

        if (ttlUnits != null) {
            ttlUnitsFactor = AgeOffPeriod.getTtlUnitsFactor(ttlUnits);
        }

        long ttlShortCircuit = 0;
        if (ttlShortCircuitStr != null) {
            ttlShortCircuit = Long.parseLong(ttlShortCircuitStr);
        }

        /**
         * Deal with scan start.
         */

        this.scanStart = scanStart;

        this.cutOffDateMillis = this.scanStart - ((Long.parseLong(ttl)) * ttlUnitsFactor);

        this.shortCircuitDateMillis = this.scanStart - (ttlShortCircuit * ttlUnitsFactor);

        this.filename = fileName;

        if (filename == null) {
            if (log.isTraceEnabled()) {
                log.trace("Configuration filename not specified for the " + "" + "ConfigurableAgeOffFilter, only the default TTL:" + ttl + " will be used.");
            }
        } else {
            initFilterRules();
        }

        if (log.isTraceEnabled()) {
            log.trace("cutOffDateMillis         = " + cutOffDateMillis);
            log.trace("cutOffDateMillis as Date = " + new Date(cutOffDateMillis));
            log.trace("shortCircuitDateMillis as Date = " + new Date(shortCircuitDateMillis));
            log.trace("scanStart                = " + scanStart);
            log.trace("scanStart as Date        = " + new Date(scanStart));
            log.trace("filename  = " + filename);
            log.trace("Number of filter rules =  " + this.filterList.size());
        }

    }

    /**
     * return true if this is a system initiated majc (a majc that is not a full major compaction)
     *
     * @param env
     */
    private boolean isSystemInitiatedMajC(IteratorEnvironment env) {
        return (env != null && env.getIteratorScope().equals(IteratorUtil.IteratorScope.majc) && !env.isFullMajorCompaction());
    }

    /**
     * Used to initialize the default parameters used by this implementation of {@code Filter}, as well as the sub-filters specified in the configuration file.
     *
     * @param options
     *            {@code Map<String, String>} object contain the configuration parameters for this {@code Filter} implementation. The parameters required are
     *            specified in the {@code AgeOffConfigParams.TTL}, {@code AgeOffConfigParams.TTL_UNITS}, and {@code AgeOffConfigParams.FILTER_CONFIG}.
     * @see org.apache.accumulo.core.iterators.Filter#init(SortedKeyValueIterator, Map, IteratorEnvironment)
     */
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        myEnv = env;
        pluginEnv = env == null ? null : env.getPluginEnv();

        // disabled if this is a system initialized major compaction and we are configured to disable as such
        String disableOnNonFullMajcStr = options.get(AgeOffConfigParams.DISABLE_ON_NON_FULL_MAJC);
        disabled = (disableOnNonFullMajcStr != null) && Boolean.parseBoolean(disableOnNonFullMajcStr) && isSystemInitiatedMajC(env);

        // if disabled, then no need to do any further initialization
        if (disabled) {
            return;
        }

        Preconditions.checkNotNull(options, "Configuration filename and " + "the default ttl must be set for the ConfigurableAgeOffFilter");

        long sessionScanStart = options.containsKey(AgeOffConfigParams.SCAN_START_TIMESTAMP)
                        ? Long.parseLong(options.get(AgeOffConfigParams.SCAN_START_TIMESTAMP))
                        : System.currentTimeMillis();

        initialize(options.get(AgeOffConfigParams.TTL), options.get(AgeOffConfigParams.TTL_UNITS), options.get(AgeOffConfigParams.TTL_SHORT_CIRCUIT),
                        sessionScanStart, options.get(AgeOffConfigParams.FILTER_CONFIG));

    }

    /**
     * This method instantiates the the necessary implementations of the {@code Filter} interface, as they are defined in the configuration file specified by
     * {@code this.filename}.
     *
     * @throws IllegalArgumentException
     *             if there is an error in the configuration file
     * @throws IOException
     */
    private void initFilterRules() throws IllegalArgumentException, IOException {
        // filename
        if (null == ruleCache) {
            synchronized (ConfigurableAgeOffFilter.class) {
                if (null == ruleCache) {
                    UPDATE_INTERVAL_MS = getLongProperty(UPDATE_INTERVAL_MS_PROP, DEFAULT_UPDATE_INTERVAL_MS); // 5 ms
                    EXPIRATION_INTERVAL_MS = getLongProperty(EXPIRATION_INTERVAL_MS_PROP, DEFAULT_EXPIRATION_INTERVAL_MS); // 1 hour
                    ruleCache = CacheBuilder.newBuilder().refreshAfterWrite(UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS)
                                    .expireAfterAccess(EXPIRATION_INTERVAL_MS, TimeUnit.MILLISECONDS).build(new ReloadableCacheBuilder());
                    // this will schedule a check to see if the update or expiration intervals have changed
                    // if so the ruleCache will be rebuilt with these new intervals
                    SIMPLE_TIMER.scheduleWithFixedDelay(() -> {
                        try {
                            long interval = getLongProperty(UPDATE_INTERVAL_MS_PROP, DEFAULT_UPDATE_INTERVAL_MS);
                            long expiration = getLongProperty(EXPIRATION_INTERVAL_MS_PROP, DEFAULT_EXPIRATION_INTERVAL_MS);
                            if (UPDATE_INTERVAL_MS != interval || EXPIRATION_INTERVAL_MS != expiration) {
                                log.info("Changing " + UPDATE_INTERVAL_MS_PROP + " to " + interval);
                                UPDATE_INTERVAL_MS = interval;
                                log.info("Changing " + EXPIRATION_INTERVAL_MS_PROP + " to " + expiration);
                                EXPIRATION_INTERVAL_MS = expiration;
                                ruleCache = CacheBuilder.newBuilder().refreshAfterWrite(UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS)
                                                .expireAfterAccess(EXPIRATION_INTERVAL_MS, TimeUnit.MILLISECONDS).build(new ReloadableCacheBuilder());
                            }
                        } catch (Throwable t) {
                            log.error(t, t);
                        }
                    }, 1, 10, TimeUnit.SECONDS);
                }
            }
        }

        Path filePath = new Path(filename);
        if (null == fs) {
            synchronized (ConfigurableAgeOffFilter.class) {
                if (null == fs) {
                    if (log.isTraceEnabled()) {
                        log.trace("Setting FileSystem reference");
                    }
                    fs = filePath.getFileSystem(new Configuration());
                }
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace("Reusing file system reference.");
            }
        }
        FileRuleWatcher watcherKey = new FileRuleWatcher(fs, filePath, 1, myEnv);

        copyRules(watcherKey);

    }

    private long getLongProperty(final String prop, final long defaultValue) {
        if (pluginEnv != null && pluginEnv.getConfiguration() != null) {
            String propValue = pluginEnv.getConfiguration().get(prop);
            if (propValue != null) {
                return Long.parseLong(propValue);
            }
        }
        return defaultValue;
    }

    protected void copyRules(FileRuleWatcher watcherKey) throws IOException {
        filterList = new ArrayList<>();
        try {
            // rule cache is lazily loaded, so the act of getting the key will populate it with the key
            // and trigger a bunch of loading logic which will ultimately call
            // FileRuleWatcher.loadContents() which will return the rules
            Collection<FilterRule> rules = ruleCache.get(watcherKey);

            if (rules != null) {
                for (FilterRule rule : rules) {
                    // NOTE: this propagates the anchor time (scanStart) to all of the applied rules
                    // This is used to calculate the AgeOffPeriod for all of the rules
                    filterList.add((AppliedRule) rule.deepCopy(this.scanStart));
                }
            }

        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    /**
     * This method is used by accumulo and its command line shell to prompt the user for the configuration options for this {@code Filter}.
     *
     * @return {@code IteratorOptions} object listing the option names and "help information" on each of them.
     */
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new TreeMap<>();
        options.put(AgeOffConfigParams.TTL, "Default time to live.");
        options.put(AgeOffConfigParams.TTL_UNITS, "Default time to live units. (" + AgeOffTtlUnits.DAYS + ", " + AgeOffTtlUnits.HOURS + ", "
                        + AgeOffTtlUnits.MINUTES + ", " + AgeOffTtlUnits.SECONDS + ", or " + AgeOffTtlUnits.MILLISECONDS + "[default = d])");
        options.put(AgeOffConfigParams.TTL_SHORT_CIRCUIT, "Interval after which no data is aged off allowing this filter to bypass calling the filters");
        options.put(AgeOffConfigParams.FILTER_CONFIG, "URL to the age off filter configuration file.");
        options.put(AgeOffConfigParams.DISABLE_ON_NON_FULL_MAJC,
                        "If set to 'true', then filters will be disabled for system-initialized full major compactions (non-full majc)");
        return new IteratorOptions("cfgAgeoff", "ConfigurableAgeOffFilter removes entries with timestamps more than <ttl> milliseconds old", options, null);
    }

    /**
     * This method is used by accumulo and its command line shell to validate values provided by the user for the configuration options for this {@code Filter}.
     *
     * @param options
     *            {@code Map<String, String>} object contain the configuration parameters for this {@code Filter} implementation. The parameters required are
     *            specified in the {@code AgeOffConfigParams.TTL}, {@code AgeOffConfigParams.TTL_UNITS}, and {@code AgeOffConfigParams.FILTER_CONFIG}.
     * @return {@code boolean} value indicating success ({@code true}) or failure ({@code false})
     */
    @Override
    public boolean validateOptions(Map<String,String> options) {
        try {
            Integer.parseInt(options.get(AgeOffConfigParams.TTL));
        } catch (NumberFormatException nfe) {
            log.error("Error initializing ConfigurableAgeOffFilter: invalid <ttl> value:" + options.get(AgeOffConfigParams.TTL));
            return false;

        }

        if (options.containsKey(AgeOffConfigParams.TTL_SHORT_CIRCUIT)) {
            try {
                Integer.parseInt(options.get(AgeOffConfigParams.TTL_SHORT_CIRCUIT));
            } catch (NumberFormatException nfe) {
                log.error("Error initializing ConfigurableAgeOffFilter: invalid <ttlShortCircuit> value:" + options.get(AgeOffConfigParams.TTL_SHORT_CIRCUIT));
                return false;
            }
        }

        String ttlUnits = options.get(AgeOffConfigParams.TTL_UNITS);
        // @formatter:off
        List<String> allUnits = Arrays.asList(
            AgeOffTtlUnits.DAYS, AgeOffTtlUnits.HOURS, AgeOffTtlUnits.MINUTES, AgeOffTtlUnits.SECONDS, AgeOffTtlUnits.MILLISECONDS);
        // @formatter:on
        return (ttlUnits != null) && allUnits.contains(ttlUnits);
    }

    /**
     * Clear the file watcher cache.
     */
    public static void clearCache() {
        if (null != ruleCache) {
            ruleCache.invalidateAll();
            ruleCache.cleanUp();
        }
    }

    public static LoadingCache<FileRuleWatcher,Collection<FilterRule>> getCache() {
        return ruleCache;
    }

    @Override
    public String toString() {
        return "ConfigurableAgeOffFilter [ default cutOffDateMillis=" + cutOffDateMillis + ", filename=" + filename + ", filterList=[" + filterList + "]]";
    }

}
