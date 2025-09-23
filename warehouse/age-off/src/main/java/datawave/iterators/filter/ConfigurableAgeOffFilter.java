package datawave.iterators.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import datawave.ingest.util.cache.watch.FileRuleCacheLoader;
import datawave.ingest.util.cache.watch.FileRuleCacheValue;
import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.AppliedRule;
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
 * </p>
 *
 * <p>
 * The filtering rules are stored in a configuration file, which may be stored in the local file system, or in HDFS. If it is stored in the local filesystem,
 * then it must be available on all of the tablet servers' filesystems. The configuration file should be specified as a full URL such as
 * {@code file:///opt/accumulo/config/configFilter.xml} or {@code hdfs://config/filters/configFilter.xml}.
 * </p>
 *
 * <p>
 * The TTL Units may be the following values:
 * </p>
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
 * </p>
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

    private static final ThreadFactory TIMER_THREAD_FACTORY = new ThreadFactoryBuilder()
                    .setNameFormat(ConfigurableAgeOffFilter.class.getSimpleName() + "-ruleCache-refresh-%d").build();

    private static final ScheduledExecutorService SIMPLE_TIMER = Executors.newSingleThreadScheduledExecutor(TIMER_THREAD_FACTORY);
    public static final String UPDATE_INTERVAL_MS_PROP = "tserver.datawave.ageoff.cache.update.interval.ms";
    protected static final long DEFAULT_UPDATE_INTERVAL_MS = 5;
    protected static long UPDATE_INTERVAL_MS = DEFAULT_UPDATE_INTERVAL_MS;

    public static final String EXPIRATION_INTERVAL_MS_PROP = "tserver.datawave.ageoff.cache.expiration.interval.ms";
    protected static final long DEFAULT_EXPIRATION_INTERVAL_MS = 60 * 60 * 1000L; // default 1 hour
    protected static long EXPIRATION_INTERVAL_MS = DEFAULT_EXPIRATION_INTERVAL_MS;

    protected static volatile LoadingCache<String,FileRuleCacheValue> ruleCache = null;

    protected Collection<AppliedRule> filterList;

    protected long cutOffDateMillis;
    protected long scanStart;

    // this is a time after which we do not need to check any filters.
    protected long shortCircuitDateMillis;

    protected String filename;

    protected IteratorEnvironment myEnv;

    protected PluginEnvironment pluginEnv;

    protected FileRuleCacheValue cacheValue;

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
     *
     * @param other
     *            another filter to base this one off
     *
     * @return the configurable age off filter
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
     *            time to live
     * @param ttlUnits
     *            time to live units
     *
     * @param ttlShortCircuitStr
     *            time to live short circuit string
     * @param scanStart
     *            scan start time
     * @param fileName
     *            file name
     * @throws IOException
     *             if error reading the file
     * @throws IllegalArgumentException
     *             if illegal arguments passed
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
     * Used to initialize the default parameters used by this implementation of {@code Filter}, as well as the sub-filters specified in the configuration file.
     *
     * @param source
     *            the source key values
     * @param options
     *            {@code Map<String, String>} object contain the configuration parameters for this {@code Filter} implementation. The parameters required are
     *            specified in the {@code AgeOffConfigParams.TTL}, {@code AgeOffConfigParams.TTL_UNITS}, and {@code AgeOffConfigParams.FILTER_CONFIG}.
     * @param env
     *            the iterator environment
     * @see org.apache.accumulo.core.iterators.Filter#init(SortedKeyValueIterator, Map, IteratorEnvironment)
     */
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        myEnv = env;
        pluginEnv = env == null ? null : env.getPluginEnv();
        disabled = shouldDisableForNonFullCompaction(options, env) || shouldDisableForNonUserCompaction(options, env);

        Preconditions.checkNotNull(options, "Configuration filename and " + "the default ttl must be set for the ConfigurableAgeOffFilter");

        long sessionScanStart = options.containsKey(AgeOffConfigParams.SCAN_START_TIMESTAMP)
                        ? Long.parseLong(options.get(AgeOffConfigParams.SCAN_START_TIMESTAMP))
                        : System.currentTimeMillis();
        initialize(options.get(AgeOffConfigParams.TTL), options.get(AgeOffConfigParams.TTL_UNITS), options.get(AgeOffConfigParams.TTL_SHORT_CIRCUIT),
                        sessionScanStart, options.get(AgeOffConfigParams.FILTER_CONFIG));

    }

    /**
     * enabled if any of the following are true:
     * <ul>
     * <li>we're not configured to disable non-full majcs</li>
     * <li>this is not a major compaction</li>
     * <li>we're doing a full majc compaction</li>
     * </ul>
     *
     * @param options
     *            map of options
     * @param env
     *            the iterator environment
     * @return true only if we should disable filtering
     */
    private boolean shouldDisableForNonFullCompaction(Map<String,String> options, IteratorEnvironment env) {
        if (!validatePropertyIsBoolean(options, AgeOffConfigParams.DISABLE_ON_NON_FULL_MAJC)) {
            throw new IllegalArgumentException(
                            "Invalid for " + AgeOffConfigParams.DISABLE_ON_NON_FULL_MAJC + ": " + options.get(AgeOffConfigParams.DISABLE_ON_NON_FULL_MAJC));
        }

        // if the configuration property is missing, we should apply the filter
        if (!options.containsKey(AgeOffConfigParams.DISABLE_ON_NON_FULL_MAJC)) {
            return false;
        }

        // if the property is set to false, we should apply the filter
        if (!Boolean.parseBoolean(options.get(AgeOffConfigParams.DISABLE_ON_NON_FULL_MAJC))) {
            return false;
        }

        // if this isn't a major compaction, we should apply the filter
        if (env == null || !env.getIteratorScope().equals(IteratorUtil.IteratorScope.majc)) {
            return false;
        }

        if (env.isFullMajorCompaction()) {
            return false;
        }

        return true;
    }

    /**
     * enabled if any of the following are true:
     * <ul>
     * <li>we're not configured to disable non-user majcs</li>
     * <li>this is not a major compaction</li>
     * <li>we're doing a user majc compaction</li>
     * </ul>
     *
     * @param options
     *            map of options
     * @param env
     *            the iterator environment
     * @return true only if we should disable filtering
     */
    private boolean shouldDisableForNonUserCompaction(Map<String,String> options, IteratorEnvironment env) {
        if (!validatePropertyIsBoolean(options, AgeOffConfigParams.ONLY_ON_USER_COMPACTION)) {
            throw new IllegalArgumentException(
                            "Invalid for " + AgeOffConfigParams.ONLY_ON_USER_COMPACTION + ": " + options.get(AgeOffConfigParams.ONLY_ON_USER_COMPACTION));
        }

        // if the configuration property is missing, we should apply the filter
        if (!options.containsKey(AgeOffConfigParams.ONLY_ON_USER_COMPACTION)) {
            return false;
        }

        // if the property is set to false, we should apply the filter
        if (!Boolean.parseBoolean(options.get(AgeOffConfigParams.ONLY_ON_USER_COMPACTION))) {
            return false;
        }

        // if this isn't a major compaction, we should apply the filter
        if (env == null || !env.getIteratorScope().equals(IteratorUtil.IteratorScope.majc)) {
            return false;
        }

        if (env.isUserCompaction()) {
            return false;
        }

        return true;
    }

    /**
     * This method instantiates the necessary implementations of the {@code Filter} interface, as they are defined in the configuration file specified by
     * {@code this.filename}.
     *
     * @throws IllegalArgumentException
     *             if there is an error in the configuration file
     * @throws IOException
     *             if there is an error reading the configuration file
     */
    private void initFilterRules() throws IllegalArgumentException, IOException {
        if (null == ruleCache) {
            synchronized (ConfigurableAgeOffFilter.class) {
                if (null == ruleCache) {
                    UPDATE_INTERVAL_MS = getLongProperty(UPDATE_INTERVAL_MS_PROP, DEFAULT_UPDATE_INTERVAL_MS); // 5 ms
                    EXPIRATION_INTERVAL_MS = getLongProperty(EXPIRATION_INTERVAL_MS_PROP, DEFAULT_EXPIRATION_INTERVAL_MS); // 1 hour
                    log.debug("Configured refresh interval (ms): " + UPDATE_INTERVAL_MS);
                    log.debug("Configured expiration interval (ms): " + EXPIRATION_INTERVAL_MS);
                    ruleCache = CacheBuilder.newBuilder().refreshAfterWrite(UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS)
                                    .expireAfterAccess(EXPIRATION_INTERVAL_MS, TimeUnit.MILLISECONDS).build(new FileRuleCacheLoader());
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
                                                .expireAfterAccess(EXPIRATION_INTERVAL_MS, TimeUnit.MILLISECONDS).build(new FileRuleCacheLoader());
                            }
                        } catch (Throwable t) {
                            log.error(t, t);
                        }
                    }, 1, 10, TimeUnit.SECONDS);
                }
            }
        }

        try {
            cacheValue = ruleCache.get(filename);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }

        // the rule cache value will be static
        // initial load of the baseline rules will occur here, that call may take time
        // after initial load, each subsequent initialize performs a deep copy (init) against the AppliedRule
        // the internal deep copy operation (i.e. calls after initial load) are anticipated to be quick
        filterList = cacheValue.newRulesetView(scanStart, myEnv);
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
                        "If set to 'true', then filters will be disabled for system-initialized full major compactions (non-full majc).  Deprecated.  Use "
                                        + AgeOffConfigParams.ONLY_ON_USER_COMPACTION);
        options.put(AgeOffConfigParams.ONLY_ON_USER_COMPACTION,
                        "If set to 'true' then filters will only be used for user-initiated major compactions and not system initiated ones. [default = false]");
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

        if (!validatePropertyIsBoolean(options, AgeOffConfigParams.ONLY_ON_USER_COMPACTION)) {
            return false;
        }

        // @formatter:off
        List<String> allUnits = Arrays.asList(
            AgeOffTtlUnits.DAYS, AgeOffTtlUnits.HOURS, AgeOffTtlUnits.MINUTES, AgeOffTtlUnits.SECONDS, AgeOffTtlUnits.MILLISECONDS);
        // @formatter:on
        return (ttlUnits != null) && allUnits.contains(ttlUnits);
    }

    private boolean validatePropertyIsBoolean(Map<String,String> options, String propertyName) {
        if (options.containsKey(propertyName)) {
            String propertyValue = options.get(propertyName);
            if (!"true".equals(propertyValue) && !"false".equals(propertyValue)) {
                log.error(propertyName + " was present, but not a valid boolean." + " Value was: " + propertyValue);
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    FileRuleCacheValue getFileRuleCacheValue() {
        return cacheValue;
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

    public static LoadingCache<String,FileRuleCacheValue> getCache() {
        return ruleCache;
    }

    @Override
    public String toString() {
        return "ConfigurableAgeOffFilter [ default cutOffDateMillis=" + cutOffDateMillis + ", filename=" + filename + ", filterList=[" + filterList + "]]";
    }

}
