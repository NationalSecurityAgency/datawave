package datawave.ingest.mapreduce.job.reduce;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.conf.ColumnToClassMapping;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.ingest.mapreduce.job.TableConfigurationUtil;
import datawave.util.ColumnSetUtil;

@SuppressWarnings("deprecation")
public abstract class AggregatingReducer<IK,IV,OK,OV> extends Reducer<IK,IV,OK,OV> {

    public static final String INGEST_VALUE_DEDUP_BY_TIMESTAMP_KEY = "ingest.value.dedup.by.timestamp";

    public static final String INGEST_VALUE_DEDUP_AGGREGATION_KEY = "ingest.value.dedup.aggregation";

    // this property can turn off the aggregation in the reducer for a specific table
    public static final String USE_AGGREGATOR_PROPERTY = ".table.aggregate";
    public static final String PROPERTY_REGEX_FOR_AGGREGATOR = "\\.(.+)\\.(\\d+)\\.(.+)";

    public static Pattern namePattern = Pattern.compile("table\\.iterator\\.(majc|minc)\\.(\\w+)");

    public static Pattern optionPattern = Pattern.compile("table\\.iterator\\.(majc|minc)\\.(\\w+)\\.opt\\.(\\w+)");

    public static Text ALL_COLUMN_FAMILIES = new Text("*");

    public static final long MILLISPERDAY = 1000l * 60l * 60l * 24l;

    // Map of table names to aggregator map. The embedded map is a map of column families to aggregator instances
    protected Map<Text,SortedSet<CustomColumnToClassMapping>> combiners = new HashMap<>();
    protected Map<Text,Boolean> useAggregators = new HashMap<>();
    protected HashSet<Text> noTSDedupTables = new HashSet<>();
    protected HashSet<Text> TSDedupTables = new HashSet<>();
    TableConfigurationUtil tcu;

    private static final Logger log = Logger.getLogger(AggregatingReducer.class);

    /**
     * Setup the reducer. Delegates to setup(Configuration)
     *
     * @param context
     *            the context
     */
    @Override
    protected final void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        setup(context.getConfiguration());
    }

    /**
     * Allow setup method that can be executed manually
     *
     * @param conf
     *            the configuration
     * @throws IOException
     *             if there is an issue with read or write
     * @throws InterruptedException
     *             if the thread is interrupted
     */
    public void setup(Configuration conf) throws IOException, InterruptedException {

        /**
         * Grab the tables that do not require timestamp deduping, but require aggregating
         */
        tcu = new TableConfigurationUtil(conf);
        tcu.setTableItersPrioritiesAndOpts();
        String[] tables = conf.getStrings(INGEST_VALUE_DEDUP_AGGREGATION_KEY);
        if (tables != null) {
            for (String table : tables) {
                noTSDedupTables.add(new Text(table));
            }
        }
        /**
         * Grab tables that will be deduped by timestamp
         */
        tables = conf.getStrings(INGEST_VALUE_DEDUP_BY_TIMESTAMP_KEY);
        if (tables != null) {
            for (String table : tables) {
                TSDedupTables.add(new Text(table));
            }
        }
        configureReductionInterface(conf);

        // turn off aggregation for tables so configured
        for (String table : TableConfigurationUtil.getJobOutputTableNames(conf)) {
            useAggregators.put(new Text(table), conf.getBoolean(table + USE_AGGREGATOR_PROPERTY, true));
        }

    }

    /**
     * Reads information from the supplied job configuration ({@code conf}) and finds any configured aggregators. If any are found, the classes are instantiated
     * here and added to a list of lookups per table. If multiple aggregators are configured for a table, and they overlap in terms of which keys they accept,
     * then they will be applied in priority order.
     *
     * @param conf
     *            the configuration
     */
    private void configureReductionInterface(Configuration conf) {
        // Build a map of table => sorted sets of aggregator options (in increasing priority order for
        // each set of aggregator options).
        configureAggregators(conf);

        configureCombiners(conf);

    }

    protected void configureCombiners(Configuration conf) {

        // Now construct the aggregator classes that are specified in the configuration, and add them
        // to a map of table => priority list of column=>class mappings. Users can just call the
        // method getAggregator with a key, and get back a list of aggregators that should be applied
        // to the corresponding value. The return list aggregators should be applied in order.
        Set<String> tables = tcu.getJobOutputTableNames(conf);
        for (String table : tables) {
            Map<Integer,Map<String,String>> priorityOptions = tcu.getTableCombiners(table);
            if (priorityOptions != null) {
                SortedSet<CustomColumnToClassMapping> list = Sets.newTreeSet();
                for (Integer priority : priorityOptions.keySet()) {
                    Map<String,String> options = Maps.newHashMap();

                    options.putAll(priorityOptions.get(priority));

                    String clazz = options.get(TableConfigurationUtil.ITERATOR_CLASS_MARKER);
                    if (null == clazz) {
                        throw new RuntimeException(
                                        "Unable to instantiate combiner class. Config item 'iterclass' not present " + priority + " " + options.entrySet());
                    }
                    log.info("configuring iterator (combiner) " + clazz + " for table " + table);

                    options.remove(TableConfigurationUtil.ITERATOR_CLASS_MARKER);

                    CustomColumnToClassMapping mapping;
                    try {

                        Combiner myCombiner = null;

                        final String columnStr = options.get("columns");

                        if (null != columnStr) {
                            List<String> columns = Lists.newArrayList(Splitter.on(",").split(columnStr));

                            Map<String,Entry<Map<String,String>,String>> columnMap = Maps.newHashMap();

                            for (String column : columns) {
                                columnMap.put(ColumnSetUtil.encodeColumns(new Text(column), null), Maps.immutableEntry(options, clazz));
                            }

                            mapping = new CustomColumnToClassMapping(columnMap, priority);

                        } else {
                            mapping = new CustomColumnToClassMapping(priority, clazz);
                            myCombiner = mapping.getObject(CustomColumnToClassMapping.ALL_CF_KEY);
                            options.put("all", "true");
                            myCombiner.init(null, options, new CustomColumnToClassMapping.StubbedIteratorEnvironment());
                        }

                        list.add(mapping);

                    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IOException | NoSuchMethodException
                                    | InvocationTargetException e) {
                        throw new RuntimeException("Unable to instantiate aggregator class for one of " + options + "for " + table + ": " + e.getMessage(), e);
                    }

                }
                combiners.put(new Text(table), list);
            }
        }

    }

    protected void configureAggregators(Configuration conf) {

        // Now construct the aggregator classes that are specified in the configuration, and add them
        // to a map of table => priority list of column=>class mappings. Users can just call the
        // method getAggregator with a key, and get back a list of aggregators that should be applied
        // to the corresponding value. The return list aggregators should be applied in order.
        Set<String> tables = tcu.getJobOutputTableNames(conf);
        for (String table : tables) {
            log.info(table);

            Map<Integer,Map<String,String>> priorityOptions = tcu.getTableAggregators(table);
            if (priorityOptions != null) {
                SortedSet<CustomColumnToClassMapping> list = Sets.newTreeSet();
                for (Entry<Integer,Map<String,String>> entry : priorityOptions.entrySet()) {

                    Map<String,String> options = entry.getValue();
                    CustomColumnToClassMapping mapping = new CustomColumnToClassMapping(entry.getKey(), options);
                    list.add(mapping);
                }
                combiners.put(new Text(table), list);
            }
        }

    }

    /**
     * Cleanup method which delegates to finish
     *
     * @param context
     *            the context
     * @throws IOException
     *             if there is an issue with read or write
     * @throws InterruptedException
     *             if the thread is interrupted
     */
    @Override
    protected final void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        finish(context);
    }

    /**
     * A cleanup method that can be executed manually. Default implementation does nothing.
     *
     * @param context
     *            the context
     * @throws IOException
     *             if there is an issue with read or write
     * @throws InterruptedException
     *             if the thread is interrupted
     */
    public void finish(TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        // NOOP
    }

    /**
     * This method is called once for each key. This method delegates to the doReduce method.
     *
     * @param key
     *            the key
     * @param values
     *            a set of values
     * @param context
     *            the context
     */
    @Override
    protected final void reduce(IK key, Iterable<IV> values, Context context) throws IOException, InterruptedException {
        doReduce(key, values, context);
    }

    /**
     * This method is called once for each key. Most applications will define their reduce class by overriding this method. The default implementation is an
     * identity function.
     *
     * @param key
     *            the key
     * @param values
     *            list of values
     * @param context
     *            the context
     * @throws IOException
     *             if there is an issue with read or write
     * @throws InterruptedException
     *             if the thread is interrupted
     */
    @SuppressWarnings("unchecked")
    public void doReduce(IK key, Iterable<IV> values, TaskInputOutputContext<?,?,OK,OV> context) throws IOException, InterruptedException {
        for (IV value : values) {
            context.write((OK) key, (OV) value);
        }
    }

    /**
     * Can be used to execute this process manually
     *
     * @param entries
     *            the map of entries
     * @param ctx
     *            the context
     * @throws IOException
     *             if there is an issue with read or write
     * @throws InterruptedException
     *             if the thread is interrupted
     */
    public void reduce(Multimap<IK,IV> entries, TaskInputOutputContext<?,?,OK,OV> ctx) throws IOException, InterruptedException {
        for (IK key : entries.keySet()) {
            Collection<IV> values = entries.get(key);
            this.doReduce(key, values, ctx);
        }
    }

    /**
     * Write the output to the context
     *
     * @param key
     *            the event key
     * @param value
     *            the value
     * @param ctx
     *            the context
     * @throws IOException
     *             if there is an issue with read or write
     * @throws InterruptedException
     *             if the thread is interrupted
     */
    protected void writeToContext(OK key, OV value, TaskInputOutputContext<?,?,OK,OV> ctx) throws IOException, InterruptedException {
        ctx.write(key, value);
    }

    /**
     * Determines whether aggregation should be performed, regardless whether any aggregators are configured.
     *
     * @param table
     *            the table
     * @return true if aggregation should be performed, false otherwise
     */
    protected boolean useAggregators(Text table) {
        return (useAggregators.get(table) != null ? useAggregators.get(table) : true);
    }

    /**
     * Gets the aggregators that should be applied to the value(s) associated with {@code key}. The aggregators in the list should be applied in order.
     *
     * @param table
     *            the table name from which {@code key} was retrieved
     * @param key
     *            the key to use for locating a set of aggregators that apply to the values for key
     * @return a list (either empty or filled, but never null) of aggregators to apply to the values for {@code key}
     */
    protected List<Combiner> getAggregators(Text table, Key key) {
        SortedSet<CustomColumnToClassMapping> mappings = combiners.get(table);
        ArrayList<Combiner> aggList = new ArrayList<>();
        if (mappings != null) {
            for (CustomColumnToClassMapping mapping : mappings) {
                Combiner agg = mapping.getObject(key);
                if (agg != null)
                    aggList.add(agg);
            }
        }
        return aggList;
    }

    /**
     * Helper class that, given a {@link Key}, determines which aggregator, if any, should be used to aggregate multiple values for that key.
     */
    protected static class CustomColumnToClassMapping extends ColumnToClassMapping<Combiner> implements Comparable<CustomColumnToClassMapping> {
        private static final String ALL_CF_STR = "*";
        protected static final Key ALL_CF_KEY = new Key("", ALL_CF_STR);
        protected Integer priority;

        public CustomColumnToClassMapping(Integer priority, Map<String,String> opts) {
            super();

            // This logic is copied from the parent constructor, but we need to since the parent
            // uses AccumuloClassLoader and we don't have that available on our class path.
            for (Entry<String,String> entry : opts.entrySet()) {
                String column = entry.getKey();

                final String className = entry.getValue();

                Pair<Text,Text> pcic;
                if (ALL_CF_STR.equals(column)) {
                    pcic = new Pair<>(ALL_CF_KEY.getColumnFamily(), null);
                } else {
                    pcic = ColumnSetUtil.decodeColumns(column);
                }

                Combiner agg = null;

                try {
                    Class<? extends Combiner> clazz = Class.forName(className).asSubclass(Combiner.class);
                    log.info("configuring iterator (aggregator) " + clazz);

                    agg = clazz.getDeclaredConstructor().newInstance();

                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }

                if (pcic.getSecond() == null) {
                    addObject(pcic.getFirst(), agg);
                } else {
                    addObject(pcic.getFirst(), pcic.getSecond(), agg);
                }
            }

            this.priority = priority;
        }

        public CustomColumnToClassMapping(Map<String,Entry<Map<String,String>,String>> columnMap, Integer priority) {
            super();

            // This logic is copied from the parent constructor, but we need to since the parent
            // uses AccumuloClassLoader and we don't have that available on our class path.
            for (Entry<String,Entry<Map<String,String>,String>> entry : columnMap.entrySet()) {
                String column = entry.getKey();
                Entry<Map<String,String>,String> clazzOptions = entry.getValue();

                final String className = clazzOptions.getValue();

                Pair<Text,Text> pcic;
                if (ALL_CF_STR.equals(column)) {
                    pcic = new Pair<>(ALL_CF_KEY.getColumnFamily(), null);
                } else {
                    pcic = ColumnSetUtil.decodeColumns(column);
                }

                Combiner agg = null;

                try {
                    Class<? extends Combiner> clazz = Class.forName(className).asSubclass(Combiner.class);

                    agg = clazz.newInstance();
                    // init with a stubbed-out class so that an upcoming call to env.getConfig in Combiner will not throw a NPE
                    agg.init(null, clazzOptions.getKey(), new StubbedIteratorEnvironment());

                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IOException e) {
                    throw new RuntimeException(e);
                }

                if (pcic.getSecond() == null) {
                    addObject(pcic.getFirst(), agg);
                } else {
                    addObject(pcic.getFirst(), pcic.getSecond(), agg);
                }
            }

            this.priority = priority;
        }

        public CustomColumnToClassMapping(Integer priority, String className)
                        throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
            super();

            Class<? extends Combiner> clazz = Class.forName(className).asSubclass(Combiner.class);

            addObject(ALL_CF_KEY.getColumnFamily(), clazz.getDeclaredConstructor().newInstance());

            this.priority = priority;
        }

        @Override
        public Combiner getObject(Key key) {
            Combiner aggregator = super.getObject(key);
            // If we didn't find a match on anything else, then try the "ALL_CF_KEY"
            // key in case there is an aggregator specified to run on all column
            // families.
            if (aggregator == null) {
                aggregator = super.getObject(ALL_CF_KEY);
            }
            return aggregator;
        }

        @Override
        public int compareTo(CustomColumnToClassMapping o) {
            return priority.compareTo(o.priority);
        }

        private static class StubbedIteratorEnvironment implements IteratorEnvironment {

            // Need this ctor for testing, to overcome javassist.CannotCompileException issue on Java 11
            StubbedIteratorEnvironment() {}

            @Override
            public AccumuloConfiguration getConfig() {
                return null;
            }

            @Override
            public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
                return null;
            }

            @Override
            public IteratorUtil.IteratorScope getIteratorScope() {
                return null;
            }

            @Override
            public boolean isUserCompaction() {
                return false;
            }

            @Override
            public boolean isFullMajorCompaction() {
                return false;
            }

            @Override
            public Authorizations getAuthorizations() {
                throw new UnsupportedOperationException();
            }

            @Override
            public IteratorEnvironment cloneWithSamplingEnabled() {
                throw new SampleNotPresentException();
            }

            @Override
            public boolean isSamplingEnabled() {
                return false;
            }

            @Override
            public SamplerConfiguration getSamplerConfiguration() {
                return null;
            }

            @Override
            public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {

            }
        }
    }
}
