package datawave.ingest.mapreduce;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.normalizer.DateNormalizer;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.filter.KeyValueFilter;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.data.config.ingest.FilterIngest;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.data.config.ingest.VirtualIngest;
import datawave.ingest.input.reader.event.EventErrorSummary;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.error.ErrorDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.ConstraintChecker;
import datawave.ingest.mapreduce.job.metrics.KeyValueCountingContextWriter;
import datawave.ingest.mapreduce.job.metrics.Metric;
import datawave.ingest.mapreduce.job.metrics.MetricsConfiguration;
import datawave.ingest.mapreduce.job.metrics.MetricsService;
import datawave.ingest.mapreduce.job.metrics.ReusableMetricsLabels;
import datawave.ingest.mapreduce.job.statsd.StatsDEnabledMapper;
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.mapreduce.job.writer.LiveContextWriter;
import datawave.ingest.mapreduce.partition.MultiTableRangePartitioner;
import datawave.ingest.metric.IngestInput;
import datawave.ingest.metric.IngestOutput;
import datawave.ingest.metric.IngestProcess;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.ingest.test.StandaloneTaskAttemptContext;
import datawave.ingest.time.Now;
import datawave.ingest.validation.FieldValidator;
import datawave.marking.MarkingFunctions;
import datawave.util.StringUtils;
import datawave.util.time.TraceStopwatch;

/**
 *
 * Implementation of a MapReduce Mapper class that reads in Event objects and processes them using implementations of the DataTypeHandler interface. Users of
 * this Mapper will need to set the SUPPORTED_TYPES parameter to a comma separated list of supported datawave.ingest.data.Types. The Type.ALL is allowed also,
 * and is used to note that all types will execute the associated handler. The TYPE_HANDLERS parameter need to be contain a list of DataTypeHandler subclass
 * class names. When an Event is sent to the map method this class will parse the Event using the BaseIngestHelper implementation that is returned by the
 * DataTypeHandler implementation. If the DISCARD_INTERVAL property is set, then the map method will determine whether to process the Event. If the Event date
 * is within the window, then the map will parse the Event into a map of field names and field values, the map method will call the process() method on each
 * DataTypeHandler implementation that has been configured for the Type of Event.
 *
 * This class no longer supports processing events in a multi-threaded fashion as that was proven to NOT be beneficial in the long run (i.e. The bulk of the
 * time is spent in the ContextWriter which is synchronized. Also multithreading mean potentially more memory use which we should already be maximizing per
 * machine in the hadoop map-reduce cluster.)
 *
 *
 *
 * @param <K1>
 *            input key
 * @param <K2>
 *            output key
 * @param <V2>
 *            output value
 */
public class EventMapper<K1,V1 extends RawRecordContainer,K2,V2> extends StatsDEnabledMapper<K1,V1,K2,V2> {

    private static final String SRC_FILE_DEL = "|";

    private static final Logger log = Logger.getLogger(EventMapper.class);

    /**
     * number which will be used to evaluate whether or not an Event should be processed. If the Event.getEventDate() is greater than (now - interval) then it
     * will be processed.
     */
    public static final String DISCARD_INTERVAL = "event.discard.interval";

    public static final String CONTEXT_WRITER_CLASS = "ingest.event.mapper.context.writer.class";
    public static final String CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS = "ingest.event.mapper.context.writer.output.table.counters";
    public static final String FILE_NAME_COUNTERS = "ingest.event.mapper.file.name.counters";

    protected boolean createSequenceFileName = true;

    protected boolean trimSequenceFileName = true;

    protected boolean createRawFileName = true;

    public static final String LOAD_DATE_FIELDNAME = "LOAD_DATE";

    public static final String SEQUENCE_FILE_FIELDNAME = "ORIG_FILE";

    public static final String LOAD_SEQUENCE_FILE_NAME = "ingest.event.mapper.load.seq.filename";

    public static final String TRIM_SEQUENCE_FILE_NAME = "ingest.event.mapper.trim.sequence.filename";

    public static final String RAW_FILE_FIELDNAME = "RAW_FILE";

    public static final String LOAD_RAW_FILE_NAME = "ingest.event.mapper.load.raw.filename";

    public static final String ID_FILTER_FSTS = "ingest.event.mapper.id.filter.fsts";

    protected Map<String,List<DataTypeHandler<K1>>> typeMap = new HashMap<>();

    /**
     * might as well cache the discard interval
     */
    protected Map<String,Long> dataTypeDiscardIntervalCache = new HashMap<>();

    private FileSplit split = null;

    private long interval = 0l;

    private static Now now = Now.getInstance();

    private StandaloneStatusReporter reporter = new StandaloneStatusReporter();

    private DateNormalizer dateNormalizer = new DateNormalizer();

    private ContextWriter<K2,V2> contextWriter = null;

    protected long offset = 0;

    protected String splitStart = null;

    Multimap<String,FieldValidator> validators;

    protected Set<String> sequenceFileNames = new HashSet<>();

    protected MarkingFunctions markingFunctions;

    private boolean metricsEnabled = false;
    private MetricsService<K2,V2> metricsService;
    private ReusableMetricsLabels metricsLabels;

    /**
     * Set up the datatype handlers
     */
    @SuppressWarnings("unchecked")
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        InputSplit is = context.getInputSplit();
        if (is instanceof FileSplit)
            split = (FileSplit) is;
        // push the current filename on to the NDC
        if (null != split) {
            NDC.push(split.getPath().toString());
            splitStart = Long.valueOf(split.getStart()).toString();
        } else
            splitStart = null;

        // Needed for side-effects
        markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();

        // Initialize the Type Registry
        TypeRegistry.getInstance(context.getConfiguration());

        interval = context.getConfiguration().getLong(DISCARD_INTERVAL, 0l);

        // default to true, but it can be disabled
        createSequenceFileName = context.getConfiguration().getBoolean(LOAD_SEQUENCE_FILE_NAME, true);

        trimSequenceFileName = context.getConfiguration().getBoolean(TRIM_SEQUENCE_FILE_NAME, true);

        createRawFileName = context.getConfiguration().getBoolean(LOAD_RAW_FILE_NAME, true);

        Class<? extends KeyValueFilter<K2,V2>> firstFilter = null;

        // Use the filter class as the context writer if any
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = EventMapper.class.getClassLoader();
        }
        Configuration filterConf = new Configuration(context.getConfiguration());
        Class<? extends KeyValueFilter<K2,V2>> lastFilter = null;
        for (String filterClassName : getDataTypeFilterClassNames()) {

            Class<? extends KeyValueFilter<K2,V2>> filterClass = null;
            try {
                filterClass = (Class<? extends KeyValueFilter<K2,V2>>) Class.forName(filterClassName, true, classLoader);
                if (firstFilter == null) {
                    firstFilter = filterClass;
                }
                if (lastFilter != null) {
                    KeyValueFilter<K2,V2> filter = lastFilter.getDeclaredConstructor().newInstance();
                    filter.configureChainedContextWriter(filterConf, filterClass);
                }
                lastFilter = filterClass;
            } catch (Exception e) {
                throw new IOException("Unable to configure " + filterClass + " on " + lastFilter, e);
            }
        }

        Class<? extends ContextWriter<K2,V2>> contextWriterClass;

        if (Mutation.class.equals(context.getMapOutputValueClass())) {
            contextWriterClass = (Class<ContextWriter<K2,V2>>) context.getConfiguration().getClass(CONTEXT_WRITER_CLASS, LiveContextWriter.class,
                            ContextWriter.class);
        } else {
            contextWriterClass = (Class<ContextWriter<K2,V2>>) context.getConfiguration().getClass(CONTEXT_WRITER_CLASS, BulkContextWriter.class,
                            ContextWriter.class);
        }

        if (lastFilter != null) {
            try {
                KeyValueFilter<K2,V2> filter = lastFilter.newInstance();
                filter.configureChainedContextWriter(filterConf, contextWriterClass);
            } catch (Exception e) {
                throw new IOException("Unable to configure " + contextWriterClass + " on " + lastFilter, e);
            }
            contextWriterClass = firstFilter;
        }
        try {
            contextWriter = contextWriterClass.getDeclaredConstructor().newInstance();
            contextWriter.setup(filterConf, filterConf.getBoolean(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, false));
        } catch (Exception e) {
            throw new IOException("Failed to initialized " + contextWriterClass + " from property " + CONTEXT_WRITER_CLASS, e);
        }

        metricsEnabled = MetricsConfiguration.isEnabled(context.getConfiguration());

        if (metricsEnabled) {
            try {
                // important that MetricsService gets the unwrapped contextWriter
                // we don't want metrics on our metrics
                metricsService = new MetricsService<>(contextWriter, context);
                metricsLabels = new ReusableMetricsLabels();

                contextWriter = new KeyValueCountingContextWriter<>(contextWriter, metricsService);
            } catch (Exception e) {
                log.error("Could not configure metrics, disabling", e);
                MetricsConfiguration.disable(context.getConfiguration());
                metricsEnabled = false;
            }
        }

        validators = ArrayListMultimap.create();

        if (null != split) {
            if (filterConf.getBoolean(FILE_NAME_COUNTERS, true)) {
                getCounter(context, IngestInput.FILE_NAME.name(), split.getPath().toString()).increment(1);
            }
        }

        getCounter(context, IngestInput.LINE_BYTES.toString(), "MIN").setValue(Long.MAX_VALUE);

        offset = 0;

        if (log.isInfoEnabled()) {
            log.info("EventMapper configured. Bulk Ingest = true");
            log.info("EventMapper configured with the following filters: " + getDataTypeFilterClassNames());
        }

    }

    /**
     * Get the data type handlers for a given type name. This will also fill the dataTypeDiscardIntervalCache and the validators as a side effect.
     *
     * @param typeStr
     *            name of the type
     * @param context
     *            the context
     * @return the data type handlers
     */
    private List<DataTypeHandler<K1>> loadDataType(String typeStr, Context context) {
        // Do not load the type twice
        if (!typeMap.containsKey(typeStr)) {

            typeMap.put(typeStr, new ArrayList<>());

            long myInterval = context.getConfiguration().getLong(typeStr + "." + DISCARD_INTERVAL, interval);

            dataTypeDiscardIntervalCache.put(typeStr, myInterval);

            log.info("Setting up type: " + typeStr + " with interval " + myInterval);

            if (!TypeRegistry.getTypeNames().contains(typeStr)) {
                log.warn("Attempted to load configuration for a type that does not exist in the registry: " + typeStr);
            } else {
                Type t = TypeRegistry.getType(typeStr);
                String fieldValidators = context.getConfiguration().get(typeStr + FieldValidator.FIELD_VALIDATOR_NAMES);

                if (fieldValidators != null) {
                    String[] validatorClasses = StringUtils.split(fieldValidators, ",");
                    for (String validatorClass : validatorClasses) {
                        try {
                            Class<? extends FieldValidator> clazz = Class.forName(validatorClass).asSubclass(FieldValidator.class);
                            FieldValidator validator = clazz.newInstance();
                            validator.init(t, context.getConfiguration());
                            validators.put(typeStr, validator);
                        } catch (ClassNotFoundException e) {
                            log.error("Error finding validator " + validatorClass, e);
                        } catch (InstantiationException | IllegalAccessException e) {
                            log.error("Error creating validator " + validatorClass, e);
                        }
                    }
                }

                String[] handlerClassNames = t.getDefaultDataTypeHandlers();

                if (handlerClassNames != null) {
                    for (String handlerClassName : handlerClassNames) {
                        log.info("Configuring handler: " + handlerClassName);
                        try {
                            @SuppressWarnings("unchecked")
                            Class<? extends DataTypeHandler<K1>> clazz = (Class<? extends DataTypeHandler<K1>>) Class.forName(handlerClassName);
                            DataTypeHandler<K1> h = clazz.getDeclaredConstructor().newInstance();
                            // Create a counter initialized to zero for all handler types.
                            getCounter(context, IngestOutput.ROWS_CREATED.name(), h.getClass().getSimpleName()).increment(0);
                            // Trick here. Set the data.name parameter to type T, then call setup on the DataTypeHandler
                            Configuration clone = new Configuration(context.getConfiguration());
                            clone.set(DataTypeHelper.Properties.DATA_NAME, t.typeName());
                            // Use the StandaloneReporter and StandaloneTaskAttemptContext for the Handlers. Because the StandaloneTaskAttemptContext
                            // is a subclass of TaskInputOutputContext and TaskAttemptContext is not. We are using this to record the counters during
                            // processing. We will need to add the counters in the StandaloneReporter to the Map.Context in the close call.
                            // TaskAttemptContext newContext = new TaskAttemptContext(clone, context.getTaskAttemptID());
                            StandaloneTaskAttemptContext<K1,V1,K2,V2> newContext = new StandaloneTaskAttemptContext<>(clone, context.getTaskAttemptID(),
                                            reporter);
                            h.setup(newContext);
                            typeMap.get(typeStr).add(h);
                        } catch (ClassNotFoundException e) {
                            log.error("Error finding DataTypeHandler " + handlerClassName, e);
                        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                            log.error("Error creating DataTypeHandler " + handlerClassName, e);
                        }
                    }
                }
            }
            log.info("EventMapper configured with the following handlers for " + typeStr + ": " + typeMap.get(typeStr));
        }

        return typeMap.get(typeStr);
    }

    private List<String> getDataTypeFilterClassNames() {

        SortedMap<Integer,String[]> priorityToFilters = new TreeMap<>();

        // The Type Registry contains information on the configured types. Pass back a
        // list of the configured filters in the appropriate priority order.
        for (Type t : TypeRegistry.getTypes()) {
            if (null != t.getDefaultDataTypeFilters() && t.getDefaultDataTypeFilters().length > 0) {
                priorityToFilters.put(t.getFilterPriority(), t.getDefaultDataTypeFilters());
            }
        }

        // now expand the filters into one list, priority order
        List<String> filters = new ArrayList<>();
        for (String[] value : priorityToFilters.values()) {
            filters.addAll(Arrays.asList(value));
        }

        return filters;
    }

    public void map(K1 key, V1 value, Context context) throws IOException, InterruptedException {

        TraceStopwatch eventMapperTimer = null;

        if (metricsEnabled) {
            eventMapperTimer = new TraceStopwatch("Time in EventMapper for " + context.getTaskAttemptID());
            eventMapperTimer.start();
        }

        // ensure this datatype's handlers etc are loaded such that the dataTypeDiscardIntervalCache and validators are filled as well
        List<DataTypeHandler<K1>> typeHandlers = loadDataType(value.getDataType().typeName(), context);

        // This is a little bit fragile, but there is no other way
        // to get the context on a partitioner, and we are only
        // using this to set some counters that collect stats.
        MultiTableRangePartitioner.setContext(context);

        Long myInterval = dataTypeDiscardIntervalCache.get(value.getDataType().typeName());

        // setup the configuration on the event
        // this is automatically done by the sequence reader....
        // value.setConf(context.getConfiguration());

        // Flag to control whether a reprocessed event caused an NDC.push
        boolean reprocessedNDCPush = false;

        long rawDataBytes = value.getDataOutputSize();
        if (rawDataBytes != -1) {
            getCounter(context, IngestInput.LINE_BYTES.toString(), "TOTAL").increment(rawDataBytes);
            long minBytes = getCounter(context, IngestInput.LINE_BYTES.toString(), "MIN").getValue();
            if (rawDataBytes < minBytes) {
                getCounter(context, IngestInput.LINE_BYTES.toString(), "MIN").setValue(rawDataBytes);
            }
            long maxBytes = getCounter(context, IngestInput.LINE_BYTES.toString(), "MAX").getValue();
            if (rawDataBytes > maxBytes) {
                getCounter(context, IngestInput.LINE_BYTES.toString(), "MAX").setValue(rawDataBytes);
            }
        }

        // First lets clear this event from the error table if we are reprocessing a previously errored event
        if (value.getAuxData() instanceof EventErrorSummary) {
            EventErrorSummary errorSummary = (EventErrorSummary) (value.getAuxData());
            value.setAuxData(null);

            // pass the processedCount through via the aux properties
            value.setAuxProperty(ErrorDataTypeHandler.PROCESSED_COUNT, Integer.toString(errorSummary.getProcessedCount() + 1));

            // delete these keys from the error table. If this fails then nothing will have changed
            if (log.isInfoEnabled())
                log.info("Purging event from the " + errorSummary.getTableName() + " table");

            try {
                // Load error dataType into typeMap
                loadDataType(TypeRegistry.ERROR_PREFIX, context);

                // purge event
                errorSummary.purge(contextWriter, context, value, typeMap);

                // Set the original file value from the event in the error table
                Collection<String> origFiles = errorSummary.getEventFields().get(SEQUENCE_FILE_FIELDNAME);
                if (!origFiles.isEmpty()) {
                    NDC.push(origFiles.iterator().next());
                    reprocessedNDCPush = true;
                }

            } catch (Exception e) {
                contextWriter.rollback();
                log.error("Failed to clean event from error table.  Terminating map", e);
                throw new IOException("Failed to clean event from error table, Terminating map", e);
            } finally {
                contextWriter.commit(context);
                context.progress();
            }
        } else {
            // pass the processedCount through via the aux properties
            value.setAuxProperty(ErrorDataTypeHandler.PROCESSED_COUNT, "1");
        }

        // Determine whether the event date is greater than the interval. Excluding fatal error events.
        if (!value.fatalError() && null != myInterval && 0L != myInterval && (value.getDate() < (now.get() - myInterval))) {
            if (log.isInfoEnabled())
                log.info("Event with time " + value.getDate() + " older than specified interval of " + (now.get() - myInterval) + ", skipping...");
            getCounter(context, IngestInput.OLD_EVENT).increment(1);
            return;
        }

        // Add the list of handlers with the ALL specified handlers
        List<DataTypeHandler<K1>> handlers = new ArrayList<>();
        handlers.addAll(typeHandlers);
        handlers.addAll(loadDataType(TypeRegistry.ALL_PREFIX, context));

        // Always include any event errors in the counters
        for (String error : value.getErrors()) {
            getCounter(context, IngestInput.EVENT_ERROR_TYPE.name(), error).increment(1);
        }

        // switch over to the errorHandlerList if still a fatal error
        if (value.fatalError()) {
            // now clear out the handlers to avoid processing this event
            handlers.clear();
            if (!value.ignorableError()) {
                // since this is not an ignorable error, lets add the error handlers back into the list
                handlers.addAll(loadDataType(TypeRegistry.ERROR_PREFIX, context));

                getCounter(context, IngestInput.EVENT_FATAL_ERROR).increment(1);
                getCounter(context, IngestInput.EVENT_FATAL_ERROR.name(), "ValidationError").increment(1);
            } else {
                getCounter(context, IngestInput.EVENT_IGNORABLE_ERROR).increment(1);
                getCounter(context, IngestInput.EVENT_IGNORABLE_ERROR.name(), "IgnorableError").increment(1);
            }

            context.progress();
        }

        Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
        try {
            processEvent(key, value, handlers, fields, context);
        } catch (Exception e) {
            // Rollback anything written for this event
            contextWriter.rollback();

            // Fail job on constraint violations
            if (e instanceof ConstraintChecker.ConstraintViolationException) {
                throw ((RuntimeException) e);
            }

            // ensure they know we are still working on it
            context.progress();

            // log error
            log.error("Runtime exception processing event", e);

            // now lets dump to the errors table
            // first set the exception on the event if not a field normalization error in which case the fields contain the errors
            if (!(e instanceof FieldNormalizationError)) {
                value.setAuxData(e);
            }
            for (DataTypeHandler<K1> handler : loadDataType(TypeRegistry.ERROR_PREFIX, context)) {
                if (log.isTraceEnabled())
                    log.trace("executing handler: " + handler.getClass().getName());
                try {
                    executeHandler(key, value, fields, handler, context);
                    context.progress();
                } catch (Exception e2) {
                    // This is a real bummer, we had a critical exception attempting to throw the event into the error table.
                    // lets terminate this job
                    log.error("Failed to process error data handlers for an event", e2);
                    throw new IOException("Failed to process error data handlers for an event", e2);
                }
            }

            // now create some counters
            getCounter(context, IngestProcess.RUNTIME_EXCEPTION).increment(1);
            List<String> exceptions = getExceptionSynopsis(e);
            for (String exception : exceptions) {
                getCounter(context, IngestProcess.RUNTIME_EXCEPTION.name(), exception).increment(1);
            }
        } finally {
            // Remove ORIG_FILE from NDC that was populated by reprocessing events from the error tables
            if (reprocessedNDCPush) {
                NDC.pop();
            }
            // cleanup the context writer
            contextWriter.commit(context);
            context.progress();
        }

        getCounter(context, IngestOutput.EVENTS_PROCESSED.name(), value.getDataType().typeName().toUpperCase()).increment(1);

        offset++;

        if (metricsEnabled && eventMapperTimer != null) {
            eventMapperTimer.stop();
            long timeInEventMapper = eventMapperTimer.elapsed(TimeUnit.MILLISECONDS);

            metricsLabels.clear();
            metricsLabels.put("dataType", value.getDataType().typeName());
            metricsService.collect(Metric.MILLIS_IN_EVENT_MAPPER, metricsLabels.get(), fields, timeInEventMapper);
        }
    }

    /**
     * Get an exception synopsis that is suitable as a counter. We want at a minimum the exception name and a useful location. A useful location is defined as
     * the highest location that is in the datawave.ingest package
     *
     * @param e
     *            the exception to check
     * @return A synopsis of the exception
     */
    private List<String> getExceptionSynopsis(Throwable e) {
        Stack<Throwable> exceptions = new Stack<>();
        while (e != null && !exceptions.contains(e)) {
            // noinspection ThrowableResultOfMethodCallIgnored
            exceptions.push(e);
            e = e.getCause();
        }

        List<String> synopsis = new ArrayList<>();
        StringBuilder buffer = new StringBuilder();

        boolean foundTrace = false;
        while (!foundTrace && !exceptions.isEmpty()) {
            e = exceptions.pop();
            StackTraceElement[] elements = e.getStackTrace();
            for (StackTraceElement element : elements) {
                if (element.getClassName().startsWith("datawave.ingest")) {
                    foundTrace = true;
                }
                if (foundTrace) {
                    buffer.append(e.getClass().getName());
                    buffer.append('@');
                    buffer.append(element.getClassName());
                    buffer.append('.').append(element.getMethodName());
                    buffer.append('(').append(element.getLineNumber()).append(')');
                    synopsis.add(buffer.toString());
                    buffer.setLength(0);
                    break;
                }
            }
        }

        // NOTE: By definition, this method is only called by datawave.ingest.EventMapper, and only
        // available to be called by this class the condition on line 542 will always be satisfied
        // and so 'foundTrace' will always be set to true. So, this code can eventually be removed.
        if (!foundTrace) {
            assert e != null;
            buffer.append(e.getClass().getName());
            synopsis.add(buffer.toString());
        }

        return synopsis;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

        // Write the metadata to the output
        for (List<DataTypeHandler<K1>> handlers : typeMap.values()) {
            for (DataTypeHandler<K1> h : handlers)
                if (h.getMetadata() != null) {
                    try {
                        contextWriter.write(h.getMetadata().getBulkMetadata(), context);
                    } finally {
                        contextWriter.commit(context);
                    }
                }
        }

        // dump any unflushed metrics
        if (metricsEnabled) {
            metricsService.close();
        }

        // cleanup the context writer
        contextWriter.cleanup(context);

        for (List<DataTypeHandler<K1>> handlers : typeMap.values()) {
            for (DataTypeHandler<K1> h : handlers)
                h.close(context);
        }
        typeMap.clear();

        // Add the counters from the standalone reporter to this context.
        Counters counters = reporter.getCounters();
        for (CounterGroup cg : counters) {
            for (Counter c : cg) {
                getCounter(context, cg.getName(), c.getName()).increment(c.getValue());
            }
        }

        super.cleanup(context);

        // we pushed the filename on the NDC if split is non null, so pop it here.
        if (null != split) {
            NDC.pop();
        }
    }

    /**
     * This is where we apply a list of handlers to an event.
     *
     * @param key
     *            The key of the map process
     * @param value
     *            The event
     * @param handlers
     *            The list of handlers to apply
     * @param fields
     *            The list which keeps the last set of fields (retained in case the caller needs to handle a thrown exception)
     * @param context
     *            The context
     * @throws Exception
     *             if there is a problem
     */
    public void processEvent(K1 key, RawRecordContainer value, List<DataTypeHandler<K1>> handlers, Multimap<String,NormalizedContentInterface> fields,
                    Context context) throws Exception {
        IngestHelperInterface previousHelper = null;

        for (DataTypeHandler<K1> handler : handlers) {
            if (log.isTraceEnabled())
                log.trace("executing handler: " + handler.getClass().getName());

            // gather the fields
            IngestHelperInterface thisHelper = handler.getHelper(value.getDataType());

            // This handler has no helper for the event's data type. Therefore, we should
            // just move on to the next handler. This can happen, for example, with the
            // edge handler, depending on the event's data type.
            if (thisHelper == null) {
                if (log.isTraceEnabled())
                    log.trace("Aborting processing due to null ingest helper");
                continue;
            }

            // Try to only parse the event once. Parse the event on the first pass and only if
            // the BaseIngestHelper class differs. The same class used by different handlers
            // *should* produce the same result.
            if (null == previousHelper || !previousHelper.getClass().getName().equals(thisHelper.getClass().getName())) {
                fields.clear();
                Throwable e = null;
                for (Map.Entry<String,NormalizedContentInterface> entry : getFields(value, handler).entries()) {
                    // noinspection ThrowableResultOfMethodCallIgnored
                    if (entry.getValue().getError() != null) {
                        e = entry.getValue().getError();
                    }
                    fields.put(entry.getKey(), entry.getValue());
                }
                if (e != null) {
                    throw new FieldNormalizationError("Failed getting all fields", e);
                }

                // Event based metrics
                if (metricsEnabled) {
                    metricsLabels.clear();
                    metricsLabels.put("dataType", value.getDataType().typeName());

                    metricsService.collect(Metric.EVENT_COUNT, metricsLabels.get(), fields, 1L);
                    metricsService.collect(Metric.BYTE_COUNT, metricsLabels.get(), fields, (long) value.getRawData().length);
                }

                previousHelper = thisHelper;
            }

            Collection<FieldValidator> fieldValidators = validators.get(value.getDataType().outputName());
            for (FieldValidator validator : fieldValidators) {
                validator.validate(value, fields);
            }

            executeHandler(key, value, fields, handler, context);

            context.progress();
        }
    }

    private static class FieldNormalizationError extends Exception {
        private static final long serialVersionUID = 1L;

        public FieldNormalizationError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public Multimap<String,NormalizedContentInterface> getFields(RawRecordContainer value, DataTypeHandler<K1> handler) throws Exception {
        Multimap<String,NormalizedContentInterface> newFields;
        // Parse the event into its field names and field values using the DataTypeHandler's BaseIngestHelper object.
        newFields = handler.getHelper(value.getDataType()).getEventFields(value);

        // Also get the virtual fields, if applicable.
        if (handler.getHelper(value.getDataType()) instanceof VirtualIngest) {
            VirtualIngest vHelper = (VirtualIngest) handler.getHelper(value.getDataType());
            Multimap<String,NormalizedContentInterface> virtualFields = vHelper.getVirtualFields(newFields);
            for (Entry<String,NormalizedContentInterface> v : virtualFields.entries())
                newFields.put(v.getKey(), v.getValue());
        }
        // Also get the composite fields, if applicable
        if (handler.getHelper(value.getDataType()) instanceof CompositeIngest) {
            CompositeIngest vHelper = (CompositeIngest) handler.getHelper(value.getDataType());
            Multimap<String,NormalizedContentInterface> compositeFields = vHelper.getCompositeFields(newFields);
            for (String fieldName : compositeFields.keySet()) {
                // if this is an overloaded composite field, we are replacing the existing field data
                if (vHelper.isOverloadedCompositeField(fieldName))
                    newFields.removeAll(fieldName);
                newFields.putAll(fieldName, compositeFields.get(fieldName));
            }
        }

        // Create a LOAD_DATE parameter, which is the current time in milliseconds, for all datatypes
        long loadDate = now.get();
        NormalizedFieldAndValue loadDateValue = new NormalizedFieldAndValue(LOAD_DATE_FIELDNAME, Long.toString(loadDate));
        // set an indexed field value for use by the date index data type handler
        loadDateValue.setIndexedFieldValue(dateNormalizer.normalizeDelegateType(new Date(loadDate)));
        newFields.put(LOAD_DATE_FIELDNAME, loadDateValue);

        String seqFileName = null;

        // place the sequence filename into the event
        if (createSequenceFileName) {
            seqFileName = NDC.peek();

            if (trimSequenceFileName) {
                seqFileName = StringUtils.substringAfterLast(seqFileName, "/");
            }

            if (null != seqFileName) {
                StringBuilder seqFile = new StringBuilder(seqFileName);

                seqFile.append(SRC_FILE_DEL).append(offset);

                if (null != splitStart) {
                    seqFile.append(SRC_FILE_DEL).append(splitStart);
                }

                newFields.put(SEQUENCE_FILE_FIELDNAME, new NormalizedFieldAndValue(SEQUENCE_FILE_FIELDNAME, seqFile.toString()));
            }
        }

        if (createRawFileName && !value.getRawFileName().isEmpty() && !value.getRawFileName().equals(seqFileName)) {
            newFields.put(RAW_FILE_FIELDNAME, new NormalizedFieldAndValue(RAW_FILE_FIELDNAME, value.getRawFileName()));
        }

        // Also if this helper needs to filter the fields before returning, apply now
        if (handler.getHelper(value.getDataType()) instanceof FilterIngest) {
            FilterIngest fHelper = (FilterIngest) handler.getHelper(value.getDataType());
            fHelper.filter(newFields);
        }

        return newFields;
    }

    @SuppressWarnings("unchecked")
    public void executeHandler(K1 key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, DataTypeHandler<K1> handler,
                    Context context) throws Exception {
        long count = 0;

        TraceStopwatch handlerTimer = null;

        // Handler based metrics
        if (metricsEnabled) {

            handlerTimer = new TraceStopwatch("Time in handler");
            handlerTimer.start();
        }

        // In the setup we determined whether or not we were performing bulk ingest. This tells us which
        // method to call on the DataTypeHandler interface.
        Multimap<BulkIngestKey,Value> r;

        if (!(handler instanceof ExtendedDataTypeHandler)) {
            r = handler.processBulk(key, event, fields, new ContextWrappedStatusReporter(getContext(context)));
            if (r == null) {
                getCounter(context, IngestInput.EVENT_FATAL_ERROR).increment(1);
                getCounter(context, IngestInput.EVENT_FATAL_ERROR.name(), "NullMultiMap").increment(1);
            } else {
                contextWriter.write(r, context);
                count = r.size();
            }
        } else {
            count = ((ExtendedDataTypeHandler<K1,K2,V2>) handler).process(key, event, fields, context, contextWriter);
            if (count == -1) {
                getCounter(context, IngestInput.EVENT_FATAL_ERROR).increment(1);
                getCounter(context, IngestInput.EVENT_FATAL_ERROR.name(), "NegOneCount").increment(1);
            }
        }

        // Update the counters
        if (count > 0) {
            getCounter(context, IngestOutput.ROWS_CREATED.name(), handler.getClass().getSimpleName()).increment(count);
            getCounter(context, IngestOutput.ROWS_CREATED).increment(count);
        }

        if (handler.getMetadata() != null) {
            handler.getMetadata().addEvent(handler.getHelper(event.getDataType()), event, fields, now.get());
        }

        if (metricsEnabled && handlerTimer != null) {
            handlerTimer.stop();
            long handlerTime = handlerTimer.elapsed(TimeUnit.MILLISECONDS);

            metricsLabels.clear();
            metricsLabels.put("dataType", event.getDataType().typeName());
            metricsLabels.put("handler", handler.getClass().getName());
            metricsService.collect(Metric.MILLIS_IN_HANDLER, metricsLabels.get(), fields, handlerTime);

            if (contextWriter instanceof KeyValueCountingContextWriter) {
                ((KeyValueCountingContextWriter) contextWriter).writeMetrics(event, fields, handler);
            }
        }
    }

    public ContextWriter<K2,V2> getContextWriter() {
        return this.contextWriter;
    }

    public Map<String,List<DataTypeHandler<K1>>> getHandlerMap() {
        return this.typeMap;
    }

}
