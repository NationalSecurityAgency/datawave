package datawave.query.tables.shard;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterators.FieldIndexCountingIterator;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.FieldIndexCountQueryTransformer;
import datawave.query.util.MetadataHelper;
import datawave.util.CompositeTimestamp;
import datawave.util.StringUtils;
import datawave.webservice.query.exception.QueryException;

/**
 * Given a date range, FieldName(s), FieldValue(s), DataType(s) pull keys directly using FieldIndexIterator and count them as specified.
 *
 */
public class FieldIndexCountQueryLogic extends ShardQueryLogic {

    private static final Logger logger = Logger.getLogger(FieldIndexCountQueryLogic.class);
    private static final String DATA_FORMAT = "yyyyMMdd";
    private Collection<String> fieldNames;
    private Collection<String> fieldValues;
    private boolean uniqueByDataType = false;
    private boolean uniqueByVisibility = false;
    protected Long maxUniqueValues = 20000L;
    protected Collection<Range> ranges;

    public FieldIndexCountQueryLogic() {}

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {

        if (logger.isTraceEnabled()) {
            logger.trace("initialize");
        }

        MetadataHelper metadataHelper = prepareMetadataHelper(client, this.getMetadataTableName(), auths);
        String modelName = this.getModelName();
        String modelTableName = this.getModelTableName();
        // Check if the default modelName and modelTableNames have been overriden by custom parameters.
        if (null != settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME)
                        && !settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim().isEmpty()) {
            modelName = settings.findParameter(QueryParameters.PARAMETER_MODEL_NAME).getParameterValue().trim();
        }
        if (null != settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME)
                        && !settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim().isEmpty()) {
            modelTableName = settings.findParameter(QueryParameters.PARAMETER_MODEL_TABLE_NAME).getParameterValue().trim();
        }

        if (null != modelName && null == modelTableName) {
            throw new IllegalArgumentException(QueryParameters.PARAMETER_MODEL_NAME + " has been specified but " + QueryParameters.PARAMETER_MODEL_TABLE_NAME
                            + " is missing. Both are required to use a model");
        }

        if (null != modelName && null != modelTableName) {
            this.queryModel = metadataHelper.getQueryModel(modelTableName, modelName, this.getUnevaluatedFields());
        }

        // I'm using this config object in a pinch, we should probably create a custom one.
        ShardQueryConfiguration config = ShardQueryConfiguration.create(this, settings);
        config.setClient(client);
        config.setAuthorizations(auths);

        this.scannerFactory = new ScannerFactory(config);

        // the following throw IllegalArgumentExceptions if validation fails.
        parseQuery(config, settings);
        configDate(config, settings);
        configTypeFilter(config, settings);

        Set<String> normalizedFieldValues = null;
        Iterator<String> fieldNameIter = fieldNames.iterator();
        while (fieldNameIter.hasNext()) {
            String fieldName = fieldNameIter.next();
            // check that the field name is actually an indexed field
            Set<Type<?>> normalizerSet = metadataHelper.getDatatypesForField(fieldName, config.getDatatypeFilter());
            if (null != normalizerSet && !normalizerSet.isEmpty()) {
                if (null != this.fieldValues && !this.fieldValues.isEmpty()) {
                    for (Type<?> norm : normalizerSet) {
                        if (null == normalizedFieldValues) {
                            normalizedFieldValues = new HashSet<>();
                        }
                        for (String val : this.fieldValues) {
                            try {
                                String normVal = norm.normalize(val);
                                if (null != normVal && !normVal.isEmpty()) {
                                    normalizedFieldValues.add(normVal);
                                }
                            } catch (Exception e) {
                                logger.debug(norm + " failed to normalize value: " + val);
                            }
                        }
                    }
                }
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("dropping fieldname " + fieldName + " because it's not indexed.");
                }
                // drop fieldName since it isn't indexed.
                fieldNameIter.remove();
            }
        }
        this.fieldValues = normalizedFieldValues;

        if (this.fieldNames.isEmpty()) {
            throw new IllegalArgumentException("Need at least 1 indexed field to query with.");
        }

        // Generate & set the query ranges
        this.ranges = generateRanges(config);

        // Find out if we need to list unique data types.
        if (null != settings.findParameter(Constants.UNIQ_DATATYPE)) {
            this.uniqueByDataType = Boolean.parseBoolean(settings.findParameter(Constants.UNIQ_DATATYPE).getParameterValue());
            if (logger.isTraceEnabled()) {
                logger.trace("uniqueByDataType: " + uniqueByDataType);
            }
        }

        // Find out if we need to list unique visibilities.
        if (null != settings.findParameter(Constants.UNIQ_VISIBILITY)) {
            this.uniqueByVisibility = Boolean.parseBoolean(settings.findParameter(Constants.UNIQ_VISIBILITY).getParameterValue());
            if (logger.isTraceEnabled()) {
                logger.trace("uniqueByVisibility: " + uniqueByVisibility);
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("FieldNames: ");
            for (String f : this.fieldNames) {
                logger.trace("\t" + f);
            }
            logger.trace("FieldValues: ");
            if (null == this.fieldValues) {
                logger.trace("\tnone");
            } else {
                for (String f : this.fieldValues) {
                    logger.trace("\t" + f);
                }
            }
            logger.trace("uniqueByDataType: " + uniqueByDataType);
            logger.trace("uniqueByVisibility: " + uniqueByVisibility);
        }
        return config;
    }

    @Override
    public FieldIndexCountQueryLogic clone() {
        return new FieldIndexCountQueryLogic(this);
    }

    public FieldIndexCountQueryLogic(FieldIndexCountQueryLogic other) {
        super(other);
        this.maxUniqueValues = other.getMaxUniqueValues();
    }

    public Long getMaxUniqueValues() {
        return maxUniqueValues;
    }

    public void setMaxUniqueValues(Long maxUniqueValues) {
        this.maxUniqueValues = maxUniqueValues;
    }

    /**
     * Create the batch scanner and set the iterator options / stack.
     *
     * @param genericConfig
     *            configuration object
     * @throws Exception
     *             for any exceptions encountered
     */
    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("setupQuery");
        }
        if (!ShardQueryConfiguration.class.isAssignableFrom(genericConfig.getClass())) {
            throw new QueryException("Did not receive a ShardQueryConfiguration instance!!");
        }

        ShardQueryConfiguration config = (ShardQueryConfiguration) genericConfig;

        // Ensure we have all of the information needed to run a query
        if (!config.canRunQuery()) {
            logger.warn("The given query '" + config.getQueryString() + "' could not be run, most likely due to not matching any records in the global index.");

            // Stub out an iterator to correctly present "no results"
            this.iterator = new Iterator<Map.Entry<Key,Value>>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Map.Entry<Key,Value> next() {
                    return null;
                }

                @Override
                public void remove() {}
            };

            this.scanner = null;
            return;
        }

        try {
            if (logger.isTraceEnabled()) {
                logger.trace("configuring batch scanner and iterators.");
            }

            BatchScanner bs = getScannerFactory().newScanner(config.getShardTableName(), config.getAuthorizations(), config.getNumQueryThreads(),
                            config.getQuery());
            bs.setRanges(this.ranges);
            // The stack we want to use
            // 21 FieldIndexCountingIterator

            // FieldIndexCountingIterator setup
            IteratorSetting cfg;
            cfg = new IteratorSetting(config.getBaseIteratorPriority() + 21, "countingIter", FieldIndexCountingIterator.class);
            cfg.addOption(FieldIndexCountingIterator.DATA_TYPES, config.getDatatypeFilterAsString());
            cfg.addOption(FieldIndexCountingIterator.FIELD_NAMES, join(this.fieldNames, FieldIndexCountingIterator.SEP));
            if (null != this.fieldValues && !this.fieldValues.isEmpty()) {
                cfg.addOption(FieldIndexCountingIterator.FIELD_VALUES, join(this.fieldValues, FieldIndexCountingIterator.SEP));
            }
            SimpleDateFormat sdf = new SimpleDateFormat(FieldIndexCountingIterator.DATE_FORMAT_STRING);
            cfg.addOption(FieldIndexCountingIterator.START_TIME, sdf.format(config.getBeginDate()));
            cfg.addOption(FieldIndexCountingIterator.STOP_TIME, sdf.format(config.getEndDate()));
            cfg.addOption(FieldIndexCountingIterator.UNIQ_BY_DATA_TYPE, Boolean.toString(this.uniqueByDataType));

            bs.addScanIterator(cfg);

            this.iterator = bs.iterator();
            this.scanner = bs;
        } catch (TableNotFoundException e) {
            logger.error("The table '" + config.getShardTableName() + "' does not exist", e);
        }
    }

    private static String join(Collection<String> values, String sep) {
        StringBuilder b = new StringBuilder();
        for (String val : values) {
            b.append(val).append(sep);
        }
        b.deleteCharAt(b.length() - 1);
        return b.toString();
    }

    public SimpleDateFormat getDateFormatter() {
        return new SimpleDateFormat(DATA_FORMAT);
    }

    private void parseQuery(ShardQueryConfiguration config, Query settings) {
        if (logger.isTraceEnabled()) {
            logger.trace("parseQuery");
        }
        String query = settings.getQuery();
        String fieldName;
        String fieldValue = null;

        if (null == query) {
            throw new IllegalArgumentException("Query cannot be null");
        } else {
            query = query.trim();
            config.setQueryString(query);
        }
        int pos = query.indexOf(':');
        if (pos > -1) {
            fieldName = query.substring(0, pos).toUpperCase().trim();
            fieldValue = query.substring(pos + 1);
        } else {
            fieldName = query.toUpperCase().trim();
        }
        if (fieldName.isEmpty()) {
            throw new IllegalArgumentException("Query was empty or not parseable");
        }
        if (this.queryModel != null) {
            this.fieldNames = new ArrayList<>(queryModel.getMappingsForAlias(fieldName));
            if (this.fieldNames.isEmpty()) {
                // try using original fieldname
                this.fieldNames.add(fieldName);
            }
        } else {
            this.fieldNames = new ArrayList<>(Collections.singletonList(fieldName));
        }
        if (null == this.fieldNames || this.fieldNames.isEmpty()) {
            throw new IllegalArgumentException("Do not have valid field name to query on.");
        }

        // Limits to one field value. In the future we should expand this query
        // logic to do multiple since the underlying iterator can do it.
        if (null != fieldValue && !fieldValue.isEmpty()) {
            this.fieldValues = Collections.singletonList(fieldValue);
        }
    }

    private void configDate(ShardQueryConfiguration config, Query settings) {
        final Date beginDate = settings.getBeginDate();
        if (null == beginDate) {
            throw new IllegalArgumentException("Begin date cannot be null");
        } else {
            config.setBeginDate(beginDate);
        }

        final Date endDate = settings.getEndDate();
        if (null == endDate) {
            throw new IllegalArgumentException("End date cannot be null");
        } else {
            config.setEndDate(endDate);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("beginDate: " + beginDate + " , endDate: " + endDate);
        }
    }

    private void configTypeFilter(ShardQueryConfiguration config, Query settings) {
        // Get the datatype set if specified
        if (null == settings.findParameter(QueryParameters.DATATYPE_FILTER_SET)) {
            config.setDatatypeFilter(new HashSet<>());
            return;
        }

        String typeList = settings.findParameter(QueryParameters.DATATYPE_FILTER_SET).getParameterValue();
        HashSet<String> typeFilter;
        if (null != typeList && !typeList.isEmpty()) {
            typeFilter = new HashSet<>();
            typeFilter.addAll(Arrays.asList(StringUtils.split(typeList, Constants.PARAM_VALUE_SEP)));

            if (!typeFilter.isEmpty()) {
                config.setDatatypeFilter(typeFilter);

                if (logger.isDebugEnabled()) {
                    logger.debug("Type Filter: " + typeFilter);
                }
            }
        }
    }

    public Collection<Range> generateRanges(ShardQueryConfiguration config) {
        Calendar beginCal = Calendar.getInstance();
        Calendar endCal = Calendar.getInstance();
        beginCal.setTime(config.getBeginDate());
        endCal.setTime(config.getEndDate());
        String shardStart = this.getDateFormatter().format(beginCal.getTime());

        // Add an extra day to the range since the date formatter truncates it.
        // we want the ending key to be the next day.
        Calendar cal = Calendar.getInstance();
        cal.setTime(endCal.getTime());
        cal.add(Calendar.DAY_OF_MONTH, 1);
        String shardEnd = this.getDateFormatter().format(cal.getTime());
        if (shardStart.equalsIgnoreCase(shardEnd)) {
            endCal.add(Calendar.DATE, 1);
            shardEnd = this.getDateFormatter().format(endCal.getTime());
        }

        Range range = new Range(new Key(new Text(shardStart)), true, new Key(shardEnd), true);
        if (logger.isTraceEnabled()) {
            logger.trace("generateRanges: " + range);
        }
        return Collections.singletonList(range);
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new FieldIndexCountQueryTransformer(this, settings, this.markingFunctions, this.responseObjectFactory);
    }

    @Override
    public TransformIterator getTransformIterator(Query settings) {
        return new MyCountAggregatingIterator(this.iterator(), getTransformer(settings), this.maxUniqueValues);
    }

    public Map buildSummary(Iterator iter, long maxValues) {
        if (logger.isTraceEnabled()) {
            logger.trace("buildSummary");
        }
        Map<String,Tuple> summary = new HashMap<>();
        String cf;
        StringBuilder mapKeyBuilder = new StringBuilder();
        String mapKey;
        while (iter.hasNext()) {
            Object input = iter.next();

            if (input instanceof Entry<?,?>) {
                @SuppressWarnings("unchecked")
                Entry<Key,Value> entry = (Entry<Key,Value>) input;

                if (entry.getKey() == null && entry.getValue() == null) {
                    break;
                }

                if (null == entry.getKey() || null == entry.getValue()) {
                    throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
                }

                Key key = entry.getKey();
                Value val = entry.getValue();
                mapKeyBuilder.delete(0, mapKeyBuilder.length());

                // build out the map key
                cf = key.getColumnFamily().toString().substring(3); // strip off fi\x00
                if (queryModel != null) {
                    String aliasReverseName = queryModel.aliasFieldNameReverseModel(cf);
                    if (null == aliasReverseName || aliasReverseName.isEmpty()) {
                        mapKeyBuilder.append(cf);
                    } else {
                        mapKeyBuilder.append(aliasReverseName);
                    }
                } else {
                    mapKeyBuilder.append(cf);
                }

                mapKeyBuilder.append(Constants.NULL_BYTE_STRING);
                mapKeyBuilder.append(key.getColumnQualifier());
                mapKey = mapKeyBuilder.toString();

                // if we are over our threshold for unique values skip it.
                if (!summary.containsKey(mapKey) && summary.size() == maxValues) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("hit maxValues for unique field values");
                    }
                    continue;
                }

                Tuple tuple = summary.containsKey(mapKey) ? summary.get(mapKey) : new Tuple(super.getMarkingFunctions());
                tuple.aggregate(key, val);
                summary.put(mapKey, tuple);
            } else {
                throw new IllegalArgumentException("Invalid input type: " + input.getClass());
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("finished building summary, map has size: " + summary.size());
        }
        return summary;
    }

    public static class Tuple {

        private final MarkingFunctions tupleMarkingFunctions;
        private long count = 0L;
        private long maxTimestamp = 0L;
        Set<Text> uniqueVisibilities = new HashSet<>();

        public Tuple(MarkingFunctions mf) {
            tupleMarkingFunctions = mf;
        }

        public void aggregate(Key key, Value val) {
            uniqueVisibilities.add(key.getColumnVisibility());
            count += Long.parseLong(new String(val.get()));
            if (maxTimestamp < CompositeTimestamp.getEventDate(key.getTimestamp())) {
                maxTimestamp = CompositeTimestamp.getEventDate(key.getTimestamp());
            }
        }

        public long getCount() {
            return count;
        }

        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        public ColumnVisibility getColumnVisibility() {
            try {
                Set<ColumnVisibility> columnVisibilities = new HashSet<>();
                for (Text t : this.uniqueVisibilities) {
                    columnVisibilities.add(new ColumnVisibility(t));
                }
                return tupleMarkingFunctions.combine(columnVisibilities);

            } catch (MarkingFunctions.Exception e) {
                logger.error("Could not create combined column visibility for the count", e);
                return null;
            }
        }
    }

    public class MyCountAggregatingIterator extends TransformIterator {

        private boolean firstTime = true;
        private Iterator<Entry<Key,Value>> bsIter;
        private long maxValues;
        private Iterator iter; // iterator over the map object we create.

        public MyCountAggregatingIterator(Iterator<Entry<Key,Value>> iterator, Transformer transformer, long maxValues) {
            super(iterator, transformer);
            this.bsIter = iterator;
            this.maxValues = maxValues;
        }

        @Override
        public boolean hasNext() {
            if (firstTime) {
                // exhaust the batch scanner and aggregate all values.
                iter = buildSummary(bsIter, maxValues).entrySet().iterator();
                firstTime = false;
            }
            return iter.hasNext();
        }

        @Override
        public Object next() {
            return getTransformer().transform(iter.next());
        }
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> optionalParams = new TreeSet<>(super.getOptionalQueryParameters());
        optionalParams.add(QueryParameters.PARAMETER_MODEL_NAME);
        optionalParams.add(QueryParameters.PARAMETER_MODEL_TABLE_NAME);
        optionalParams.add(QueryParameters.DATATYPE_FILTER_SET);
        return optionalParams;
    }

}
