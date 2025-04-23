package datawave.query;

public class QueryParameters {

    /**
     * Set of datatypes to limit the query to.
     */
    public static final String DATATYPE_FILTER_SET = "datatype.filter.set";

    /**
     * Include the hierarchy fields such as PARENT_UID and CHILD_COUNT
     */
    public static final String INCLUDE_HIERARCHY_FIELDS = "include.hierarchy.fields";

    /**
     * Only return data as represented in the DB. This setting was originally created for the purpose of the mutable metadata service. To ensure you are seeing
     * the actual data that can be mutated, this option may help. This is a master setting which will automatically do the following and possibly more as
     * required in the future: modelName is removed include hierarchy fields will be set false include datatype as field will be set false transform content
     * fields will be set false raw.types will be set true include grouping context will be set true.
     */
    public static final String RAW_DATA_ONLY = "raw.data.only";

    /**
     * Override the transformation of the content fields to contain the UID.
     */
    public static final String TRANSFORM_CONTENT_TO_UID = "transform.content.to.uid";

    /**
     * Allows user to specify query syntax (i.e. JEXL or LUCENE)
     */
    public static final String QUERY_SYNTAX = "query.syntax";

    /**
     * Allows user to specify the query profile (Options are defined in QueryLogicFactory)
     */
    public static final String QUERY_PROFILE = "query.profile";

    /**
     * ??
     */
    public static final String NON_EVENT_KEY_PREFIXES = "non.event.key.prefixes";

    /**
     * ??
     */
    public static final String DISALLOWLISTED_FIELDS = "disallowlisted.fields";

    /**
     * The list of fields to return
     */
    public static final String RETURN_FIELDS = "return.fields";

    /**
     * The list of fields to rename (post all model mappings)
     */
    public static final String RENAME_FIELDS = "rename.fields";

    /**
     * Should masked values be filtered out when the unmasked variant is available
     */
    public static final String FILTER_MASKED_VALUES = "filter.masked.values";

    /**
     * Should the DATATYPE field be included in the results?
     */
    public static final String INCLUDE_DATATYPE_AS_FIELD = "include.datatype.as.field";

    /**
     * Should the RECORD_ID field be included in the results (default is true)
     */
    public static final String INCLUDE_RECORD_ID = "include.record.id";

    /**
     * Should the grouping portion of the fieldnames be returned as well. These are numbers tagged onto the end of the field names to denote what group they
     * belong to. This is helpful when determining which foo values goes with which bar value when an event contains multiple foo and bar values.
     */
    public static final String INCLUDE_GROUPING_CONTEXT = "include.grouping.context";

    /**
     * Should the CHILD_COUNT field be included in the results?
     */
    public static final String INCLUDE_CHILD_COUNT = "include.child.count";

    /**
     * Should the PARENT field be included in the results. The PARENT field will contain the shardid/datatype/uid of the parent record/event.
     */
    public static final String INCLUDE_PARENT = "include.parent";

    /**
     * The classname of the return uid mapper class. To return the parent document, this should be datawave.core.iterators.uid.ParentDocumentUidMapper. To
     * return the top level document, this should be datawave.core.iterators.uid.TopLevelDocumentUidMapper.
     */
    public static final String RETURN_UID_MAPPER = "return.uid.mapper";

    /**
     * The classname of the query context uid mapper class. To change the context of the query to be an entire document versus a single event, this should be
     * datawave.core.iterators.uid.TopLevelDocumentUidMapper.
     */
    public static final String QUERY_CONTEXT_UID_MAPPER = "query.context.uid.mapper";

    /**
     * ??
     */
    public static final String FILTERING_ENABLED = "filtering.enabled";

    /**
     * ??
     */
    public static final String FILTERING_CLASSES = "filtering.classes";

    /**
     * ??
     */
    public static final String UNEVALUATED_FIELDS = "filtering.unevaluated.fields";

    /**
     * The name of the model to use. The model will map a fieldname to 1 or more underlying fieldnames.
     */
    public static final String PARAMETER_MODEL_NAME = "model.name";

    /**
     * The table in which the model specified above is stored.
     */
    public static final String PARAMETER_MODEL_TABLE_NAME = "model.table.name";

    /**
     * Used by the content query table to pull back a specific view of the data
     */
    public static final String CONTENT_VIEW_NAME = "content.view.name";

    /**
     * Used by the content query table to pull back content documents for parent and all children
     */
    public static final String CONTENT_VIEW_ALL = "content.view.all";

    /**
     * Used by the ContentQueryLogic to determine if views should be decoded from base64
     */
    public static final String DECODE_VIEW = "content.view.decode";

    /**
     * Used to specify the class used to perform visibility interpretations into markings.
     */
    public static final String VISIBILITY_INTERPRETER = "visibility.interpreter";

    /**
     * Used to limit the number of values returned for specific fields
     */
    public static final String LIMIT_FIELDS = "limit.fields";
    /**
     * Used to tie field groups together such that if a field in one group is not being limited the fields in matching groups will not be limited.
     */
    public static final String MATCHING_FIELD_SETS = "matching.field.sets";

    /**
     * Used to specify fields to perform a group-by with.
     */
    public static final String GROUP_FIELDS = "group.fields";

    /**
     * Used to specify the fields for which a sum should be calculated in groups resulting from a group-by operation.
     */
    public static final String SUM_FIELDS = "sum.fields";

    /**
     * Used to specify the fields for which the max should be found in groups resulting from a group-by operation.
     */
    public static final String MAX_FIELDS = "max.fields";

    /**
     * Used to specify the fields for which the min should be found in groups resulting from a group-by operation.
     */
    public static final String MIN_FIELDS = "min.fields";

    /**
     * Used to specify the fields for which a count should be calculated in groups resulting from a group-by operation.
     */
    public static final String COUNT_FIELDS = "count.fields";

    /**
     * Used to specify the fields for which an average should be calculated in groups resulting from a group-by operation.
     */
    public static final String AVERAGE_FIELDS = "average.fields";

    public static final String GROUP_FIELDS_BATCH_SIZE = "group.fields.batch.size";
    public static final String UNIQUE_FIELDS = "unique.fields";
    public static final String MOST_RECENT_UNIQUE = "most.recent.unique";

    /**
     * Used to specify fields which are excluded from QueryModel expansion
     */
    public static final String NO_EXPANSION_FIELDS = "no.expansion.fields";

    /**
     * Used to cause Documents to contain a list of selectors that hit;
     */
    public static final String HIT_LIST = "hit.list";

    /**
     * The type of dates used in the date range. Leaving blank will use the default.
     */
    public static final String DATE_RANGE_TYPE = "date.type";

    public static final String RAW_TYPES = "raw.types";

    public static final String DATE_INDEX_TIME_TRAVEL = "date.index.time.travel";

    public static final String IGNORE_NONEXISTENT_FIELDS = "ignore.nonexistent.fields";

    /**
     * Used to specify a SHARDS_AND_DAYS hint within the options function.
     */
    public static final String SHARDS_AND_DAYS = "shards.and.days";

    /**
     * Used to specify phrase excerpts that should be returned.
     */
    public static final String EXCERPT_FIELDS = "excerpt.fields";

    /**
     * Used to specify summaries that should be returned.
     */
    public static final String SUMMARY_OPTIONS = "summary.options";

    /**
     * Used to specify model or DB fields that should be treated as lenient (can be skipped if normalization fails)
     */
    public static final String LENIENT_FIELDS = "lenient.fields";

    /**
     * Used to specify model or DB fields that must be treated as strict (cannot be skipped if normalization fails)
     */
    public static final String STRICT_FIELDS = "strict.fields";
}
