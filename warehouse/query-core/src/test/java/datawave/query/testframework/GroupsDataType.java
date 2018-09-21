package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.csv.mr.handler.ContentCSVColumnBasedHandler;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GroupsDataType extends AbstractDataTypeConfig {
    
    private static final Logger log = Logger.getLogger(GroupsDataType.class);
    
    public enum GroupsEntry {
        // predefined ip address data
        cities("input/city-groups.csv", "groups");
        
        private final String ingestFile;
        private final String datatype;
        
        GroupsEntry(final String file, final String name) {
            this.ingestFile = file;
            this.datatype = name;
        }
        
        private String getIngestFile() {
            return this.ingestFile;
        }
        
        private String getDatatype() {
            return this.datatype;
        }
    }
    
    public enum GroupsShardId {
        // list of shards for testing
        DATE_2015_0404("20150404"),
        DATE_2015_0505("20150505"),
        DATE_2015_0606("20150606"),
        DATE_2015_0707("20150707"),
        DATE_2015_0808("20150808"),
        DATE_2015_0909("20150909"),
        DATE_2015_1010("20151010"),
        DATE_2015_1111("20151111");
        
        private final String dateStr;
        private final Date date;
        
        static final Object sync = new Object();
        static ShardInfo shardInfo;
        
        static Date[] getStartEndDates(final boolean random) {
            // use double check locking
            if (null == shardInfo) {
                synchronized (sync) {
                    if (null == shardInfo) {
                        final List<Date> dates = new ArrayList<>();
                        for (final GroupsShardId id : GroupsShardId.values()) {
                            dates.add(id.date);
                        }
                        shardInfo = new ShardInfo(dates);
                    }
                }
            }
            
            return shardInfo.getStartEndDates(random);
        }
        
        GroupsShardId(final String str) {
            this.dateStr = str;
            try {
                this.date = YMD_DateFormat.parse(str);
            } catch (ParseException pe) {
                throw new AssertionError("invalid date string(" + str + ")");
            }
        }
        
        /**
         * Returns the accumulo shard id string representation.
         *
         * @return accumulo shard id
         */
        String getShardId() {
            return this.dateStr + "_0";
        }
        
        static Collection<String> shards() {
            return Stream.of(GroupsDataType.GroupsShardId.values()).map(e -> e.getShardId()).collect(Collectors.toList());
        }
    }
    
    /**
     * Grouping is handled differently as it will consist of a header field and a query field. The event entries in Accumulo are based upon the header value.
     * Indexes and queries are based upon the query value.
     */
    public enum GroupField {
        // maintain correct order for input csv files
        START_DATE("START_DATE", Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        EVENT_ID("EVENT_ID", Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        CITY_EAST("CITY.EAST", "CITY", Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        STATE_EAST("STATE.EAST", "STATE", Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        COUNT_EAST("COUNT.EAST", "COUNT", Normalizer.NUMBER_NORMALIZER),
        CITY_WEST("CITY.WEST", "CITY", Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        STATE_WEST("STATE.WEST", "STATE", Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        COUNT_WEST("COUNT.WEST", "COUNT", Normalizer.NUMBER_NORMALIZER),
        // field is used for testing tokens
        TOKENS("TOKENS", Normalizer.LC_NO_DIACRITICS_NORMALIZER);
        
        private static final List<String> headers;
        
        static {
            headers = Stream.of(GroupField.values()).map(e -> e.hdrField).collect(Collectors.toList());
        }
        
        public static List<String> headers() {
            return headers;
        }
        
        private static final Map<String,BaseRawData.RawMetaData> metadataMapping = new HashMap<>();
        
        /**
         * Retrieves the query field based upon the header field name.
         * 
         * @param header
         *            header field name
         * @return query field name
         */
        static String getQueryField(final String header) {
            for (GroupField group : GroupField.values()) {
                if (header.equalsIgnoreCase(group.hdrField)) {
                    return group.queryField;
                }
            }
            
            throw new AssertionError("invalid group header(" + header + ")");
        }
        
        /**
         * Returns true when a field should be tokenized.
         * 
         * @param header
         *            header field value
         * @return true for tokenized field
         */
        public static boolean isTokenField(final String header) {
            return TOKENS.getHdrField().equalsIgnoreCase(header);
        }
        
        private BaseRawData.RawMetaData metadata;
        private final String hdrField;
        private final String queryField;
        
        GroupField(final String name, final Normalizer<?> normalizer) {
            this(name, name, normalizer);
        }
        
        GroupField(final String headerField, final String queryField, final Normalizer<?> normalizer) {
            this.hdrField = headerField;
            this.queryField = queryField;
            this.metadata = new BaseRawData.RawMetaData(this.name().toLowerCase(), normalizer, false);
        }
        
        public String getQueryField() {
            return this.queryField;
        }
        
        String getHdrField() {
            return this.hdrField;
        }
        
        /**
         * Returns the metadata for this field.
         *
         * @return metadata
         */
        public BaseRawData.RawMetaData getMetadata() {
            return metadata;
        }
    }
    
    // ==================================
    // data manager info
    private static final RawDataManager manager = new GroupsDataManager();
    
    public static RawDataManager getManager() {
        return manager;
    }
    
    /**
     * Creates a groups datatype entry with all of the key/value configuration settings.
     *
     * @param group
     *            entry for ingest containing datatype and ingest file
     * @param config
     *            hadoop field configuration
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             unable to resolve ingest file
     */
    public GroupsDataType(final GroupsEntry group, final FieldConfig config) throws IOException, URISyntaxException {
        this(group.getDatatype(), group.getIngestFile(), config);
    }
    
    /**
     * Constructor for groups ingest files that are not defined in the class {@link GroupsEntry}.
     *
     * @param datatype
     *            name of the datatype
     * @param ingestFile
     *            ingest file path
     * @param config
     *            hadoop field configuration
     * @throws IOException
     *             error loading ingest data
     * @throws URISyntaxException
     *             ingest file name error
     */
    public GroupsDataType(final String datatype, final String ingestFile, final FieldConfig config) throws IOException, URISyntaxException {
        super(datatype, ingestFile, config, manager);
        
        // NOTE: see super for default settings
        // set datatype settings
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, GroupField.START_DATE.getHdrField());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_FORMAT, DATE_FIELD_FORMAT);
        
        this.hConf.set(this.dataType + ExtendedCSVHelper.Properties.EVENT_ID_FIELD_NAME, GroupField.EVENT_ID.getHdrField());
        
        this.hConf.set(this.dataType + "." + GroupField.COUNT_EAST.getQueryField() + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        
        // fields
        this.hConf.set(this.dataType + CSVHelper.DATA_HEADER, String.join(",", GroupField.headers()));
        
        this.hConf.set(this.dataType + "." + GroupField.TOKENS.name() + BaseIngestHelper.FIELD_TYPE, LcNoDiacriticsType.class.getName());
        
        // settings for tokens
        this.hConf.set(this.dataType + TypeRegistry.HANDLER_CLASSES, ContentCSVColumnBasedHandler.class.getName());
        this.hConf.set(this.dataType + ContentBaseIngestHelper.TOKEN_FIELDNAME_DESIGNATOR_ENABLED, "false");
        this.hConf.set(this.dataType + ContentBaseIngestHelper.TOKEN_INDEX_WHITELIST, GroupField.TOKENS.name());
        this.hConf.set(this.dataType + ContentBaseIngestHelper.TOKEN_REV_INDEX_WHITELIST, GroupField.TOKENS.name());
        
        log.debug(this.toString());
    }
    
    @Override
    public Collection<String> getShardIds() {
        return GroupsDataType.GroupsShardId.shards();
    }
    
    @Override
    public String toString() {
        return "GroupsDataType{" + super.toString() + "}";
    }
}
