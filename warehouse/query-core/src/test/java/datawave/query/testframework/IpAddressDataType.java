package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.IpAddressType;
import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.data.config.CSVHelper;
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

/**
 * Data configuration for IP address data.
 */
public class IpAddressDataType extends AbstractDataTypeConfig {
    
    private static final Logger log = Logger.getLogger(IpAddressDataType.class);
    
    public enum IpAddrEntry {
        // predefined ip address data
        ipbase("input/ipaddress.csv", "ipaddr");
        
        private final String ingestFile;
        private final String datatype;
        
        IpAddrEntry(final String file, final String name) {
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
    
    public enum IpAddrShardId {
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
                        for (final IpAddrShardId id : IpAddrShardId.values()) {
                            dates.add(id.date);
                        }
                        shardInfo = new ShardInfo(dates);
                    }
                }
            }
            
            return shardInfo.getStartEndDates(random);
        }
        
        IpAddrShardId(final String str) {
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
            return Stream.of(IpAddrShardId.values()).map(e -> e.getShardId()).collect(Collectors.toList());
        }
    }
    
    public enum IpAddrField {
        // maintain correct order for input csv files
        START_DATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        EVENT_ID(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        PUBLIC_IP(Normalizer.IP_ADDRESS_NORMALIZER, true),
        PRIVATE_IP(Normalizer.IP_ADDRESS_NORMALIZER),
        LOCATION(Normalizer.LC_NO_DIACRITICS_NORMALIZER),
        PLANET(Normalizer.LC_NO_DIACRITICS_NORMALIZER, true);
        
        private static final List<String> Headers;
        
        static {
            Headers = Stream.of(IpAddrField.values()).map(e -> e.name()).collect(Collectors.toList());
        }
        
        /**
         * Retrieves the enumeration that matches the specified field.
         *
         * @param field
         *            string representation of the field as returned by {@link #name}
         * @return enumeration value
         * @throws AssertionError
         *             field does not match any of the enumeration values
         */
        public static IpAddrField getField(final String field) {
            for (final IpAddrField f : IpAddrField.values()) {
                if (f.name().equalsIgnoreCase(field)) {
                    return f;
                }
            }
            
            throw new AssertionError("invalid field(" + field + ")");
        }
        
        public static List<String> headers() {
            return Headers;
        }
        
        private static final Map<String,BaseRawData.RawMetaData> metadataMapping = new HashMap<>();
        
        private BaseRawData.RawMetaData metadata;
        
        IpAddrField(final Normalizer<?> normalizer) {
            this(normalizer, false);
        }
        
        IpAddrField(final Normalizer<?> normalizer, final boolean isMulti) {
            this.metadata = new BaseRawData.RawMetaData(this.name(), normalizer, isMulti);
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
    private static final RawDataManager manager = new IpAddressDataManager();
    
    public static RawDataManager getManager() {
        return manager;
    }
    
    /**
     * Creates an ip address datatype entry with all of the key/value configuration settings.
     *
     * @param addr
     *            entry for ingest containing datatype and ingest file
     * @param config
     *            hadoop field configuration
     * @throws IOException
     *             unable to load ingest file
     * @throws URISyntaxException
     *             unable to resolve ingest file
     */
    public IpAddressDataType(final IpAddrEntry addr, final FieldConfig config) throws IOException, URISyntaxException {
        this(addr.getDatatype(), addr.getIngestFile(), config);
    }
    
    /**
     * Constructor for ip address ingest files that are not defined in the class {@link IpAddressDataType.IpAddrEntry}.
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
    public IpAddressDataType(final String datatype, final String ingestFile, final FieldConfig config) throws IOException, URISyntaxException {
        super(datatype, ingestFile, config, manager);
        
        // NOTE: see super for default settings
        // set datatype settings
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_NAME, IpAddrField.START_DATE.name());
        this.hConf.set(this.dataType + EventRecordReader.Properties.EVENT_DATE_FIELD_FORMAT, DATE_FIELD_FORMAT);
        
        this.hConf.set(this.dataType + ExtendedCSVHelper.Properties.EVENT_ID_FIELD_NAME, IpAddrField.EVENT_ID.name());
        
        // set type for ip address
        this.hConf.set(this.dataType + "." + IpAddrField.PUBLIC_IP.name() + ".data.field.type.class", IpAddressType.class.getName());
        this.hConf.set(this.dataType + "." + IpAddrField.PRIVATE_IP.name() + ".data.field.type.class", IpAddressType.class.getName());
        
        // fields
        this.hConf.set(this.dataType + CSVHelper.DATA_HEADER, String.join(",", IpAddrField.headers()));
        
        log.debug(this.toString());
    }
    
    @Override
    public Collection<String> getShardIds() {
        return IpAddrShardId.shards();
    }
    
    @Override
    public String toString() {
        return "IpAddressDataType{" + super.toString() + "}";
    }
}
