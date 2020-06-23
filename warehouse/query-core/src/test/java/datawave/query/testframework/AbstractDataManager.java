package datawave.query.testframework;

import datawave.data.normalizer.GeoNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.NumberNormalizer;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Base class for all raw data managers. Each manager is responsible for managing the data for one or more sets of datatype entries.
 */
public abstract class AbstractDataManager implements RawDataManager {
    
    private static final Logger log = Logger.getLogger(AbstractDataManager.class);
    
    /**
     * Defines the key field name.
     */
    private final String rawKeyField;
    /**
     * Defines the name of the shard id field.
     */
    private final String shardDate;
    /**
     * Mapping of datatype to the raw entries that should match the datatype entries in Accumulo. This data will be used to determine the expected results.
     */
    protected final Map<String,Set<RawData>> rawData;
    /**
     * Mapping of datatype to the indexes for the datatype.
     */
    protected final Map<String,Set<String>> rawDataIndex;
    /**
     * Configured shard id values for computing range requests.
     */
    protected ShardIdValues shardValues;
    
    /**
     * Mapping of the lowercase field name to the metadata associated with a field. Classes should populate the metadata list.
     */
    protected Map<String,RawMetaData> metadata;
    
    /**
     *
     * @param keyField
     *            key field returned for results validation
     * @param shardField
     *            field name containing shard id
     */
    protected AbstractDataManager(final String keyField, final String shardField) {
        this(keyField, shardField, null, SHARD_ID_VALUES);
    }
    
    protected AbstractDataManager(final String keyField, final String shardField, final Map<String,RawMetaData> metaDataMap) {
        this(keyField, shardField, metaDataMap, SHARD_ID_VALUES);
    }
    
    protected AbstractDataManager(final String keyField, final String shardField, final Map<String,RawMetaData> metaDataMap, final ShardIdValues shardInfo) {
        this.rawKeyField = keyField.toLowerCase();
        this.rawData = new HashMap<>();
        this.rawDataIndex = new HashMap<>();
        this.shardDate = shardField.toLowerCase();
        this.metadata = metaDataMap;
        this.shardValues = shardInfo;
    }
    
    @Override
    public Iterator<Map<String,String>> rangeData(Date start, Date end) {
        log.debug("start(" + start + ") end(" + end + ")");
        final Set<Map<String,String>> raw = new HashSet<>();
        for (final Map.Entry<String,Set<RawData>> entry : this.rawData.entrySet()) {
            for (final RawData rawEntry : entry.getValue()) {
                String dateStr = rawEntry.getValue(this.shardDate);
                try {
                    Date shardDate = DataTypeHadoopConfig.YMD_DateFormat.parse(dateStr);
                    
                    if (0 >= start.compareTo(shardDate) && 0 <= end.compareTo(shardDate)) {
                        final Set<Map<String,String>> matchers = rawEntry.getMapping();
                        raw.addAll(matchers);
                    }
                } catch (ParseException pe) {
                    log.error(pe);
                    Assert.fail("invalid shard date value(" + dateStr + ")");
                }
            }
        }
        
        return raw.iterator();
    }
    
    @Override
    public Date[] getRandomStartEndDate() {
        return this.shardValues.getStartEndDates(true);
    }
    
    @Override
    public Date[] getShardStartEndDate() {
        return this.shardValues.getStartEndDates(false);
    }
    
    @Override
    public Normalizer getNormalizer(String field) {
        return this.metadata.containsKey(field.toLowerCase()) ? this.metadata.get(field.toLowerCase()).normalizer : null;
    }
    
    @Override
    public String convertAnyField(final String phrase) {
        return convertAnyField(phrase, OR_OP);
    }
    
    @Override
    public String convertAnyField(final String phrase, final String op) {
        Set<String> fieldIndexes = new HashSet<>();
        for (Set<String> val : this.rawDataIndex.values()) {
            fieldIndexes.addAll(val);
        }
        // opStr will be empty until the first term is added
        String opStr = "";
        
        final StringBuilder buf = new StringBuilder("(");
        for (String field : fieldIndexes) {
            // assume field is added
            try {
                if (!(getNormalizer(field) instanceof GeoNormalizer)) {
                    if (getNormalizer(field) instanceof NumberNormalizer) {
                        // remove quotes from phrase
                        String num = "";
                        String numPhrase = phrase;
                        int start = phrase.indexOf('\'');
                        if (0 <= start) {
                            num = phrase.substring(start + 1);
                            if (num.endsWith("'")) {
                                num = num.substring(0, num.length() - 1);
                            }
                            numPhrase = phrase.substring(0, start) + num;
                        } else {
                            String[] split = phrase.split(" ");
                            num = split[split.length - 1];
                        }
                        
                        // if it can't be parsed as an int then ignore it
                        Integer.parseInt(num);
                        buf.append(opStr).append(field).append(" ").append(numPhrase);
                    } else {
                        buf.append(opStr).append(field).append(" ").append(phrase);
                    }
                    opStr = " " + op + " ";
                }
            } catch (NumberFormatException nfe) {
                // don't add numeric field if it is not a valid numeric
            }
        }
        
        buf.append(")");
        return buf.toString();
    }
    
    @Override
    public Set<String> getKeys(Set<Map<String,String>> entries) {
        final Set<String> keys = new HashSet<>(entries.size());
        for (Map<String,String> entry : entries) {
            keys.add(entry.get(this.rawKeyField));
        }
        
        return keys;
    }
}
