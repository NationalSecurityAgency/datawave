package datawave.query.testframework;

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
 * Base class for all raw data managers. Each manager is responsible for managing the data for one or more datatype.
 */
public abstract class AbstractDataManager implements IRawDataManager {
    
    private static final Logger log = Logger.getLogger(AbstractDataManager.class);
    
    /**
     * Defines the key field name.
     */
    private final String rawKeyField;
    /**
     * Defines the name of the shard id field.
     */
    private final String shardId;
    /**
     * Mapping of datatype to the raw entries that should match the datatype entries in Accumulo.
     */
    protected final Map<String,Set<IRawData>> rawData;
    /**
     * Mapping of datatype to the indexes for the datatype.
     */
    protected final Map<String,Set<String>> rawDataIndex;
    
    /**
     * Mapping of the field lowercase field name to the metadata associated with a field. Classes should populate the metadata list.
     */
    protected Map<String,BaseRawData.RawMetaData> metadata = new HashMap<>();
    
    /**
     *
     * @param keyField
     *            key field returned for results validation
     */
    AbstractDataManager(final String keyField, final String shardField) {
        this.rawKeyField = keyField.toLowerCase();
        this.rawData = new HashMap<>();
        this.rawDataIndex = new HashMap<>();
        this.shardId = shardField.toLowerCase();
    }
    
    @Override
    public Iterator<Map<String,String>> rangeData(Date start, Date end) {
        log.debug("start(" + start.toString() + ") end(" + end.toString() + ")");
        final Set<Map<String,String>> raw = new HashSet<>();
        for (final Map.Entry<String,Set<IRawData>> entry : this.rawData.entrySet()) {
            for (final IRawData rawEntry : entry.getValue()) {
                String dateStr = rawEntry.getValue(this.shardId);
                try {
                    Date shardDate = IDataTypeHadoopConfig.YMD_DateFormat.parse(dateStr);
                    
                    if (0 >= start.compareTo(shardDate) && 0 <= end.compareTo(shardDate)) {
                        final Set<Map<String,String>> matchers = rawEntry.getMapping();
                        int n = 0;
                        for (Map<String,String> m : matchers) {
                            n += m.size();
                        }
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
    public Normalizer getNormalizer(String field) {
        return metadata.containsKey(field.toLowerCase()) ? metadata.get(field.toLowerCase()).normalizer : null;
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
        boolean addOp = false;
        final StringBuilder buf = new StringBuilder("(");
        for (String field : fieldIndexes) {
            if (addOp) {
                buf.append(" ").append(op).append(" ");
            } else {
                addOp = true;
            }
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
                try {
                    // if it can't be parsed as an int then ignore it
                    Integer.parseInt(num);
                    buf.append(field).append(" ").append(numPhrase);
                } catch (NumberFormatException nfe) {
                    // don't add numeric field
                    addOp = false;
                }
            } else {
                buf.append(field).append(" ").append(phrase);
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
