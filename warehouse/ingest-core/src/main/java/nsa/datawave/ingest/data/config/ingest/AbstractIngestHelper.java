package nsa.datawave.ingest.data.config.ingest;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import nsa.datawave.ingest.data.config.DataTypeHelperImpl;
import nsa.datawave.ingest.data.config.MaskedFieldHelper;

import org.apache.log4j.Logger;

/**
 * Specialization of the Helper type that validates the configuration for Ingest purposes. These helper classes also have the logic to parse the field names and
 * fields values from the datatypes that they represent.
 * 
 * 
 * 
 */
public abstract class AbstractIngestHelper extends DataTypeHelperImpl implements IngestHelperInterface {
    private static final Logger log = Logger.getLogger(AbstractIngestHelper.class);
    
    protected boolean deleteMode = false;
    protected boolean replaceMalformedUTF8 = false;
    protected DataTypeHelperImpl embeddedHelper = null;
    
    /* Map of field names to normalizers, null key is the default normalizer */
    protected MaskedFieldHelper mfHelper = null;
    protected Map<String,String> normalizedMaskedValues = null;
    protected Map<String,String> maskedValues = null;
    protected Set<String> unMaskedValues = null;
    protected Set<String> shardExclusions = new HashSet<>();
    protected boolean hasIndexBlacklist = false;
    protected boolean hasReverseIndexBlacklist = false;
    
    public boolean getReplaceMalformedUTF8() {
        return replaceMalformedUTF8;
    }
    
    public boolean getDeleteMode() {
        return deleteMode;
    }
    
    public DataTypeHelperImpl getEmbeddedHelper() {
        return embeddedHelper;
    }
    
    public void setEmbeddedHelper(DataTypeHelperImpl embeddedHelper) {
        this.embeddedHelper = embeddedHelper;
    }
    
    /**
     * 
     * @return map of field names to normalized masked values
     */
    public Map<String,String> getNormalizedMaskedValues() {
        return normalizedMaskedValues;
    }
    
    /**
     * 
     * @return map of field names to masked values (NOT NORMALIZED)
     */
    public Map<String,String> getMaskedValues() {
        return maskedValues;
    }
    
    /**
     * 
     * @return true if EmbeddedHelper is an instance of MaskedFieldHelper
     */
    public boolean isEmbeddedHelperMaskedFieldHelper() {
        return (null != mfHelper);
    }
    
    /**
     * 
     * @return EmbeddedHelper as a MaskedFieldHelper object
     */
    public MaskedFieldHelper getEmbeddedHelperAsMaskedFieldHelper() {
        return mfHelper;
    }
    
    public Set<String> getShardExclusions() {
        return shardExclusions;
    }
    
    protected void setHasIndexBlacklist(boolean hasIndexBlacklist) {
        this.hasIndexBlacklist = hasIndexBlacklist;
    }
    
    protected boolean hasIndexBlacklist() {
        return this.hasIndexBlacklist;
    }
    
    protected boolean hasReverseIndexBlacklist() {
        return this.hasReverseIndexBlacklist;
    }
    
    protected void setHasReverseIndexBlacklist(boolean hasReverseIndexBlacklist) {
        this.hasReverseIndexBlacklist = hasReverseIndexBlacklist;
    }
    
    public void upperCaseSetEntries(Set<String> input, String warnMessage) {
        Set<String> removeList = new TreeSet<>();
        Set<String> addList = new TreeSet<>();
        for (String s : input) {
            if (!s.toUpperCase().equals(s)) {
                removeList.add(s);
                addList.add(s.toUpperCase());
                log.warn(" has a value " + s + "that was converted to uppercase.");
            }
        }
        input.removeAll(removeList);
        input.addAll(addList);
    }
    
}
