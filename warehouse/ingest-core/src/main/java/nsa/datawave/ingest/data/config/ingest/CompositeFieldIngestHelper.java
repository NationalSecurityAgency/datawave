package nsa.datawave.ingest.data.config.ingest;

import java.util.Map;

import nsa.datawave.ingest.data.Type;
import nsa.datawave.ingest.data.config.NormalizedContentInterface;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

/**
 * This class will add the CompositeFieldNormalizer to the list of normalizers. Note that this can be done directly via the configuration.
 * 
 * 
 */
public class CompositeFieldIngestHelper implements CompositeIngest {
    
    private Type type;
    private CompositeFieldNormalizer compositeFieldNormalizer = new CompositeFieldNormalizer();
    
    public CompositeFieldIngestHelper(Type type) {
        this.type = type;
    }
    
    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        compositeFieldNormalizer.setup(type, null, config);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.ingest.data.config.ingest.CompositeIngest#getCompositeFieldDefinitions()
     */
    @Override
    public Map<String,String[]> getCompositeFieldDefinitions() {
        return compositeFieldNormalizer.getCompositeFieldDefinitions();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.ingest.data.config.ingest.CompositeIngest#setCompositeFieldDefinitions(java.util.Map)
     */
    @Override
    public void setCompositeFieldDefinitions(Map<String,String[]> compositeFieldDefinitions) {
        compositeFieldNormalizer.setCompositeFieldDefinitions(compositeFieldDefinitions);
    }
    
    /**
     * @deprecated
     */
    public String getSep() {
        return compositeFieldNormalizer.getSep();
    }
    
    /**
     * @deprecated
     */
    public void setSep(String sep) {
        compositeFieldNormalizer.setSep(sep);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.ingest.data.config.ingest.CompositeIngest#getDefaultSeparator()
     */
    @Override
    public String getDefaultCompositeFieldSeparator() {
        return compositeFieldNormalizer.getDefaultSeparator();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.ingest.data.config.ingest.CompositeIngest#setDefaultSeparator(java.lang.String)
     */
    @Override
    public void setDefaultCompositeFieldSeparator(String sep) {
        compositeFieldNormalizer.setDefaultSeparator(sep);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.ingest.data.config.ingest.CompositeIngest#getCompositeFields(com.google.common.collect.Multimap)
     */
    @Override
    public Multimap<String,NormalizedContentInterface> getCompositeFields(Multimap<String,NormalizedContentInterface> fields) {
        return compositeFieldNormalizer.normalizeMap(fields);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.ingest.data.config.ingest.CompositeIngest#isCompositeField(java.lang.String)
     */
    @Override
    public boolean isCompositeField(String fieldName) {
        Map<String,String[]> map = this.getCompositeFieldDefinitions();
        return map.containsKey(fieldName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.ingest.data.config.ingest.CompositeIngest#getCompositeNameAndIndex(java.lang.String)
     */
    @Override
    public Map<String,String[]> getCompositeNameAndIndex(String compositeFieldName) {
        return this.getCompositeFieldDefinitions();
    }
}
