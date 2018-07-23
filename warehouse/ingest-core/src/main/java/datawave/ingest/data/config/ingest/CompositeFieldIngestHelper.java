package datawave.ingest.data.config.ingest;

import com.google.common.collect.Multimap;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.apache.hadoop.conf.Configuration;

import java.util.Date;
import java.util.Map;
import java.util.Set;

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
     * @see datawave.ingest.data.config.ingest.CompositeIngest#getCompositeFieldDefinitions()
     */
    @Override
    public Map<String,String[]> getCompositeFieldDefinitions() {
        return compositeFieldNormalizer.getCompositeFieldDefinitions();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.ingest.data.config.ingest.CompositeIngest#setCompositeFieldDefinitions(java.util.Map)
     */
    @Override
    public void setCompositeFieldDefinitions(Map<String,String[]> compositeFieldDefinitions) {
        compositeFieldNormalizer.setCompositeFieldDefinitions(compositeFieldDefinitions);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.ingest.data.config.ingest.CompositeIngest#getCompositeFields(com.google.common.collect.Multimap)
     */
    @Override
    public Multimap<String,NormalizedContentInterface> getCompositeFields(Multimap<String,NormalizedContentInterface> fields) {
        return compositeFieldNormalizer.normalizeMap(fields);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.ingest.data.config.ingest.CompositeIngest#isCompositeField(java.lang.String)
     */
    @Override
    public boolean isCompositeField(String fieldName) {
        Map<String,String[]> map = this.getCompositeFieldDefinitions();
        return map.containsKey(fieldName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.ingest.data.config.ingest.CompositeIngest#isFixedLengthCompositeField(java.lang.String)
     */
    @Override
    public boolean isFixedLengthCompositeField(String fieldName) {
        Set<String> fixedLengthFields = compositeFieldNormalizer.getFixedLengthFields();
        return fixedLengthFields != null && fixedLengthFields.contains(fieldName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.ingest.data.config.ingest.CompositeIngest#isTransitionedCompositeField(java.lang.String)
     */
    @Override
    public boolean isTransitionedCompositeField(String fieldName) {
        Map<String,Date> fieldTransitionDateMap = compositeFieldNormalizer.getFieldTransitionDateMap();
        return fieldTransitionDateMap != null && fieldTransitionDateMap.containsKey(fieldName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.ingest.data.config.ingest.CompositeIngest#getCompositeFieldTransitionDate(java.lang.String)
     */
    @Override
    public Date getCompositeFieldTransitionDate(String fieldName) {
        Date transitionDate = null;
        if (isTransitionedCompositeField(fieldName))
            transitionDate = compositeFieldNormalizer.getFieldTransitionDateMap().get(fieldName);
        return transitionDate;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.ingest.data.config.ingest.CompositeIngest#isOverloadedCompositeField(java.lang.String)
     */
    @Override
    public boolean isOverloadedCompositeField(String fieldName) {
        return CompositeIngest.isOverloadedCompositeField(getCompositeFieldDefinitions(), fieldName);
    }
}
