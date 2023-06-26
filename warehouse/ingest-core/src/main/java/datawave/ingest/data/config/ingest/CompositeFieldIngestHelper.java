package datawave.ingest.data.config.ingest;

import com.google.common.collect.Multimap;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

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
        compositeFieldNormalizer.setup(type, config);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.CompositeIngest#getCompositeToFieldMap()
     */
    @Override
    public Multimap<String,String> getCompositeFieldDefinitions() {
        return compositeFieldNormalizer.getCompositeToFieldMap();
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.CompositeIngest#getCompositeFieldSeparators()
     */
    @Override
    public Map<String,String> getCompositeFieldSeparators() {
        return compositeFieldNormalizer.getCompositeFieldSeparators();
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.CompositeIngest#setCompositeToFieldMap(java.util.Map)
     */
    @Override
    public void setCompositeFieldDefinitions(Multimap<String,String> compositeFieldDefinitions) {
        compositeFieldNormalizer.setCompositeToFieldMap(compositeFieldDefinitions);
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
        return this.getCompositeFieldDefinitions().containsKey(fieldName);
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
