package datawave.ingest.data.config.ingest;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * This class will add the VirtualFieldNormalizer to the list of normalizers. Note that this can be done directly via the configuration.
 *
 *
 *
 */
public class VirtualFieldIngestHelper implements VirtualIngest {

    protected VirtualFieldNormalizer virtualFieldNormalizer = new VirtualFieldNormalizer();
    protected Multimap<String,NormalizedContentInterface> fields;

    protected Type type;

    public VirtualFieldIngestHelper(Type type) {
        this.type = type;
    }

    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        virtualFieldNormalizer.setup(type, null, config);
    }

    @Override
    public Map<String,String[]> getVirtualFieldDefinitions() {
        return virtualFieldNormalizer.getVirtualFieldDefinitions();
    }

    @Override
    public void setVirtualFieldDefinitions(Map<String,String[]> virtualFieldDefinitions) {
        virtualFieldNormalizer.setVirtualFieldDefinitions(virtualFieldDefinitions);
    }

    @Override
    public String getDefaultVirtualFieldSeparator() {
        return virtualFieldNormalizer.getDefaultSeparator();
    }

    @Override
    public void setDefaultVirtualFieldSeparator(String sep) {
        virtualFieldNormalizer.setDefaultSeparator(sep);
    }

    @Override
    public Multimap<String,NormalizedContentInterface> getVirtualFields(Multimap<String,NormalizedContentInterface> fields) {
        this.fields = fields;
        return virtualFieldNormalizer.normalizeMap(fields);
    }

    @Override
    public boolean isVirtualIndexedField(String fieldName) {
        Map<String,String[]> map = this.getVirtualFieldDefinitions();
        return map.containsKey(fieldName);
    }

    @Override
    public Map<String,String[]> getVirtualNameAndIndex(String virtualFieldName) {
        return this.getVirtualFieldDefinitions();
    }

    public void setVirtualFieldNormalizer(VirtualFieldNormalizer virtualFieldNormalizer) {
        this.virtualFieldNormalizer = virtualFieldNormalizer;
    }

    public VirtualFieldNormalizer getVirtualFieldNormalizer() {
        return this.virtualFieldNormalizer;
    }

    public Multimap<String,NormalizedContentInterface> getFields() {
        return fields;
    }

    public void setFields(Multimap<String,NormalizedContentInterface> fields) {
        this.fields = fields;
    }

}
