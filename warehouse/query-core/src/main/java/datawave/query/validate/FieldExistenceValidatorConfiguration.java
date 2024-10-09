package datawave.query.validate;

import datawave.query.util.MetadataHelper;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import java.util.Set;

public class FieldExistenceValidatorConfiguration implements QueryValidatorConfiguration {
    
    @XmlElementWrapper(name = "specialFields")
    @XmlElement(name = "field")
    private Set<String> specialFields;
    
    private MetadataHelper metadataHelper;
    
    public Set<String> getSpecialFields() {
        return specialFields;
    }
    
    public void setSpecialFields(Set<String> specialFields) {
        this.specialFields = specialFields;
    }
    
    @Override
    public MetadataHelper getMetadataHelper() {
        return metadataHelper;
    }
    
    @Override
    public void setMetadataHelper(MetadataHelper metadataHelper) {
        this.metadataHelper = metadataHelper;
    }
}
