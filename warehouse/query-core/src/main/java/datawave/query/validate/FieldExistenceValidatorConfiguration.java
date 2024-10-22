package datawave.query.validate;

import com.google.common.collect.Sets;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.util.MetadataHelper;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import java.util.Set;

public class FieldExistenceValidatorConfiguration extends AbstractQueryValidatorConfiguration {
    
    private Set<String> specialFields;
    
    public Set<String> getSpecialFields() {
        return specialFields;
    }
    
    public void setSpecialFields(Set<String> specialFields) {
        this.specialFields = specialFields;
    }
    
    @Override
    public QueryValidatorConfiguration getBaseCopy() {
        FieldExistenceValidatorConfiguration configuration = new FieldExistenceValidatorConfiguration();
        configuration.setSpecialFields(Sets.newHashSet(this.specialFields));
        return configuration;
    }
}
