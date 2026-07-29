package datawave.webservice.query.limit;

import com.google.common.base.Preconditions;

import datawave.zookeeper.ObjectValidator;

public class QueryLimitConfigurationValidator implements ObjectValidator {

    @Override
    public void validate(Object object) {
        Preconditions.checkArgument((object instanceof QueryLimitConfiguration), "Object must be an instance of " + QueryLimitConfiguration.class.getName());
        QueryLimitConfigurationValidationUtils.validate((QueryLimitConfiguration) object);
    }
}
