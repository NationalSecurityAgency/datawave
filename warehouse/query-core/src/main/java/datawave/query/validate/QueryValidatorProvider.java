package datawave.query.validate;

import java.util.List;

public interface QueryValidatorProvider {

    List<QueryValidator> getValidators();
}
