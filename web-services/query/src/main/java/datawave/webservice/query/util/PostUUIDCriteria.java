package datawave.webservice.query.util;

import org.springframework.util.MultiValueMap;

/**
 * Lookup criteria for one or more UUIDs
 */
public class PostUUIDCriteria extends AbstractUUIDLookupCriteria {
    private final String uuidPairs;

    public PostUUIDCriteria(final String uuidPairs, MultiValueMap<String,String> queryParameters) {
        super(queryParameters);
        this.uuidPairs = uuidPairs;
    }

    @Override
    public String getRawQueryString() {
        return this.uuidPairs;
    }
}
