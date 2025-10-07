package datawave.webservice.query.util;

import org.springframework.util.MultiValueMap;

/**
 * Criteria for one and only one UIDQuery-based lookup
 */
public class UIDQueryCriteria extends GetUUIDCriteria {
    public UIDQueryCriteria(final String uuid, final String uuidType, MultiValueMap<String,String> queryParameters) {
        super(uuid, uuidType, queryParameters);
    }

    @Override
    public String getRawQueryString() {
        return this.uuidType + LookupUUIDUtil.UUID_TERM_DELIMITER + this.uuid; // No quotes!
    }
}
