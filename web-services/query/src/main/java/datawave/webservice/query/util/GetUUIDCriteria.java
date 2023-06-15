package datawave.webservice.query.util;

import javax.ws.rs.core.MultivaluedMap;

/**
 * Lookup criteria for one and only one UUID
 */
public class GetUUIDCriteria extends AbstractUUIDLookupCriteria {
    protected final String uuid;
    protected final String uuidType;

    public GetUUIDCriteria(final String uuid, final String uuidType, MultivaluedMap<String,String> queryParameters) {
        super(queryParameters);

        this.uuid = uuid;
        this.uuidType = uuidType;
    }

    @Override
    public String getRawQueryString() {
        return this.uuidType + LookupUUIDUtil.UUID_TERM_DELIMITER + LookupUUIDUtil.QUOTE + this.uuid + LookupUUIDUtil.QUOTE;
    }
}
