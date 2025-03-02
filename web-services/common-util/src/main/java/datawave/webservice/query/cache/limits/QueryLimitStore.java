package datawave.webservice.query.cache.limits;

public interface QueryLimitStore {
    /**
     * Set Query limit for a given DN
     *
     * @param dn
     *            DN for user/system.
     * @param limit
     *            max number of concurrent queries
     */
    Integer setQueryLimit(final String dn, int limit);

    /**
     * Current query limit for the DN.
     *
     * @param dn
     *            User or System DN.
     * @param defaultValue
     *            Default concurrent query limit.
     * @return Integer value.
     */
    Integer getQueryLimit(final String dn, Integer defaultValue);
}
