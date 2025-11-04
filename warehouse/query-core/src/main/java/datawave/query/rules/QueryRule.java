package datawave.query.rules;

public interface QueryRule {

    /**
     * Return the name of this {@link QueryRule}.
     *
     * @return the name
     */
    String getName();

    /**
     * Set the name of this {@link QueryRule}.
     *
     * @param name
     *            the name
     */
    void setName(String name);

    /**
     * Returns whether this {@link QueryRule} can validate its criteria against the given configuration via
     * {@link QueryRule#validate(QueryValidationConfiguration)}.
     *
     * @param configuration
     *            the configuration
     * @return true if this rule can validate the configuration, or false otherwise
     */
    boolean canValidate(QueryValidationConfiguration configuration);

    /**
     * Validates the given query against the criteria of this {@link QueryRule} and returns a list of messages detailing any issues.
     *
     * @param configuration
     *            the query validation configuration to use when validating the given query
     * @return the details of any issues found within the query
     * @throws Exception
     *             if any exception occurs
     */
    QueryRuleResult validate(QueryValidationConfiguration configuration) throws Exception;

    /**
     * Returns a copy of this {@link QueryRule}.
     *
     * @return the clone
     */
    public QueryRule copy();
}
