package datawave.core.query.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Iterators;
import datawave.core.common.util.EnvProvider;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.util.TableName;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.security.Authorizations;

import javax.xml.bind.annotation.XmlTransient;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * A basic query configuration object that contains the information needed to run a query.
 * </p>
 *
 * <p>
 * Provides some "expected" default values for parameters. This configuration object also encapsulates iterators and their options that would be set on a
 * {@link BatchScanner}.
 * </p>
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class GenericQueryConfiguration implements Serializable {
    // is this execution expected to be checkpointable (changes how we allocate ranges to scanners)
    private boolean checkpointable = false;

    private transient AccumuloClient client = null;

    // This is just used for (de)serialization
    private Set<String> auths = Collections.emptySet();

    private Set<Authorizations> authorizations = Collections.singleton(Authorizations.EMPTY);

    private Query query = null;

    // Leave in a top-level query for backwards-compatibility purposes
    private String queryString = null;

    private Date beginDate = null;
    private Date endDate = null;

    // The max number of next + seek calls made by the underlying iterators
    private Long maxWork = -1L;

    protected int baseIteratorPriority = 100;

    // Table name
    private String tableName = TableName.SHARD;

    private Collection<QueryData> queries = Collections.emptyList();

    @JsonIgnore
    private transient Iterator<QueryData> queriesIter = Collections.emptyIterator();
    protected boolean bypassAccumulo;

    // use a value like 'env:PASS' to pull from the environment
    private String accumuloPassword = "";

    /**
     * Empty default constructor
     */
    public GenericQueryConfiguration() {

    }

    /**
     * Pulls the table name, max query results, and max rows to scan from the provided argument
     *
     * @param configuredLogic
     *            A pre-configured BaseQueryLogic to initialize the Configuration with
     */
    public GenericQueryConfiguration(BaseQueryLogic<?> configuredLogic) {
        this(configuredLogic.getConfig());
    }

    public GenericQueryConfiguration(GenericQueryConfiguration genericConfig) {
        this.setQuery(genericConfig.getQuery());
        this.setCheckpointable(genericConfig.isCheckpointable());
        this.setBaseIteratorPriority(genericConfig.getBaseIteratorPriority());
        this.setBypassAccumulo(genericConfig.getBypassAccumulo());
        this.setAccumuloPassword(genericConfig.getAccumuloPassword());
        this.setAuthorizations(genericConfig.getAuthorizations());
        this.setBeginDate(genericConfig.getBeginDate());
        this.setClient(genericConfig.getClient());
        this.setEndDate(genericConfig.getEndDate());
        this.setMaxWork(genericConfig.getMaxWork());
        this.setQueries(genericConfig.getQueries());
        this.setQueriesIter(genericConfig.getQueriesIter());
        this.setQueryString(genericConfig.getQueryString());
        this.setTableName(genericConfig.getTableName());
    }

    public Collection<QueryData> getQueries() {
        return queries;
    }

    public void setQueries(Collection<QueryData> queries) {
        this.queries = queries;
    }

    /**
     * Return the configured {@code Iterator<QueryData>}
     *
     * @return An iterator of query ranges
     */
    public Iterator<QueryData> getQueriesIter() {
        if ((queriesIter == null || !queriesIter.hasNext()) && queries != null) {
            return Iterators.unmodifiableIterator(queries.iterator());
        } else {
            return Iterators.unmodifiableIterator(this.queriesIter);
        }
    }

    /**
     * Set the queries to be run.
     *
     * @param queriesIter
     *            An iterator of query ranges
     */
    public void setQueriesIter(Iterator<QueryData> queriesIter) {
        this.queriesIter = queriesIter;
    }

    public boolean isCheckpointable() {
        return checkpointable;
    }

    public void setCheckpointable(boolean checkpointable) {
        this.checkpointable = checkpointable;
    }

    @JsonIgnore
    @XmlTransient
    public AccumuloClient getClient() {
        return client;
    }

    public void setClient(AccumuloClient client) {
        this.client = client;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setQueryString(String query) {
        this.queryString = query;
    }

    public String getQueryString() {
        return queryString;
    }

    public Set<String> getAuths() {
        if (auths == null && authorizations != null) {
            auths = authorizations.stream().flatMap(a -> a.getAuthorizations().stream()).map(b -> new String(b, StandardCharsets.UTF_8))
                            .collect(Collectors.toSet());
        }
        return auths;
    }

    public void setAuths(Set<String> auths) {
        this.auths = auths;
        this.authorizations = null;
        getAuthorizations();
    }

    @JsonIgnore
    public Set<Authorizations> getAuthorizations() {
        if (authorizations == null && auths != null) {
            authorizations = Collections
                            .singleton(new Authorizations(auths.stream().map(a -> a.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList())));
        }
        return authorizations;
    }

    public void setAuthorizations(Set<Authorizations> authorizations) {
        this.authorizations = authorizations;
        this.auths = null;
        getAuths();
    }

    public int getBaseIteratorPriority() {
        return baseIteratorPriority;
    }

    public void setBaseIteratorPriority(final int baseIteratorPriority) {
        this.baseIteratorPriority = baseIteratorPriority;
    }

    public Date getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(Date beginDate) {
        this.beginDate = beginDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Long getMaxWork() {
        return maxWork;
    }

    public void setMaxWork(Long maxWork) {
        this.maxWork = maxWork;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean getBypassAccumulo() {
        return bypassAccumulo;
    }

    public void setBypassAccumulo(boolean bypassAccumulo) {
        this.bypassAccumulo = bypassAccumulo;
    }

    /**
     * @return - the accumulo password
     */
    public String getAccumuloPassword() {
        return this.accumuloPassword;
    }

    /**
     * Sets configured password for accumulo access
     *
     * @param password
     *            the password used to connect to accumulo
     */
    public void setAccumuloPassword(String password) {
        this.accumuloPassword = EnvProvider.resolve(password);
    }

    /**
     * Checks for non-null, sane values for the configured values
     *
     * @return True if all of the encapsulated values have legitimate values, otherwise false
     */
    public boolean canRunQuery() {
        // Ensure we were given connector and authorizations
        if (null == this.getClient() || null == this.getAuthorizations()) {
            return false;
        }

        // Ensure valid dates
        if (null == this.getBeginDate() || null == this.getEndDate() || endDate.before(beginDate)) {
            return false;
        }

        // A non-empty table was given
        if (null == getTableName() || this.getTableName().isEmpty()) {
            return false;
        }

        // At least one QueryData was provided
        if (null == this.getQueriesIter()) {
            return false;
        }

        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GenericQueryConfiguration that = (GenericQueryConfiguration) o;
        return isCheckpointable() == that.isCheckpointable() && getBaseIteratorPriority() == that.getBaseIteratorPriority()
                        && getBypassAccumulo() == that.getBypassAccumulo() && Objects.equals(getAuthorizations(), that.getAuthorizations())
                        && Objects.equals(getQuery(), that.getQuery()) && Objects.equals(getQueryString(), that.getQueryString())
                        && Objects.equals(getBeginDate(), that.getBeginDate()) && Objects.equals(getEndDate(), that.getEndDate())
                        && Objects.equals(getMaxWork(), that.getMaxWork()) && Objects.equals(getTableName(), that.getTableName())
                        && Objects.equals(getQueries(), that.getQueries());
    }

    @Override
    public int hashCode() {
        return Objects.hash(isCheckpointable(), getAuthorizations(), getQuery(), getQueryString(), getBeginDate(), getEndDate(), getMaxWork(),
                        getBaseIteratorPriority(), getTableName(), getQueries(), getBypassAccumulo());
    }
}
