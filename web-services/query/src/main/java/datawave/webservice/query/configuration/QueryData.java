package datawave.webservice.query.configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Class to encapsulate all required information to run a query.
 */
public class QueryData {
    private String query;
    private Collection<Range> ranges = new HashSet<>();
    private Collection<String> columnFamilies = new HashSet<>();
    private List<IteratorSetting> settings = new ArrayList<>();
    private int hashCode = -1;

    public QueryData() {
        // empty constructor
    }

    /**
     * Full constructor
     *
     * @param query
     *            the query string
     * @param ranges
     *            a collection of ranges
     * @param columnFamilies
     *            a collection of column families
     * @param settings
     *            a list of IteratorSetting
     */
    public QueryData(String query, Collection<Range> ranges, Collection<String> columnFamilies, List<IteratorSetting> settings) {
        this.query = query;
        this.ranges = ranges;
        this.columnFamilies = columnFamilies;
        this.settings = settings;
    }

    /**
     * Copy constructor
     *
     * @param other
     *            another instance of QueryData
     */
    public QueryData(QueryData other) {
        this.query = other.query;
        this.ranges = new HashSet<>(other.ranges);
        this.columnFamilies = new HashSet<>(other.columnFamilies);
        this.settings = new ArrayList<>(other.settings);
        this.hashCode = other.hashCode;
    }

    public QueryData withQuery(String query) {
        setQuery(query);
        return this;
    }

    public QueryData withRanges(Collection<Range> ranges) {
        setRanges(ranges);
        return this;
    }

    public QueryData withColumnFamilies(Collection<String> columnFamilies) {
        setColumnFamilies(columnFamilies);
        return this;
    }

    public QueryData withSettings(List<IteratorSetting> settings) {
        setSettings(settings);
        return this;
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryData(String query, Collection<Range> ranges, List<IteratorSetting> settings) {
        setQuery(query);
        setRanges(ranges);
        setSettings(settings);
    }

    /**
     * Weak copy constructor that updates the ranges
     *
     * @param other
     *            another QueryData
     * @param ranges
     *            a collection of updated ranges
     * @deprecated
     */
    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryData(QueryData other, Collection<Range> ranges) {
        setQuery(other.getQuery());
        setSettings(other.getSettings());
        setRanges(ranges);
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryData(String queryString, List<Range> ranges, List<IteratorSetting> settings, Collection<String> columnFamilies) {
        this(queryString, ranges, settings);
        this.columnFamilies.addAll(columnFamilies);
    }

    public List<IteratorSetting> getSettings() {
        return settings;
    }

    public void setSettings(List<IteratorSetting> settings) {
        this.settings = new ArrayList<>(settings);
        resetHashCode();
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
        resetHashCode();
    }

    public Collection<Range> getRanges() {
        return ranges;
    }

    public Collection<String> getColumnFamilies() {
        return columnFamilies;
    }

    public void setColumnFamilies(Collection<String> columnFamilies) {
        this.columnFamilies = columnFamilies;
        resetHashCode();
    }

    public void setRanges(Collection<Range> ranges) {
        this.ranges = ranges;
        resetHashCode();
    }

    public void addIterator(IteratorSetting cfg) {
        this.settings.add(cfg);
        hashCode = -1;
    }

    @Override
    public String toString() {
        //  @formatter:off
        return new StringBuilder()
                        .append("Query: '").append(this.query)
                        .append("', Ranges: ").append(this.ranges)
                        .append(", Settings: ").append(this.settings)
                        .toString();
        //  @formatter:on
    }

    public boolean equals(Object o) {
        if (o instanceof QueryData) {
            QueryData other = (QueryData) o;
            //  @formatter:off
            return new EqualsBuilder()
                            .append(query, other.query)
                            .append(ranges, other.ranges)
                            .append(columnFamilies, other.columnFamilies)
                            .append(settings, other.settings)
                            .isEquals();
            //  @formatter:on
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (hashCode == -1) {
            //  @formatter:off
            hashCode = new HashCodeBuilder()
                            .append(query)
                            .append(ranges)
                            .append(columnFamilies)
                            .append(settings)
                            .hashCode();
            //  @formatter:on
        }
        return hashCode;
    }

    /**
     * Method to reset the hashcode when an internal variable is updated
     */
    private void resetHashCode() {
        hashCode = -1;
    }
}
