package datawave.query.planner;

import static datawave.query.iterator.QueryOptions.QUERY;
import static datawave.query.iterator.QueryOptions.RANGES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.webservice.query.configuration.QueryData;

/**
 * Base representation of a query plan
 */
public class QueryPlan {

    protected JexlNode queryTree = null;
    protected String queryTreeString = null;
    protected Collection<Range> ranges = null;
    protected Collection<String> columnFamilies = Lists.newArrayList();
    protected List<IteratorSetting> settings = Lists.newArrayList();
    protected int hashCode;

    public QueryPlan() {
        // empty constructor for when using a builder-like pattern
    }

    /**
     * Preferred full constructor
     *
     * @param queryTree
     *            the query tree
     * @param ranges
     *            a collection of ranges
     * @param columnFamilies
     *            a collection of column families
     * @param settings
     *            a list of IteratorSetting
     */
    public QueryPlan(JexlNode queryTree, Collection<Range> ranges, Collection<String> columnFamilies, List<IteratorSetting> settings) {
        this.queryTree = queryTree;
        this.queryTreeString = JexlStringBuildingVisitor.buildQueryWithoutParse(queryTree);
        this.ranges = ranges;
        this.columnFamilies = columnFamilies;
        this.settings = settings;
        resetHashCode();
    }

    /**
     * Alternate full constructor, accepts a query string instead of a JexlNode
     *
     * @param queryString
     *            the query string
     * @param ranges
     *            a collection of ranges
     * @param columnFamilies
     *            a collection of column families
     * @param settings
     *            a list of IteratorSetting
     */
    public QueryPlan(String queryString, Collection<Range> ranges, Collection<String> columnFamilies, List<IteratorSetting> settings) {
        this.queryTree = null;
        this.queryTreeString = queryString;
        this.ranges = ranges;
        this.columnFamilies = columnFamilies;
        this.settings = settings;
        resetHashCode();
    }

    /**
     * Copy constructor
     *
     * @param other
     *            a different QueryPlan
     */
    public QueryPlan(QueryPlan other) {
        this.queryTree = other.queryTree;
        this.queryTreeString = other.queryTreeString;
        this.ranges = new ArrayList<>(other.ranges);
        this.columnFamilies = new ArrayList<>(other.columnFamilies);
        this.settings = new ArrayList<>(other.settings);
        resetHashCode();
    }

    /**
     * Builder style method for setting the query tree
     *
     * @param node
     *            the root node of a query tree
     * @return this QueryPlan
     */
    public QueryPlan withQueryTree(JexlNode node) {
        this.queryTree = node;
        resetHashCode();
        return this;
    }

    /**
     * Builder style method for setting the query tree string
     *
     * @param queryString
     *            the query string
     * @return this QueryPlan
     */
    public QueryPlan withQueryString(String queryString) {
        this.queryTreeString = queryString;
        resetHashCode();
        return this;
    }

    /**
     * Builder style method for setting the ranges
     *
     * @param ranges
     *            a collection of ranges
     * @return this QueryPlan
     */
    public QueryPlan withRanges(Collection<Range> ranges) {
        this.ranges = ranges;
        resetHashCode();
        return this;
    }

    /**
     * Builder style method for setting the column families
     *
     * @param columnFamilies
     *            a collection of column families
     * @return this QueryPlan
     */
    public QueryPlan withColumnFamilies(Collection<String> columnFamilies) {
        this.columnFamilies = columnFamilies;
        resetHashCode();
        return this;
    }

    /**
     * Builder style method for setting the IteratorSetting
     *
     * @param settings
     *            a collection of IteratorSetting
     * @return this QueryPlan
     */
    public QueryPlan withSettings(List<IteratorSetting> settings) {
        this.settings = settings;
        resetHashCode();
        return this;
    }

    /**
     * Partial constructor, missing IteratorSetting
     *
     * @param queryTreeString
     *            the query string
     * @param queryTree
     *            the query tree
     * @param ranges
     *            the ranges
     * @deprecated
     */
    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryPlan(String queryTreeString, JexlNode queryTree, Iterable<Range> ranges) {
        this(queryTreeString, queryTree, ranges, null);
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryPlan(String queryTreeString, JexlNode queryTree, Iterable<Range> ranges, List<IteratorSetting> settings) {
        Preconditions.checkNotNull(queryTree);
        this.queryTree = queryTree;
        this.queryTreeString = queryTreeString;
        this.ranges = Lists.newArrayList(ranges);
        if (null != settings) {
            this.settings = settings;
        }
        resetHashCode();
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryPlan(JexlNode queryTree, Iterable<Range> ranges, Collection<String> columnFamilies) {
        Preconditions.checkNotNull(queryTree);
        this.queryTree = queryTree;
        this.ranges = Lists.newArrayList(ranges);
        this.columnFamilies = Lists.newArrayList(columnFamilies);
        resetHashCode();
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryPlan(JexlNode queryTree, Range range) {
        Preconditions.checkNotNull(queryTree);
        this.queryTree = queryTree;
        this.ranges = Lists.newArrayList(range);
        resetHashCode();
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryPlan(QueryData currentQueryData) throws ParseException {
        this.queryTreeString = currentQueryData.getQuery();
        this.ranges = Lists.newArrayList(currentQueryData.getRanges());
        this.settings.addAll(currentQueryData.getSettings());
        this.columnFamilies.addAll(currentQueryData.getColumnFamilies());
        resetHashCode();
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryPlan(JexlNode queryTree, Iterable<Range> rangeIter, List<IteratorSetting> settings, Collection<String> columnFamilies) {
        this.queryTree = queryTree;
        this.ranges = Lists.newArrayList(rangeIter);
        for (IteratorSetting setting : settings) {
            IteratorSetting newSetting = new IteratorSetting(setting.getPriority(), setting.getName(), setting.getIteratorClass());
            newSetting.addOptions(setting.getOptions());
            if (newSetting.getOptions().containsKey(QUERY)) {
                newSetting.addOption(QUERY, JexlStringBuildingVisitor.buildQuery(queryTree));
                newSetting.addOption(RANGES, this.ranges.stream().map(Range::toString).collect(Collectors.joining(",", "[", "]")));
            }
            this.settings.add(newSetting);

        }
        if (null != columnFamilies) {
            this.columnFamilies.addAll(columnFamilies);
        }
        resetHashCode();
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public QueryPlan(JexlNode queryTree, Iterable<Range> rangeIter, List<IteratorSetting> settings) {
        this(queryTree, rangeIter, settings, null);
    }

    public JexlNode getQueryTree() {
        if (null == queryTree) {
            Preconditions.checkNotNull(queryTreeString);
            try {
                queryTree = JexlASTHelper.parseAndFlattenJexlQuery(queryTreeString);
                resetHashCode();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        return queryTree;
    }

    public void setQueryTree(JexlNode queryTree) {
        this.queryTree = queryTree;
        resetHashCode();
    }

    public void setQueryTreeString(String queryString) {
        this.queryTreeString = queryString;
        resetHashCode();
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public void setQuery(String queryString, JexlNode queryTree) {
        this.queryTree = queryTree;
        this.queryTreeString = queryString;
        resetHashCode();
    }

    @Deprecated(since = "6.5.0", forRemoval = true)
    public void setQuery(String queryString, ASTJexlScript queryTree) {
        this.queryTree = queryTree;
        this.queryTreeString = queryString;
        resetHashCode();
    }

    public String getQueryString() {
        if (null == queryTreeString) {
            Preconditions.checkNotNull(queryTree);
            queryTreeString = JexlStringBuildingVisitor.buildQuery(queryTree);
            resetHashCode();
        }
        return queryTreeString;
    }

    public Iterable<Range> getRanges() {
        return ranges;
    }

    public void setRanges(Collection<Range> ranges) {
        this.ranges.clear();
        this.ranges.addAll(ranges);
        resetHashCode();
    }

    public void addRanges(Collection<Range> ranges) {
        this.ranges.addAll(ranges);
    }

    public void addRange(Range range) {
        ranges.add(range);
    }

    public void addRanges(Iterable<Range> ranges) {
        Iterables.addAll(this.ranges, ranges);
    }

    public Collection<String> getColumnFamilies() {
        return columnFamilies;
    }

    public void addColumnFamily(String cf) {
        columnFamilies.add(cf);
        resetHashCode();
    }

    public List<IteratorSetting> getSettings() {
        return settings;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueryPlan) {
            QueryPlan other = (QueryPlan) obj;
            //  @formatter:off
            return new EqualsBuilder()
                            .append(queryTree, other.queryTree)
                            .append(queryTreeString, other.queryTreeString)
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
                            .append(queryTree)
                            .append(queryTreeString)
                            .append(ranges)
                            .append(columnFamilies)
                            .append(settings)
                            .hashCode();
            //  @formatter:on
        }
        return hashCode;
    }

    /**
     * Resets the hashcode. This method is called when an update is made.
     */
    private void resetHashCode() {
        hashCode = -1;
    }

    @Override
    public String toString() {
        return (ranges + getQueryString() + columnFamilies).intern();
    }
}
