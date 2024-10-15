package datawave.query.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.count.CountMap;

/**
 * Base representation of a query plan
 */
public class QueryPlan {

    private static final Logger log = ThreadConfigurableLogger.getLogger(QueryPlan.class);

    protected String tableName = null;
    protected JexlNode queryTree = null;
    protected String queryTreeString = null;
    protected Collection<Range> ranges = null;
    protected Collection<String> columnFamilies = new ArrayList<>();
    protected List<IteratorSetting> settings = new ArrayList<>();
    protected CountMap termCounts;
    protected CountMap fieldCounts;

    protected boolean rebuildHashCode = true;
    protected int hashCode;

    public QueryPlan() {
        // empty constructor for when using a builder-like pattern
    }

    /**
     * Preferred full constructor
     *
     * @param tableName
     *            the table name
     * @param queryTree
     *            the query tree
     * @param ranges
     *            a collection of ranges
     * @param columnFamilies
     *            a collection of column families
     * @param settings
     *            a list of IteratorSetting
     */
    public QueryPlan(String tableName, JexlNode queryTree, Collection<Range> ranges, Collection<String> columnFamilies, List<IteratorSetting> settings) {
        this.tableName = tableName;
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
     * @param tableName
     *            the table name
     * @param queryString
     *            the query string
     * @param ranges
     *            a collection of ranges
     * @param columnFamilies
     *            a collection of column families
     * @param settings
     *            a list of IteratorSetting
     */
    public QueryPlan(String tableName, String queryString, Collection<Range> ranges, Collection<String> columnFamilies, List<IteratorSetting> settings) {
        this.tableName = tableName;
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
        this.tableName = other.tableName;
        this.queryTree = other.queryTree;
        this.queryTreeString = other.queryTreeString;
        this.ranges = new ArrayList<>(other.ranges);
        this.columnFamilies = new ArrayList<>(other.columnFamilies);
        this.settings = new ArrayList<>(other.settings);
        this.fieldCounts = other.fieldCounts;
        this.termCounts = other.termCounts;
        this.hashCode = other.hashCode;
        this.rebuildHashCode = other.rebuildHashCode;
    }

    public QueryPlan(JexlNode queryTree, Collection<Range> ranges) {
        this.queryTree = queryTree;
        this.ranges = ranges;
        this.columnFamilies = new ArrayList<>();
        this.settings = new ArrayList<>();
        resetHashCode();
    }

    // builder methods

    /**
     * Builder style method for setting the table name
     *
     * @param tableName
     *            the table name
     * @return this QueryPlan
     */
    public QueryPlan withTableName(String tableName) {
        this.tableName = tableName;
        resetHashCode();
        return this;
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

    public QueryPlan withFieldCounts(CountMap fieldCounts) {
        this.fieldCounts = fieldCounts;
        resetHashCode();
        return this;
    }

    public QueryPlan withTermCounts(CountMap termCounts) {
        this.termCounts = termCounts;
        resetHashCode();
        return this;
    }

    public JexlNode getQueryTree() {
        if (null == queryTree) {
            try {
                Preconditions.checkNotNull(queryTreeString);
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

    public String getQueryString() {
        if (null == queryTreeString) {
            Preconditions.checkNotNull(queryTree);
            queryTreeString = JexlStringBuildingVisitor.buildQuery(queryTree);
            resetHashCode();
        }
        return queryTreeString;
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

    public Collection<Range> getRanges() {
        return ranges;
    }

    public void addColumnFamily(String cf) {
        columnFamilies.add(cf);
        resetHashCode();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get the settings
     *
     * @return the settings
     */
    public List<IteratorSetting> getSettings() {
        return settings;
    }

    public CountMap getFieldCounts() {
        return this.fieldCounts;
    }

    public CountMap getTermCounts() {
        return this.termCounts;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueryPlan) {
            QueryPlan other = (QueryPlan) obj;

            if (hashCode == other.hashCode) {
                return true; // short circuit on precomputed hashcode
            }

            //  @formatter:off
            return new EqualsBuilder()
                            .append(tableName, other.tableName)
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
        if (rebuildHashCode) {
            //  @formatter:off
            hashCode = new HashCodeBuilder()
                            .append(tableName)
                            .append(queryTree)
                            .append(queryTreeString)
                            .append(ranges)
                            .append(columnFamilies)
                            .append(settings)
                            .hashCode();
            //  @formatter:on
            rebuildHashCode = false;
        }
        return hashCode;
    }

    /**
     * Resets the hashcode. This method is called when an update is made.
     */
    private void resetHashCode() {
        rebuildHashCode = true;
    }

    @Override
    public String toString() {
        return (tableName + ranges + getQueryString() + columnFamilies).intern();
    }
}
