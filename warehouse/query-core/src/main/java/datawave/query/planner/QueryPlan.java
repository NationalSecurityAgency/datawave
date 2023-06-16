package datawave.query.planner;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import datawave.query.iterator.QueryIterator;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.configuration.QueryData;

/**
 * Base representation of a query plan
 */
public class QueryPlan {

    private static final Logger log = ThreadConfigurableLogger.getLogger(QueryPlan.class);

    protected JexlNode queryTree = null;
    protected String queryTreeString = null;
    protected List<Range> ranges = null;
    protected int hashCode;
    protected List<String> columnFamilies = Lists.newArrayList();
    protected List<IteratorSetting> settings = Lists.newArrayList();

    public QueryPlan(String queryTreeString, JexlNode queryTree, Iterable<Range> ranges) {
        this(queryTreeString, queryTree, ranges, null);
    }

    public QueryPlan(String queryTreeString, JexlNode queryTree, Iterable<Range> ranges, List<IteratorSetting> settings) {
        Preconditions.checkNotNull(queryTree);
        this.queryTree = queryTree;
        this.queryTreeString = queryTreeString;
        this.ranges = Lists.newArrayList(ranges);
        if (null != settings)
            this.settings = settings;
        buildHashCode();
    }

    public QueryPlan(JexlNode queryTree, Iterable<Range> ranges, Collection<String> columnFamilies) {
        Preconditions.checkNotNull(queryTree);
        this.queryTree = queryTree;
        this.ranges = Lists.newArrayList(ranges);
        this.columnFamilies = Lists.newArrayList(columnFamilies);
        buildHashCode();
    }

    public QueryPlan(JexlNode queryTree, Range range) {
        Preconditions.checkNotNull(queryTree);
        this.queryTree = queryTree;
        this.ranges = Lists.newArrayList(range);
        buildHashCode();
    }

    public void setQuery(String queryString, JexlNode queryTree) {
        this.queryTree = queryTree;
        this.queryTreeString = queryString;
        buildHashCode();
    }

    private void buildHashCode() {

        HashCodeBuilder builder = new HashCodeBuilder();

        if (null != queryTree) {
            builder = builder.append(queryTree);
        } else if (null != queryTreeString) {
            builder = builder.append(queryTreeString);
        }

        for (Range range : ranges) {
            builder = builder.append(range);
        }

        for (String cf : columnFamilies) {
            builder = builder.append(cf);
        }

        builder.append(settings);

        hashCode = builder.toHashCode();

    }

    public QueryPlan(QueryData currentQueryData) throws ParseException {
        this.queryTreeString = currentQueryData.getQuery();
        this.ranges = Lists.newArrayList(currentQueryData.getRanges());
        this.settings.addAll(currentQueryData.getSettings());
        this.columnFamilies.addAll(currentQueryData.getColumnFamilies());
        buildHashCode();
    }

    public QueryPlan(JexlNode queryTree, Iterable<Range> rangeIter, List<IteratorSetting> settings, Collection<String> columnFamilies) {
        this.queryTree = queryTree;
        this.ranges = Lists.newArrayList(rangeIter);
        for (IteratorSetting setting : settings) {
            IteratorSetting newSetting = new IteratorSetting(setting.getPriority(), setting.getName(), setting.getIteratorClass());
            newSetting.addOptions(setting.getOptions());
            if (newSetting.getOptions().containsKey(QueryIterator.QUERY)) {
                newSetting.addOption(QueryIterator.QUERY, JexlStringBuildingVisitor.buildQuery(queryTree));
                newSetting.addOption(QueryIterator.RANGES, this.ranges.stream().map(Range::toString).collect(Collectors.joining(",", "[", "]")));
            }
            this.settings.add(newSetting);

        }
        if (null != columnFamilies)
            this.columnFamilies.addAll(columnFamilies);
        buildHashCode();
    }

    public QueryPlan(JexlNode queryTree, Iterable<Range> rangeIter, List<IteratorSetting> settings) {
        this(queryTree, rangeIter, settings, null);
    }

    public JexlNode getQueryTree() {
        if (null == queryTree) {
            Preconditions.checkNotNull(queryTreeString);

            try {
                queryTree = JexlASTHelper.parseAndFlattenJexlQuery(queryTreeString);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

        }
        return queryTree;
    }

    public String getQueryString() {
        if (null == queryTreeString) {
            Preconditions.checkNotNull(queryTree);
            queryTreeString = JexlStringBuildingVisitor.buildQuery(queryTree);
        }
        return queryTreeString;
    }

    public void addColumnFamily(String cf) {
        columnFamilies.add(cf);
    }

    public Collection<String> getColumnFamilies() {
        return columnFamilies;
    }

    public void addRange(Range range) {
        ranges.add(range);
        buildHashCode();
    }

    public void addRanges(Collection<Range> ranges) {
        this.ranges.addAll(ranges);
        buildHashCode();
    }

    public void addRanges(Iterable<Range> ranges) {
        Iterables.addAll(this.ranges, ranges);
        buildHashCode();
    }

    public void setRanges(Collection<Range> ranges) {
        this.ranges.clear();
        addRanges(ranges);
    }

    public Iterable<Range> getRanges() {
        return ranges;
    }

    public List<IteratorSetting> getSettings() {
        return settings;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueryPlan) {
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(columnFamilies, ((QueryPlan) obj).columnFamilies);
            return hashCode == ((QueryPlan) obj).hashCode && equalsBuilder.append(ranges, ((QueryPlan) obj).ranges).isEquals();

        } else
            return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(ranges).append(getQueryString()).append(columnFamilies).toString().intern();
    }

    public void setQuery(String queryString, ASTJexlScript queryTree) {
        this.queryTree = queryTree;
        this.queryTreeString = queryString;
        buildHashCode();

    }

}
