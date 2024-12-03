package datawave.query.jexl.util;

import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;

public abstract class AbstractQueryGenerator implements QueryGenerator {

    protected boolean intersectionsEnabled = true;
    protected boolean unionsEnabled = true;
    protected boolean negationsEnabled = false;
    protected boolean regexEnabled = false;
    protected boolean filterFunctionsEnabled = false;
    protected boolean contentFunctionsEnabled = false;
    protected boolean groupingFunctionsEnabled = false;
    protected boolean noFieldedFunctionsEnabled = false;
    protected boolean multiFieldedFunctionsEnabled = false;
    protected boolean nullLiteralsEnabled = false;

    protected final Random random = new Random();
    protected final StringBuilder sb = new StringBuilder();

    protected final List<String> fields;
    protected final List<String> values;

    enum NodeType {
        EQ, NE, ER, FILTER_FUNCTION, CONTENT_FUNCTION, GROUPING_FUNCTION, EQ_NULL, NE_NULL
    }

    public AbstractQueryGenerator(Set<String> fields, Set<String> values) {
        this.fields = Lists.newArrayList(fields);
        this.values = Lists.newArrayList(values);
    }

    public void validateOptions() {
        if (!intersectionsEnabled && !unionsEnabled) {
            throw new IllegalStateException("cannot disable both unions and intersections");
        }
    }

    public QueryGenerator enableIntersections() {
        intersectionsEnabled = true;
        return this;
    }

    public QueryGenerator disableIntersections() {
        intersectionsEnabled = false;
        return this;
    }

    public QueryGenerator enableUnions() {
        unionsEnabled = true;
        return this;
    }

    public QueryGenerator disableUnions() {
        unionsEnabled = false;
        return this;
    }

    public QueryGenerator enableNegations() {
        negationsEnabled = true;
        return this;
    }

    public QueryGenerator disableNegations() {
        negationsEnabled = false;
        return this;
    }

    public QueryGenerator enableRegexes() {
        regexEnabled = true;
        return this;
    }

    public QueryGenerator disableRegexes() {
        regexEnabled = false;
        return this;
    }

    public QueryGenerator enableFilterFunctions() {
        filterFunctionsEnabled = true;
        return this;
    }

    public QueryGenerator disableFilterFunctions() {
        filterFunctionsEnabled = false;
        return this;
    }

    public QueryGenerator enableContentFunctions() {
        contentFunctionsEnabled = true;
        return this;
    }

    public QueryGenerator disableContentFunctions() {
        contentFunctionsEnabled = false;
        return this;
    }

    public QueryGenerator enableGroupingFunctions() {
        groupingFunctionsEnabled = true;
        return this;
    }

    public QueryGenerator disableNoFieldedFunctions() {
        noFieldedFunctionsEnabled = false;
        return this;
    }

    public QueryGenerator enableNoFieldedFunctions() {
        noFieldedFunctionsEnabled = true;
        return this;
    }

    public QueryGenerator disableGroupingFunctions() {
        groupingFunctionsEnabled = false;
        return this;
    }

    public QueryGenerator enableMultiFieldedFunctions() {
        multiFieldedFunctionsEnabled = true;
        return this;
    }

    public QueryGenerator disableMultiFieldedFunctions() {
        multiFieldedFunctionsEnabled = false;
        return this;
    }

    public QueryGenerator enableNullLiterals() {
        nullLiteralsEnabled = true;
        return this;
    }

    public QueryGenerator disableNullLiterals() {
        nullLiteralsEnabled = false;
        return this;
    }

    public QueryGenerator enableAllOptions() {
        //  @formatter:off
        return enableNegations()
                        .enableRegexes()
                        .enableFilterFunctions()
                        .enableContentFunctions()
                        .enableGroupingFunctions()
                        .enableNoFieldedFunctions()
                        .enableMultiFieldedFunctions()
                        .enableNullLiterals();
        //  @formatter:on
    }

    public QueryGenerator disableAllOptions() {
        //  @formatter:off
        return disableNegations()
                        .disableRegexes()
                        .disableFilterFunctions()
                        .disableContentFunctions()
                        .disableGroupingFunctions()
                        .disableNoFieldedFunctions()
                        .disableMultiFieldedFunctions()
                        .disableNullLiterals();
        //  @formatter:on
    }

    public boolean isIntersectionsEnabled() {
        return intersectionsEnabled;
    }

    public boolean isUnionsEnabled() {
        return unionsEnabled;
    }

    public boolean isNegationsEnabled() {
        return negationsEnabled;
    }

    public boolean isRegexesEnabled() {
        return regexEnabled;
    }

    public boolean isFilterFunctionsEnabled() {
        return filterFunctionsEnabled;
    }

    public boolean isContentFunctionsEnabled() {
        return contentFunctionsEnabled;
    }

    public boolean isGroupingFunctionsEnabled() {
        return groupingFunctionsEnabled;
    }

    public boolean isNoFieldedFunctionsEnabled() {
        return noFieldedFunctionsEnabled;
    }

    public boolean isMultiFieldedFunctionsEnabled() {
        return multiFieldedFunctionsEnabled;
    }

    public boolean isNullLiteralsEnabled() {
        return nullLiteralsEnabled;
    }

    protected abstract void buildNode();

    protected abstract void buildJunction();

    protected abstract void buildLeaf();
}
