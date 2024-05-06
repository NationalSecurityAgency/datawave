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

    protected final Random random = new Random();
    protected final StringBuilder sb = new StringBuilder();

    protected final List<String> fields;
    protected final List<String> values;

    enum NodeType {
        EQ, NE, ER, FILTER_FUNCTION, CONTENT_FUNCTION, GROUPING_FUNCTION
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

    public QueryGenerator disableGroupingFunctions() {
        groupingFunctionsEnabled = false;
        return this;
    }

    public QueryGenerator enableAllOptions() {
        return enableNegations().enableRegexes().enableFilterFunctions().enableContentFunctions().enableGroupingFunctions();
    }

    public QueryGenerator disableAllOptions() {
        return disableNegations().disableRegexes().disableFilterFunctions().disableContentFunctions().disableGroupingFunctions();
    }

    protected abstract void buildNode();

    protected abstract void buildJunction();

    protected abstract void buildLeaf();
}
