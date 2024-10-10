package datawave.core.query.language.functions.lucene;

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.core.query.language.functions.QueryFunction;
import datawave.core.query.search.FieldedTerm;
import datawave.core.query.search.WildcardFieldedFilter;

@Deprecated
public abstract class LuceneQueryFunction implements QueryFunction {

    protected String name = null;
    protected List<String> parameterList = null;
    protected int depth = -1;
    protected WildcardFieldedFilter fieldedFilter = null;
    protected QueryNode parent = null;

    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        this.parameterList = parameterList;
        this.depth = depth;
        this.parent = parent;
        validate();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public List<String> getParameterList() {
        return parameterList;
    }

    @Override
    public void setParameterList(List<String> parameterList) {
        this.parameterList = parameterList;
    }

    public LuceneQueryFunction(String functionName, List<String> parameterList) {
        this.name = functionName;
        this.parameterList = parameterList;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        boolean firstParam = true;
        sb.append(name);
        sb.append("(");
        for (String s : parameterList) {
            if (firstParam) {
                firstParam = false;
            } else {
                sb.append(", ");
            }
            sb.append(s);
        }
        sb.append(")");

        return sb.toString();
    }

    public FieldedTerm getFieldedTerm() {
        return fieldedFilter;
    }

    public void setFieldedTerm(WildcardFieldedFilter fieldedFilter) {
        this.fieldedFilter = fieldedFilter;
    }
}
