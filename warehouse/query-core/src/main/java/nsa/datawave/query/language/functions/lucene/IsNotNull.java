package nsa.datawave.query.language.functions.lucene;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import nsa.datawave.query.language.functions.QueryFunction;
import nsa.datawave.query.search.WildcardFieldedFilter;

import nsa.datawave.webservice.query.exception.BadRequestQueryException;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class IsNotNull extends LuceneQueryFunction {
    public IsNotNull() {
        super("isnotnull", new ArrayList<String>());
    }
    
    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        super.initialize(parameterList, depth, parent);
        // the dot (.) does not match line terminator characters, so we must specify them here
        this.fieldedFilter = new WildcardFieldedFilter(true, WildcardFieldedFilter.BooleanType.AND);
        this.fieldedFilter.addCondition(parameterList.get(0), ".+");
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() != 1) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        if (this.depth != 1) {
            throw new IllegalArgumentException("function: " + this.name + " must be at the top level of the query");
        }
        if (!(this.parent instanceof AndQueryNode || this.parent instanceof BooleanQueryNode)) {
            throw new IllegalArgumentException("function: " + this.name + " must be part of an AND expression");
        }
    }
    
    @Override
    public String toString() {
        return this.fieldedFilter.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new IsNotNull();
    }
}
