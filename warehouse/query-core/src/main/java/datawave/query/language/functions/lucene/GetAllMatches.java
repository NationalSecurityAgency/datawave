package datawave.query.language.functions.lucene;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.query.language.functions.QueryFunction;
import datawave.query.search.WildcardFieldedFilter;

import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

@Deprecated
public class GetAllMatches extends LuceneQueryFunction {
    public GetAllMatches() {
        super("getAllMatches", new ArrayList<>());
    }
    
    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        super.initialize(parameterList, depth, parent);
        WildcardFieldedFilter.BooleanType type = WildcardFieldedFilter.BooleanType.AND;
        int x = 0;
        if (this.parameterList.size() % 2 == 1) {
            try {
                String firstArg = this.parameterList.get(0);
                type = WildcardFieldedFilter.BooleanType.valueOf(firstArg.toUpperCase());
            } catch (Exception e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", e,
                                this.name));
                throw new IllegalArgumentException(qe);
            }
            x = 1;
        }
        this.fieldedFilter = new WildcardFieldedFilter(true, type);
        
        while (x < parameterList.size()) {
            String field = parameterList.get(x++);
            String regex = parameterList.get(x++);
            this.fieldedFilter.addCondition(field, regex);
        }
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.depth != 1) {
            throw new IllegalArgumentException("function: " + this.name + " must be at the top level of the query");
        }
        if (!(this.parent instanceof AndQueryNode)) {
            throw new IllegalArgumentException("function: " + this.name + " must be part of an AND expression");
        }
        if (this.parameterList.size() < 2) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        String firstArg = this.parameterList.get(0);
        if (firstArg.equalsIgnoreCase("and") || firstArg.equalsIgnoreCase("or")) {
            if (this.parameterList.size() % 2 != 1) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        } else {
            if (this.parameterList.size() % 2 != 0) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        }
    }
    
    @Override
    public String toString() {
        return this.fieldedFilter.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new GetAllMatches();
    }
}
