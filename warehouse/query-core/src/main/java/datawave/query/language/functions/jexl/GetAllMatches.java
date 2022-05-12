package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.query.language.functions.QueryFunction;
import datawave.query.search.WildcardFieldedFilter;

import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * <pre>
 * Function is like the Include function, except where the Include function returns only
 * the first match that makes the result 'true' (non-empty return set), the GetAllMatches
 * function returns all of the matches for the supplied field and value.
 * </pre>
 */
public class GetAllMatches extends JexlQueryFunction {
    WildcardFieldedFilter.BooleanType type = null;
    
    public GetAllMatches() {
        super("get_all_matches", new ArrayList<>());
    }
    
    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        super.initialize(parameterList, depth, parent);
        type = WildcardFieldedFilter.BooleanType.AND;
        int x = 0;
        if (this.parameterList.size() % 2 == 1) {
            try {
                String firstArg = this.parameterList.get(0);
                type = WildcardFieldedFilter.BooleanType.valueOf(firstArg.toUpperCase());
            } catch (Exception e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0}", e, this.name));
                throw new IllegalArgumentException(qe);
            }
            x = 1;
            this.parameterList.remove(0);
        }
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() < 2) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        String firstArg = this.parameterList.get(0);
        if (firstArg.equalsIgnoreCase("and") || firstArg.equalsIgnoreCase("or")) {
            if (this.parameterList.size() % 2 != 1) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        } else {
            if (this.parameterList.size() % 2 != 0) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String operation = (this.type.equals(WildcardFieldedFilter.BooleanType.AND)) ? " && " : " || ";
        
        if (parameterList.size() > 2) // do not wrap single terms
            sb.append("(");
        
        int x = 0;
        while (x < parameterList.size()) {
            if (x >= 2) {
                sb.append(operation);
            }
            String field = parameterList.get(x++);
            String regex = parameterList.get(x++);
            sb.append("filter:getAllMatches(").append(field).append(", ").append(escapeString(regex)).append(")");
        }
        
        if (parameterList.size() > 2)
            sb.append(")");
        
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new GetAllMatches();
    }
}
