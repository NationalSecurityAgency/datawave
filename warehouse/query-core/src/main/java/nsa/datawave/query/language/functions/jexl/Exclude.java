package nsa.datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import nsa.datawave.query.language.functions.QueryFunction;
import nsa.datawave.query.search.WildcardFieldedFilter;

import nsa.datawave.webservice.query.exception.BadRequestQueryException;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class Exclude extends JexlQueryFunction {
    WildcardFieldedFilter.BooleanType type = null;
    
    public Exclude() {
        super("exclude", new ArrayList<String>());
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
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
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
        StringBuilder sb = new StringBuilder();
        String operation = (this.type.equals(WildcardFieldedFilter.BooleanType.AND)) ? " || " : " && ";
        
        sb.append("(");
        int x = 0;
        while (x < parameterList.size()) {
            if (x >= 2) {
                sb.append(operation);
            }
            String field = parameterList.get(x++);
            String regex = parameterList.get(x++);
            sb.append("not(filter:includeRegex(").append(field).append(", ").append(escapeString(regex)).append("))");
        }
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Exclude();
    }
}
