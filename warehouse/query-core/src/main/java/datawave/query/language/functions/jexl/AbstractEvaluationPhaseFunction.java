package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.Constants;
import datawave.query.search.WildcardFieldedFilter;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public abstract class AbstractEvaluationPhaseFunction extends JexlQueryFunction {
    WildcardFieldedFilter.BooleanType type = null;

    public AbstractEvaluationPhaseFunction(String functionName) {
        super(functionName, new ArrayList<>());
    }

    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        // super initialize will call validate
        super.initialize(parameterList, depth, parent);
        type = WildcardFieldedFilter.BooleanType.AND;
        int x = 0;
        if (this.parameterList.size() != 1 && this.parameterList.size() % 2 == 1) {
            try {
                String firstArg = this.parameterList.get(0);
                type = WildcardFieldedFilter.BooleanType.valueOf(firstArg.toUpperCase());
            } catch (Exception e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0}", this.name));
                throw new IllegalArgumentException(qe);
            }
            x = 1;
            this.parameterList.remove(0);
        }
    }

    @Override
    public void validate() throws IllegalArgumentException {
        // special case where we allow one value to be run against _ANYFIELD_
        if (this.parameterList.size() == 1) {
            return;
        }
        if (this.parameterList.isEmpty()) {
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

    protected String toString(String prefix, String suffix, String operation) {
        StringBuilder sb = new StringBuilder();

        if (parameterList.size() == 1) {
            sb.append(prefix).append(Constants.ANY_FIELD).append(", ").append(escapeString(parameterList.get(0))).append(suffix);
        } else {

            if (parameterList.size() > 2) // do not wrap single term functions
                sb.append("(");

            int x = 0;
            while (x < parameterList.size()) {
                if (x >= 2) {
                    sb.append(operation);
                }
                String field = parameterList.get(x++);
                String regex = parameterList.get(x++);
                sb.append(prefix).append(field).append(", ").append(escapeString(regex)).append(suffix);
            }

            if (parameterList.size() > 2)
                sb.append(")");
        }
        return sb.toString();
    }

}
