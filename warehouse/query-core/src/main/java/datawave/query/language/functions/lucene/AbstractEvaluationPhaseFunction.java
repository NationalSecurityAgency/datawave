package datawave.query.language.functions.lucene;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.Constants;
import datawave.query.search.WildcardFieldedFilter;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

@Deprecated
public abstract class AbstractEvaluationPhaseFunction extends LuceneQueryFunction {
    private boolean includeIfMatch;

    public AbstractEvaluationPhaseFunction(String functionName, boolean includeIfMatch) {
        super(functionName, new ArrayList<>());
        this.includeIfMatch = includeIfMatch;
    }

    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        // super initialize will call validate
        super.initialize(parameterList, depth, parent);
        WildcardFieldedFilter.BooleanType type = WildcardFieldedFilter.BooleanType.AND;
        int x = 0;
        if (this.parameterList.size() != 1 && this.parameterList.size() % 2 == 1) {
            try {
                String firstArg = this.parameterList.get(0);
                type = WildcardFieldedFilter.BooleanType.valueOf(firstArg.toUpperCase());
            } catch (Exception e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0}", e, this.name));
                throw new IllegalArgumentException(qe);
            }
            x = 1;
        }
        this.fieldedFilter = new WildcardFieldedFilter(includeIfMatch, type);

        // special case where one argument will be matched against any field
        if (this.parameterList.size() == 1) {
            this.fieldedFilter.addCondition(Constants.ANY_FIELD, parameterList.get(0));
        } else {
            while (x < parameterList.size()) {
                String field = parameterList.get(x++);
                String regex = parameterList.get(x++);
                this.fieldedFilter.addCondition(field, regex);
            }
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

    @Override
    public String toString() {
        return this.fieldedFilter.toString();
    }

}
