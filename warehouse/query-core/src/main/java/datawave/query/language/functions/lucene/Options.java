package datawave.query.language.functions.lucene;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

@Deprecated
public class Options extends LuceneQueryFunction {
    public Options() {
        super(QueryFunctions.OPTIONS_FUNCTION, new ArrayList<>());
    }

    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        super.initialize(parameterList, depth, parent);
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() % 2 != 0) { // must have even number of args
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
    public QueryFunction duplicate() {
        return new Options();
    }
}
