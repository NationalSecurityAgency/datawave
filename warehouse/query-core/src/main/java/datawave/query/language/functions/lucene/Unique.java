package datawave.query.language.functions.lucene;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

@Deprecated
public class Unique extends LuceneQueryFunction {
    public Unique() {
        super(QueryFunctions.UNIQUE_FUNCTION, new ArrayList<>());
    }

    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        super.initialize(parameterList, depth, parent);
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.isEmpty()) {
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
        return new Unique();
    }
}
