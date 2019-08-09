package datawave.query.language.functions.lucene;

import datawave.query.language.functions.QueryFunction;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class EvaluationOnly extends LuceneQueryFunction {
    public EvaluationOnly() {
        super("evaluation_only", new ArrayList<>());
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() != 1) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("((ASTEvaluationOnly = true) && ");
        List<String> params = getParameterList();
        if (params.size() != 1) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        } else {
            LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
            try {
                sb.append(parser.parse(params.get(0)).getOriginalQuery());
            } catch (ParseException e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        }
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new EvaluationOnly();
    }
}
