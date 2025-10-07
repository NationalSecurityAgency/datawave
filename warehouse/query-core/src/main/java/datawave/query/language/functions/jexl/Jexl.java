package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * This function accepts one string argument, which is a jexl query string. The purpose is to allow the lucene parse to form queries that are too complex for
 * the lucene parser.
 */
public class Jexl extends JexlQueryFunction {

    public Jexl() {
        super("jexl", new ArrayList<>());
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
        List<String> params = getParameterList();
        for (String param : params) {
            sb.append(param);
        }
        return sb.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new Jexl();
    }

}
