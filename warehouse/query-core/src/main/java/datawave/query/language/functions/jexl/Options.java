package datawave.query.language.functions.jexl;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

import java.text.MessageFormat;
import java.util.ArrayList;

public class Options extends JexlQueryFunction {
    
    public Options() {
        super("options", new ArrayList<String>());
    }
    
    /**
     * query options are pairs of key/value. Ensure that the number of args is even
     * 
     * @throws IllegalArgumentException
     */
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() % 2 != 0) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("(");
        int x = 0;
        while (x < parameterList.size()) {
            String key = parameterList.get(x++);
            String value = parameterList.get(x++);
            sb.append("filter:options(").append(escapeString(key)).append(", ").append(escapeString(value)).append(")");
        }
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Options();
    }
}
