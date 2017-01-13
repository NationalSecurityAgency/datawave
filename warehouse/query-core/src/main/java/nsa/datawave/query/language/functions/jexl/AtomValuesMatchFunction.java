package nsa.datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import nsa.datawave.query.language.functions.QueryFunction;
import nsa.datawave.webservice.query.exception.BadRequestQueryException;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * function to test whether two key/value pairs match within datafields of the same (grouping context) group For example FROM_ADDRESS.1 == 1.2.3.4 and
 * DIRECTION.1 == 1
 */
public class AtomValuesMatchFunction extends JexlQueryFunction {
    
    public AtomValuesMatchFunction() {
        super("atom_values_match", new ArrayList<String>());
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() < 2) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<String> params = getParameterList();
        for (int i = 0; i < params.size(); i++) {
            if (sb.length() > 0)
                sb.append(", ");
            sb.append(params.get(i));
        }
        sb.insert(0, "grouping:atomValuesMatch(");
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new AtomValuesMatchFunction();
    }
    
}
