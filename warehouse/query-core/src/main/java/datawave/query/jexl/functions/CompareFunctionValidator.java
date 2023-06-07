package datawave.query.jexl.functions;

import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * <pre>
 * Function arguments are a field name, an operator, a mode(ALL/ANY) and another field name
 * 
 * compare(FOO, '&gt;=', BAR) will return true if both sets are empty or any elements evaluate to true
 * </pre>
 */
public class CompareFunctionValidator {
    
    public enum Mode {
        ANY, ALL
    }
    
    public static final List<String> operators = Collections.unmodifiableList(Arrays.asList("<", "<=", ">", ">=", "==", "=", "!="));
    
    public static void validate(String function, List<JexlNode> args) throws IllegalArgumentException {
        if (args.size() != 4) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                            MessageFormat.format("{0} requires 4 arguments", function));
            throw new IllegalArgumentException(qe);
        } else {
            if (!operators.contains(args.get(1).image)) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0} requires valid comparison operator (<, <=, >, >=, ==/= or !=) as 2nd arguments", function));
                throw new IllegalArgumentException(qe);
            }
            
            try {
                Mode.valueOf(args.get(2).image);
            } catch (IllegalArgumentException iae) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0} requires ANY or ALL as 3rd arguments", function));
                throw new IllegalArgumentException(qe);
            }
        }
    }
}
