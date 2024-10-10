package datawave.query.jexl.functions;

import java.text.MessageFormat;
import java.util.List;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import datawave.core.query.language.functions.jexl.Compare;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * <pre>
 * Function arguments are a field name, an operator, a mode(ALL/ANY) and another field name
 *
 * compare(FOO, '&gt;=', BAR) will return true if both sets are empty or any elements evaluate to true
 * </pre>
 */
public class CompareFunctionValidator {

    public static void validate(String function, List<JexlNode> args) throws IllegalArgumentException {
        if (args.size() != 4) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                            MessageFormat.format("{0} requires 4 arguments", function));
            throw new IllegalArgumentException(qe);
        } else {
            if (!Compare.operators.contains(JexlNodes.getIdentifierOrLiteralAsString(args.get(1)))) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0} requires valid comparison operator (<, <=, >, >=, ==/= or !=) as 2nd arguments", function));
                throw new IllegalArgumentException(qe);
            }

            try {
                Compare.Mode.valueOf(JexlNodes.getIdentifierOrLiteralAsString(args.get(2)));
            } catch (IllegalArgumentException iae) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0} requires ANY or ALL as 3rd arguments", function));
                throw new IllegalArgumentException(qe);
            }
        }
    }
}
