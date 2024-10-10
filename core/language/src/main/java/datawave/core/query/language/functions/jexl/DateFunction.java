package datawave.core.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;

import datawave.core.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public class DateFunction extends JexlQueryFunction {
    private static final Set<String> COMMANDS = Sets.newHashSet("after", "before", "between");

    public DateFunction() {
        super("date", new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() < 2) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        String type = this.parameterList.get(1).toLowerCase();
        boolean knownType = COMMANDS.contains(type);
        if (!knownType) {
            // if the type is not specified, then we accept the between arguments
            if (this.parameterList.size() < 3 || this.parameterList.size() > 5) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        } else if (type.equals("between")) {
            if (this.parameterList.size() < 4 || this.parameterList.size() > 6) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        } else {
            if (this.parameterList.size() < 3 || this.parameterList.size() > 5) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        }
    }

    @Override
    public String toString() {
        Iterator<String> param = getParameterList().iterator();
        StringBuilder f = new StringBuilder(64);
        String field = param.next().toUpperCase();
        String type = param.next();
        boolean knownType = COMMANDS.contains(type.toLowerCase());
        f.append("filter:").append(knownType ? type.toLowerCase() : "between").append("Date");
        if ("between".equals(knownType ? type.toLowerCase() : "between")) {
            f.append('s');
        }
        f.append('(').append(field).append(", ");
        if (!knownType) {
            f.append(escapeString(type)).append(", ");
        }
        while (param.hasNext()) {
            f.append(escapeString(param.next())).append(", ");
        }
        f.setLength(f.length() - ", ".length());
        f.append(')');
        return f.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new DateFunction();
    }

}
