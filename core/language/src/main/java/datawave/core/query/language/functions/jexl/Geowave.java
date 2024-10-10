package datawave.core.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public abstract class Geowave extends JexlQueryFunction {

    private static final Logger log = LoggerFactory.getLogger(Geowave.class);

    public Geowave(String functionName) {
        super(functionName, new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() != 2) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("geowave:" + name + "(");
        List<String> params = getParameterList();
        if (!params.isEmpty()) {
            sb.append(params.get(0)); // the field name
        }
        for (int i = 1; i < params.size(); i++) {
            if (sb.length() > 0)
                sb.append(", ");
            sb.append(escapeString(params.get(i)));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public QueryFunction duplicate() {
        QueryFunction queryFunction = null;
        try {
            queryFunction = this.getClass().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            log.error("Unable to duplicate Geowave query function: [" + this.getClass().getName() + "]");
        }
        return queryFunction;
    }

    public static class Contains extends Geowave {
        public Contains() {
            super("contains");
        }
    }

    public static class CoveredBy extends Geowave {
        public CoveredBy() {
            super("covered_by");
        }
    }

    public static class Covers extends Geowave {
        public Covers() {
            super("covers");
        }
    }

    public static class Crosses extends Geowave {
        public Crosses() {
            super("crosses");
        }
    }

    public static class Intersects extends Geowave {
        public Intersects() {
            super("intersects");
        }
    }

    public static class Overlaps extends Geowave {
        public Overlaps() {
            super("overlaps");
        }
    }

    public static class Within extends Geowave {
        public Within() {
            super("within");
        }
    }
}
