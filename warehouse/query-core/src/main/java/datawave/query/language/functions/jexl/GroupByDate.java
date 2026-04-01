package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.List;

import datawave.query.Constants;
import datawave.query.attributes.TemporalGranularity;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public abstract class GroupByDate extends JexlQueryFunction {

    public static final String GROUPBY_MILLISECOND_FUNCTION = "groupby_millisecond";
    public static final String GROUPBY_SECOND_FUNCTION = "groupby_second";
    public static final String GROUPBY_MINUTE_FUNCTION = "groupby_minute";
    public static final String GROUPBY_TENTH_OF_HOUR_FUNCTION = "groupby_tenth_of_hour";
    public static final String GROUPBY_HOUR_FUNCTION = "groupby_hour";
    public static final String GROUPBY_DAY_FUNCTION = "groupby_day";
    public static final String GROUPBY_MONTH_FUNCTION = "groupby_month";
    public static final String GROUPBY_YEAR_FUNCTION = "groupby_year";

    public enum GroupByDateFunction {
        GROUPBY_MILLISECOND(GROUPBY_MILLISECOND_FUNCTION, TemporalGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND),
        GROUPBY_SECOND(GROUPBY_SECOND_FUNCTION, TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND),
        GROUPBY_MINUTE(GROUPBY_MINUTE_FUNCTION, TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE),
        GROUPBY_TENTH_OF_HOUR(GROUPBY_TENTH_OF_HOUR_FUNCTION, TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR),
        GROUPBY_HOUR(GROUPBY_HOUR_FUNCTION, TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR),
        GROUPBY_DAY(GROUPBY_DAY_FUNCTION, TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY),
        GROUPBY_MONTH(GROUPBY_MONTH_FUNCTION, TemporalGranularity.TRUNCATE_TEMPORAL_TO_MONTH),
        GROUPBY_YEAR(GROUPBY_YEAR_FUNCTION, TemporalGranularity.TRUNCATE_TEMPORAL_TO_YEAR);

        public final String name;
        public final TemporalGranularity granularity;

        GroupByDateFunction(String name, TemporalGranularity granularity) {
            this.name = name;
            this.granularity = granularity;
        }

        public String getName() {
            return name;
        }

        public static GroupByDateFunction findByName(String name) {
            return GroupByDateFunction.valueOf(name.toUpperCase());
        }
    }

    public GroupByDate(String functionName, List<String> parameterList) {
        super(functionName, parameterList);
    }

    @Override
    public void validate() throws IllegalArgumentException {
        // Verify at least one parameter was passed in.
        if (this.parameterList.isEmpty()) {
            BadRequestQueryException e = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                            MessageFormat.format("{0} requires at least one argument", this.name));
            throw new IllegalArgumentException(e);
        }

        // Verify that the advanced group-by syntax, e.g. FIELD[DAY,HOUR], is not used.
        for (String param : this.parameterList) {
            if (param.contains(Constants.BRACKET_START)) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format(
                                "{0} does not support the advanced group-by syntax, only a simple comma-delimited list of fields is allowed.", this.name));
                throw new IllegalArgumentException(qe);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(QueryFunctions.QUERY_FUNCTION_NAMESPACE).append(':').append(this.name);
        if (parameterList.isEmpty()) {
            sb.append("()");
        } else {
            char separator = '(';
            for (String param : parameterList) {
                sb.append(separator).append(escapeString(param));
                separator = ',';
            }
            sb.append(")");
        }
        return sb.toString();
    }
}
