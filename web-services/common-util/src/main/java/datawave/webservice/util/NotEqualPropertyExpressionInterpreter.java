package datawave.webservice.util;

import org.apache.deltaspike.core.api.config.ConfigResolver;
import org.apache.deltaspike.core.api.interpreter.ExpressionInterpreter;

/**
 * An {@link ExpressionInterpreter} that is only used for not equal property comparisons. The default deltaspike interpreter will evaluate to false if a not
 * equal comparison is attempted against a property that doesn't exist. We want {@code <non_existent_property>!=true} to evaluate to {@code true}.
 */
public class NotEqualPropertyExpressionInterpreter implements ExpressionInterpreter<String,Boolean> {
    @Override
    public Boolean evaluate(String expression) {
        if (expression == null) {
            return false;
        }

        String[] values = expression.split("!=");

        if (values.length != 2) {
            throw new IllegalArgumentException("'" + expression + "' is not a supported syntax");
        }

        String configuredValue = ConfigResolver.getPropertyValue(values[0], null);

        // exclude if null or the configured value is different
        return configuredValue == null || !values[1].trim().equalsIgnoreCase(configuredValue);
    }
}
