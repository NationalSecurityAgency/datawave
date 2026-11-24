package datawave.marking;

import org.apache.accumulo.access.AccessExpression;

import datawave.validation.ParameterValidator;

/**
 * A security marking supplied with a request (for example the {@code columnVisibility} parameter when creating a query), translated into an Accumulo access
 * expression. Deployments can provide their own implementation to accept other marking formats.
 */
public interface SecurityMarking extends ParameterValidator {

    /**
     * @return the marking as an access expression; the empty expression if no marking was supplied
     * @throws IllegalArgumentException
     *             if the marking is not a valid access expression
     */
    AccessExpression toAccessExpression();

    /**
     * @return the normalized access expression string, or null if no marking was supplied
     */
    String toAccessExpressionString();

    void clear();
}
