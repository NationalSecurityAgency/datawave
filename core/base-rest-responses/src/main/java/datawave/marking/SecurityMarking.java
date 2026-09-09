package datawave.marking;

import org.apache.accumulo.access.AccessExpression;

import datawave.validation.ParameterValidator;

public interface SecurityMarking extends ParameterValidator {

    AccessExpression toAccessExpression() throws MarkingFunctions.Exception;

    String toAccessExpressionString();

    Markings<?> toMarkings() throws MarkingFunctions.Exception;

    void clear();
}
