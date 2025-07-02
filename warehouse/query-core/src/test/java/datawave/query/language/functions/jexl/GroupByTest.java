package datawave.query.language.functions.jexl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import datawave.webservice.query.exception.BadRequestQueryException;

class GroupByTest {

    /**
     * Verify that {@link GroupBy#validate()} throws no error for the query {@code #GROUPBY(field1,field2,field3)}.
     */
    @Test
    public void testValidateWithParameters() {
        GroupBy groupBy = new GroupBy();
        groupBy.setParameterList(Lists.newArrayList("field1", "field2", "field3"));
        groupBy.validate();
    }

    /**
     * Verify that {@link GroupBy#validate()} throws an error for the query {@code #GROUPBY()}.
     */
    @Test
    public void testValidateWithNoParameters() {
        GroupBy groupBy = new GroupBy();
        Exception exception = assertThrows(IllegalArgumentException.class, groupBy::validate);
        assertEquals("datawave.webservice.query.exception.BadRequestQueryException: Invalid arguments to function. groupby requires at least one argument",
                        exception.getMessage());
    }

    /**
     * Verify that {@link GroupBy#validate()} throws no error for the query {@code #GROUPBY(field1[DAY],field2[HOUR,MINUTE],field3[ALL,DAY])}.
     */
    @Test
    public void testValidateWithComplexParameters() {
        GroupBy groupBy = new GroupBy();
        groupBy.setParameterList(Lists.newArrayList("field1[DAY]", "field2[HOUR,MINUTE]", "field3[ALL,DAY]"));
        groupBy.validate();
    }

    /**
     * Verify that {@link GroupBy#validate()} throws an error for the query {@code #GROUPBY(field1[BAD_TRANSFORMER],field2[HOUR,MINUTE],field3[ALL,DAY])}.
     */
    @Test
    public void testValidateWithInvalidTransformer() {
        GroupBy groupBy = new GroupBy();
        groupBy.setParameterList(Lists.newArrayList("field1[BAD_TRANSFORMER]", "field2[HOUR,MINUTE]", "field3[ALL,DAY]"));
        Exception exception = assertThrows(IllegalArgumentException.class, groupBy::validate);
        assertEquals("datawave.webservice.query.exception.BadRequestQueryException: Invalid arguments to function. Unable to parse fields from arguments for function groupby",
                        exception.getMessage());
        Throwable childCause = exception.getCause();
        // Verify that the original exception was not swallowed, and is present in the stack trace.
        Assertions.assertInstanceOf(BadRequestQueryException.class, childCause);
        Throwable grandchildCause = childCause.getCause();
        Assertions.assertInstanceOf(IllegalArgumentException.class, grandchildCause);
        Assertions.assertEquals("No TemporalGranularity exists with the name BAD_TRANSFORMER", grandchildCause.getMessage());
    }

    @Test
    public void testToStringWithNoParameters() {
        GroupBy groupBy = new GroupBy();
        assertEquals("f:groupby()", groupBy.toString());
    }

    @Test
    public void testToStringWithParameters() {
        GroupBy groupBy = new GroupBy();
        groupBy.setParameterList(Lists.newArrayList("field1", "field2[HOUR]", "field3[DAY]"));
        assertEquals("f:groupby('field1','field2[HOUR]','field3[DAY]')", groupBy.toString());
    }

}
