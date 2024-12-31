package datawave.query.language.functions.jexl;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.webservice.query.exception.BadRequestQueryException;

class MaxUniqueCountTest {

    /**
     * Verify that an exception is thrown no arguments are given.
     */
    @Test
    void testValidateWithNoArguments() {
        MaxUniqueCount function = new MaxUniqueCount();

        Throwable throwable = Assertions.assertThrows(IllegalArgumentException.class, function::validate);
        Throwable cause = throwable.getCause();
        Assertions.assertInstanceOf(BadRequestQueryException.class, cause);
        Assertions.assertEquals("Invalid arguments to function. max_unique_count requires a single integer argument greater than 0.", cause.getMessage());
    }

    /**
     * Verify that an exception is thrown when multiple arguments are given.
     */
    @Test
    void testValidateWithMultipleArguments() {
        MaxUniqueCount function = new MaxUniqueCount();
        function.setParameterList(List.of("1", "2"));

        Throwable throwable = Assertions.assertThrows(IllegalArgumentException.class, function::validate);
        Throwable cause = throwable.getCause();
        Assertions.assertInstanceOf(BadRequestQueryException.class, cause);
        Assertions.assertEquals("Invalid arguments to function. max_unique_count requires a single integer argument greater than 0.", cause.getMessage());
    }

    /**
     * Verify that an exception is thrown when the argument cannot be parsed to an integer.
     */
    @Test
    void testValidateWithNonIntegerArg() {
        MaxUniqueCount function = new MaxUniqueCount();
        function.setParameterList(List.of("a"));

        Throwable throwable = Assertions.assertThrows(IllegalArgumentException.class, function::validate);
        Throwable cause = throwable.getCause();
        Assertions.assertInstanceOf(BadRequestQueryException.class, cause);
        Assertions.assertEquals("Invalid arguments to function. Failed to parse argument in f:max_unique_count(a) to an integer.", cause.getMessage());
    }

    /**
     * Verify that an exception is thrown when the argument is an integer less than one.
     */
    @Test
    void testValidateWithIntegerArgLessThanOne() {
        MaxUniqueCount function = new MaxUniqueCount();
        function.setParameterList(List.of("0"));

        Throwable throwable = Assertions.assertThrows(IllegalArgumentException.class, function::validate);
        Throwable cause = throwable.getCause();
        Assertions.assertInstanceOf(BadRequestQueryException.class, cause);
        Assertions.assertEquals("Invalid arguments to function. max_unique_count requires an integer argument greater than 0.", cause.getMessage());
    }

    /**
     * Verify that an exception is not thrown when a valid argument is given.
     */
    @Test
    void testValidateWithValidArguments() {
        MaxUniqueCount function = new MaxUniqueCount();
        function.setParameterList(List.of("1"));

        function.validate();
    }
}
