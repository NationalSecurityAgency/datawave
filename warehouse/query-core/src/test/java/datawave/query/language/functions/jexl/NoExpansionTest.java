package datawave.query.language.functions.jexl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.List;

import org.junit.Test;

public class NoExpansionTest {

    /**
     * Verify that {@link NoExpansion#validate()} throws an exception given an empty parameter list.
     */
    @Test
    public void testValidateWithEmptyParameters() {
        NoExpansion noExpansion = new NoExpansion();
        noExpansion.setParameterList(List.of());
        Exception exception = assertThrows(IllegalArgumentException.class, noExpansion::validate);
        assertEquals("datawave.webservice.query.exception.BadRequestQueryException: Invalid arguments to function. noExpansion requires at least one argument",
                        exception.getMessage());
    }

    /**
     * Verify that {@link NoExpansion#validate()} does not throw an error for a single parameter.
     */
    @Test
    public void testValidateWithOneField() {
        NoExpansion noExpansion = new NoExpansion();
        noExpansion.setParameterList(List.of("field1"));
        noExpansion.validate();
    }

    /**
     * Verify that {@link NoExpansion#validate()} does not throw an error for multiple parameters.
     */
    @Test
    public void testValidateWithMultipleFields() {
        NoExpansion noExpansion = new NoExpansion();
        noExpansion.setParameterList(List.of("field1", "field2", "field3"));
        noExpansion.validate();
    }

    @Test
    public void testToStringWithNoParameters() {
        NoExpansion noExpansion = new NoExpansion();
        assertEquals("f:noExpansion()", noExpansion.toString());
    }

    @Test
    public void testToStringWithOneParameter() {
        NoExpansion noExpansion = new NoExpansion();
        noExpansion.setParameterList(List.of("field1"));
        assertEquals("f:noExpansion('field1')", noExpansion.toString());
    }

    @Test
    public void testToStringWithMultipleParameter() {
        NoExpansion noExpansion = new NoExpansion();
        noExpansion.setParameterList(List.of("field1", "field2", "field3"));
        assertEquals("f:noExpansion('field1','field2','field3')", noExpansion.toString());
    }
}
