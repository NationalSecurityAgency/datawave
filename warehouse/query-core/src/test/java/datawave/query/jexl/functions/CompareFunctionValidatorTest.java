package datawave.query.jexl.functions;

import datawave.query.jexl.JexlNodeFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompareFunctionValidatorTest {
    @Test
    public void testValidateWithAnyParameters() {
        CompareFunctionValidator.validate("compare", Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<"),
                        JexlNodeFactory.buildIdentifier("ANY"), JexlNodeFactory.buildIdentifier("bar")));
    }
    
    @Test
    public void testValidateWithAllParameters() {
        CompareFunctionValidator.validate("compare", Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<"),
                        JexlNodeFactory.buildIdentifier("ALL"), JexlNodeFactory.buildIdentifier("bar")));
    }
    
    @Test
    public void testValidateWithNoParameters() {
        assertThrows(IllegalArgumentException.class, () -> CompareFunctionValidator.validate("compare", Collections.emptyList()));
    }
    
    @Test
    public void testValidateWithoutEnoughParameters() {
        assertThrows(IllegalArgumentException.class,
                        () -> CompareFunctionValidator.validate(
                                        "compare",
                                        Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<"),
                                                        JexlNodeFactory.buildIdentifier("bar"))));
    }
    
    @Test
    public void testValidateWithWrongModeParameters() {
        assertThrows(IllegalArgumentException.class,
                        () -> CompareFunctionValidator.validate(
                                        "compare",
                                        Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<"),
                                                        JexlNodeFactory.buildIdentifier("WHAT"), JexlNodeFactory.buildIdentifier("bar"))));
    }
    
    @Test
    public void testValidateWithWrongOperatorParameters() {
        assertThrows(IllegalArgumentException.class,
                        () -> CompareFunctionValidator.validate(
                                        "compare",
                                        Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<>"),
                                                        JexlNodeFactory.buildIdentifier("ANY"), JexlNodeFactory.buildIdentifier("bar"))));
    }
}
