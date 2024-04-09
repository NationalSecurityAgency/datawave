package datawave.query.jexl.functions;

import static org.junit.Assert.assertThrows;

import java.util.Arrays;

import org.junit.Test;

import datawave.core.query.jexl.JexlNodeFactory;

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
        assertThrows(IllegalArgumentException.class, () -> {
            CompareFunctionValidator.validate("compare", Arrays.asList());
        });
    }

    @Test
    public void testValidateWithoutEnoughParameters() {
        assertThrows(IllegalArgumentException.class, () -> {
            CompareFunctionValidator.validate("compare", Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<"),
                            JexlNodeFactory.buildIdentifier("bar")));
        });
    }

    @Test
    public void testValidateWithWrongModeParameters() {
        assertThrows(IllegalArgumentException.class, () -> {
            CompareFunctionValidator.validate("compare", Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<"),
                            JexlNodeFactory.buildIdentifier("WHAT"), JexlNodeFactory.buildIdentifier("bar")));
        });
    }

    @Test
    public void testValidateWithWrongOperatorParameters() {
        assertThrows(IllegalArgumentException.class, () -> {
            CompareFunctionValidator.validate("compare", Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<>"),
                            JexlNodeFactory.buildIdentifier("ANY"), JexlNodeFactory.buildIdentifier("bar")));
        });
    }
}
