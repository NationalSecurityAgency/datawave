package datawave.query.jexl.functions;

import datawave.query.jexl.JexlNodeFactory;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertThrows;

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
        assertThrows(IllegalArgumentException.class,
                        () -> {
                            CompareFunctionValidator.validate(
                                            "compare",
                                            Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<"),
                                                            JexlNodeFactory.buildIdentifier("bar")));
                        });
    }
    
    @Test
    public void testValidateWithWrongModeParameters() {
        assertThrows(IllegalArgumentException.class,
                        () -> {
                            CompareFunctionValidator.validate(
                                            "compare",
                                            Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<"),
                                                            JexlNodeFactory.buildIdentifier("WHAT"), JexlNodeFactory.buildIdentifier("bar")));
                        });
    }
    
    @Test
    public void testValidateWithWrongOperatorParameters() {
        assertThrows(IllegalArgumentException.class,
                        () -> {
                            CompareFunctionValidator.validate(
                                            "compare",
                                            Arrays.asList(JexlNodeFactory.buildIdentifier("foo"), JexlNodeFactory.buildIdentifier("<>"),
                                                            JexlNodeFactory.buildIdentifier("ANY"), JexlNodeFactory.buildIdentifier("bar")));
                        });
    }
}
