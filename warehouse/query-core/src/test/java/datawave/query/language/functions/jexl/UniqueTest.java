package datawave.query.language.functions.jexl;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class UniqueTest {
    
    /**
     * Verify that {@link Unique#validate()} throws no error for the query {@code #UNIQUE(field1,field2,field3)}.
     */
    @Test
    public void testValidateWithParameters() {
        Unique unique = new Unique();
        unique.setParameterList(Lists.newArrayList("field1", "field2", "field3"));
        unique.validate();
    }
    
    /**
     * Verify that {@link Unique#validate()} throws an error for the query {@code #UNIQUE()}.
     */
    @Test
    public void testValidateWithNoParameters() {
        Unique unique = new Unique();
        Exception exception = assertThrows(IllegalArgumentException.class, unique::validate);
        assertEquals("datawave.webservice.query.exception.BadRequestQueryException: Invalid arguments to function. unique requires at least one argument",
                        exception.getMessage());
    }
    
    /**
     * Verify that {@link Unique#validate()} throws no error for the query {@code #UNIQUE(field1[DAY],field2[HOUR,MINUTE],field3[ALL,DAY])}.
     */
    @Test
    public void testValidateWithComplexParameters() {
        Unique unique = new Unique();
        unique.setParameterList(Lists.newArrayList("field1[DAY]", "field2[HOUR,MINUTE]", "field3[ALL,DAY]"));
        unique.validate();
    }
    
    /**
     * Verify that {@link Unique#validate()} throws an error for the query {@code #UNIQUE(field1[BAD_TRANSFORMER],field2[HOUR,MINUTE],field3[ALL,DAY])}.
     */
    @Test
    public void testValidateWithInvalidTransformer() {
        Unique unique = new Unique();
        unique.setParameterList(Lists.newArrayList("field1[BAD_TRANSFORMER]", "field2[HOUR,MINUTE]", "field3[ALL,DAY]"));
        Exception exception = assertThrows(IllegalArgumentException.class, unique::validate);
        assertEquals("datawave.webservice.query.exception.BadRequestQueryException: Invalid arguments to function. Unable to parse unique fields from arguments for function unique",
                        exception.getMessage());
    }
    
    @Test
    public void testToStringWithNoParameters() {
        Unique unique = new Unique();
        assertEquals("f:unique()", unique.toString());
    }
    
    @Test
    public void testToStringWithParameters() {
        Unique unique = new Unique();
        unique.setParameterList(Lists.newArrayList("field1", "field2[HOUR]", "field3[DAY]"));
        assertEquals("f:unique('field1','field2[HOUR]','field3[DAY]')", unique.toString());
    }
}
