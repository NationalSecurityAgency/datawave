package datawave.common.cl;

import org.apache.commons.cli.Option;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class OptionBuilderTest {
    
    private OptionBuilder optionBuilder;
    
    @BeforeEach
    public void setup() {
        this.optionBuilder = new OptionBuilder();
        this.optionBuilder.reset();
    }
    
    @Test
    public void testCreate() throws Exception {
        Option option = this.optionBuilder.create("opt", "desc");
        assertEquals(option.getArgs(), 0);
        assertEquals(String.class, option.getType());
        assertEquals(option.getOpt(), "opt");
        assertEquals(option.getDescription(), "desc");
        assertNull(option.getLongOpt());
        assertFalse(option.isRequired());
        assertEquals(option.getValueSeparator(), 0);
    }
    
    @Test
    public void testCreate1() throws Exception {
        Option option = this.optionBuilder.create("opt", "longOpt", "desc");
        assertEquals(option.getLongOpt(), "longOpt");
    }
}
