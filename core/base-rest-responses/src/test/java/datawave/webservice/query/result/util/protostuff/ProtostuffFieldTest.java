package datawave.webservice.query.result.util.protostuff;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ProtostuffFieldTest {
    
    private enum FIELD_BASE implements FieldAccessor {
        UNKNOWN(0, "unknown"), AFIELD(1, "aField"), BFIELD(2, "bField"), CFIELD(3, "cField");
        
        private final int fn;
        private final String name;
        
        FIELD_BASE(int fn, String name) {
            this.fn = fn;
            this.name = name;
        }
        
        @Override
        public int getFieldNumber() {
            return fn;
        }
        
        @Override
        public String getFieldName() {
            return name;
        }
    }
    
    private final ProtostuffField<FIELD_BASE> FIELD = new ProtostuffField<>(FIELD_BASE.class);
    
    private enum BAD_BASE implements FieldAccessor {
        AFIELD(1, "aField"), BFIELD(2, "bField"), CFIELD(3, "cField");
        
        private final int fn;
        private final String name;
        
        BAD_BASE(int fn, String name) {
            this.fn = fn;
            this.name = name;
        }
        
        @Override
        public int getFieldNumber() {
            return fn;
        }
        
        @Override
        public String getFieldName() {
            return name;
        }
    }
    
    @Test
    public void testParseFieldNumber() {
        assertEquals(FIELD_BASE.UNKNOWN, FIELD.parseFieldNumber(0));
        assertEquals(FIELD_BASE.CFIELD, FIELD.parseFieldNumber(3));
        assertEquals(FIELD_BASE.AFIELD, FIELD.parseFieldNumber(1));
        assertEquals(FIELD_BASE.BFIELD, FIELD.parseFieldNumber(2));
    }
    
    @Test
    public void testParseFieldName() {
        assertEquals(FIELD_BASE.UNKNOWN, FIELD.parseFieldName("unknown"));
        assertEquals(FIELD_BASE.AFIELD, FIELD.parseFieldName("aField"));
        assertEquals(FIELD_BASE.CFIELD, FIELD.parseFieldName("cField"));
        assertEquals(FIELD_BASE.BFIELD, FIELD.parseFieldName("bField"));
    }
    
    @Test
    public void testInvalidEnum() {
        // this should throw because BAD_BASE doesn't contain a 0 index
        assertThrows(IllegalArgumentException.class, () -> new ProtostuffField<>(BAD_BASE.class));
    }
}
