package datawave.ingest.data.config;

import java.util.Map;

import datawave.data.type.Type;
import org.junit.Assert;
import org.junit.Test;

public class NormalizedFieldAndValueTest {
    
    public static class NonGroupedInstance implements NormalizedContentInterface {
        
        private String _fieldName;
        
        private String _indexedFieldName;
        private String _indexedFieldValue;
        
        private String _eventFieldName;
        private String _eventFieldValue;
        
        private Map<String,String> _markings;
        private Throwable _error;
        
        protected NonGroupedInstance() {
            _fieldName = "TestNonGroupedInstance";
            
            _indexedFieldName = "TestIndexedField";
            _indexedFieldValue = "hello, world";
            
            _eventFieldName = "TestEventField";
            _eventFieldValue = "hello, world";
            
            _markings = null;
            _error = null;
            
        }
        
        @Override
        public void setFieldName(String name) {
            
            _fieldName = name;
        }
        
        public String getFieldName() {
            
            return _fieldName;
        }
        
        @Override
        public String getIndexedFieldName() {
            
            return _indexedFieldName;
        }
        
        @Override
        public String getEventFieldName() {
            
            return _eventFieldName;
        }
        
        @Override
        public String getEventFieldValue() {
            
            return _eventFieldValue;
        }
        
        @Override
        public void setEventFieldValue(String val) {
            
            _eventFieldValue = val;
        }
        
        @Override
        public String getIndexedFieldValue() {
            
            return _indexedFieldValue;
        }
        
        @Override
        public void setIndexedFieldValue(String val) {
            
            _indexedFieldValue = val;
        }
        
        @Override
        public void setMarkings(Map<String,String> markings) {
            _markings = markings;
        }
        
        @Override
        public void setError(Throwable e) {
            
            _error = e;
        }
        
        @Override
        public Map<String,String> getMarkings() {
            return _markings;
        }
        
        @Override
        public Throwable getError() {
            
            return _error;
        }
        
        @Override
        public Object clone() {
            return this;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see datawave.ingest.data.config.NormalizedContentInterface#normalize(datawave.data.type.Type)
         */
        @Override
        public void normalize(Type<?> datawaveType) {
            try {
                this.setIndexedFieldValue(datawaveType.normalize(this.getIndexedFieldValue()));
            } catch (Exception e) {
                this.setError(e);
            }
        }
    }
    
    public static class GroupedInstance implements GroupedNormalizedContentInterface {
        
        private String _fieldName;
        
        private String _indexedFieldName;
        private String _indexedFieldValue;
        
        private String _eventFieldName;
        private String _eventFieldValue;
        
        private Map<String,String> _markings;
        private Throwable _error;
        private boolean _grouped;
        
        private String _group;
        private String _subGroup;
        
        protected GroupedInstance() {
            _fieldName = "TestNonGroupedInstance";
            
            _indexedFieldName = "TestIndexedField";
            _indexedFieldValue = "hello, world";
            
            _eventFieldName = "TestEventField";
            _eventFieldValue = "hello, world";
            
            _markings = null;
            _error = null;
            
            _grouped = true;
            _group = "group";
            _subGroup = "subGroup";
            
        }
        
        @Override
        public void setFieldName(String name) {
            
            _fieldName = name;
        }
        
        public String getFieldName() {
            
            return _fieldName;
        }
        
        @Override
        public String getIndexedFieldName() {
            
            return _indexedFieldName;
        }
        
        @Override
        public String getEventFieldName() {
            
            return _eventFieldName;
        }
        
        @Override
        public String getEventFieldValue() {
            
            return _eventFieldValue;
        }
        
        @Override
        public void setEventFieldValue(String val) {
            
            _eventFieldValue = val;
        }
        
        @Override
        public String getIndexedFieldValue() {
            
            return _indexedFieldValue;
        }
        
        @Override
        public void setIndexedFieldValue(String val) {
            
            _indexedFieldValue = val;
        }
        
        @Override
        public void setMarkings(Map<String,String> markings) {
            _markings = markings;
        }
        
        @Override
        public void setError(Throwable e) {
            
            _error = e;
        }
        
        @Override
        public Map<String,String> getMarkings() {
            return _markings;
        }
        
        @Override
        public Throwable getError() {
            
            return _error;
        }
        
        @Override
        public boolean isGrouped() {
            
            return _grouped;
        }
        
        @Override
        public void setGrouped(boolean grouped) {
            
            _grouped = grouped;
        }
        
        @Override
        public String getSubGroup() {
            
            return _subGroup;
        }
        
        @Override
        public void setSubGroup(String group) {
            
            _subGroup = group;
        }
        
        @Override
        public String getGroup() {
            
            return _group;
        }
        
        @Override
        public void setGroup(String type) {
            
            _group = type;
        }
        
        @Override
        public Object clone() {
            return this;
        }
        
        @Override
        public void setEventFieldName(String name) {
            
            _eventFieldName = name;
        }
        
        @Override
        public void setIndexedFieldName(String name) {
            
            _indexedFieldName = name;
        }
        
        @Override
        public void normalize(Type<?> datawaveType) {
            try {
                this.setIndexedFieldValue(datawaveType.normalize(this.getIndexedFieldValue()));
            } catch (Exception e) {
                this.setError(e);
            }
        }
    }
    
    @Test
    public void testConstructorPassedNonGroupedInterfaceInstance() {
        
        NormalizedContentInterface nci = new NormalizedFieldAndValueTest.NonGroupedInstance();
        
        NormalizedFieldAndValue uut = new NormalizedFieldAndValue(nci);
        
        Assert.assertNull("Constructor created a non-existance Group for Test Content", uut.getGroup());
        Assert.assertNull("Constructor created a non-existance SubGroup for Test Content", uut.getSubGroup());
    }
    
    @Test
    public void testConstructorPassedGroupedInterfaceInstance() {
        
        NormalizedContentInterface nci = new NormalizedFieldAndValueTest.GroupedInstance();
        
        NormalizedFieldAndValue uut = new NormalizedFieldAndValue(nci);
        
        Assert.assertTrue("Constructor failed to create a Group for the Test Content", uut.isGrouped());
        Assert.assertNotNull("Constructor failed to create Group for Test Content", uut.getGroup());
        Assert.assertNotNull("Constructor failed to create a SubGroup for Test Content", uut.getSubGroup());
    }
    
    @Test
    public void testDecode() {
        
        String testValue = "Hello, World";
        
        // Test a simple String as bytes but not expecting it to be Binary
        String results = NormalizedFieldAndValue.decode(testValue.getBytes(), false);
        Assert.assertEquals("Decode failed to correctly decode test value", testValue, results);
        
        // Test a simple String as bytes but expecting it to be Binary
        results = NormalizedFieldAndValue.decode(testValue.getBytes(), true);
        Assert.assertEquals("Decode failed to correctly decode test value", testValue, results);
        
        String expected = "a";
        byte[] binary = new byte[] {(byte) 0x61};
        
        results = NormalizedFieldAndValue.decode(binary, false);
        Assert.assertEquals("Decode failed to correctly decode binary test", expected, results);
        
        results = NormalizedFieldAndValue.decode(binary, true);
        Assert.assertEquals("Decode failed to correctly decode binary test", expected, results);
        
    }
    
    @Test
    public void testGetDefaultEventFieldName() {
        
        NormalizedContentInterface nci = new NormalizedFieldAndValueTest.GroupedInstance();
        
        NormalizedFieldAndValue uut = new NormalizedFieldAndValue(nci);
        uut.setEventFieldName(null);
        
        String generatedEventFieldName = uut.getEventFieldName();
        String expectedName = "TestIndexedField.GROUP.SUBGROUP";
        
        Assert.assertEquals("NormalizedFieldAndValue.getEventFieldName failed to generate the expected default Event Field Name", expectedName,
                        generatedEventFieldName);
        
        nci = new NormalizedFieldAndValueTest.NonGroupedInstance();
        
        uut = new NormalizedFieldAndValue(nci);
        expectedName = "TestIndexedField";
        generatedEventFieldName = uut.getEventFieldName();
        
        Assert.assertEquals("NormalizedFieldAndValue.getEventFieldName failed to generate the expected default Event Field Name", expectedName,
                        generatedEventFieldName);
    }
}
