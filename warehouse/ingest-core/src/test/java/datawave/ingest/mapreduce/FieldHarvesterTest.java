package datawave.ingest.mapreduce;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValueTest;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.data.config.ingest.MinimalistIngestHelperImpl;
import datawave.ingest.data.config.ingest.VirtualIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.NDC;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

public class FieldHarvesterTest {
    private static final String SAMPLE_FIELD_NAME = "SAMPLE_FIELD_NAME";
    private static final String COMPOSITE_FIELD = "COMPOSITE_FIELD";
    private static final String VIRTUAL_FIELD = "VIRTUAL_FIELD";
    private static final String SEQ_FILENAME_ONLY = "InputFile.seq";
    private static final String SEQ_FILENAME_AND_DIR = "/input/directory/" + SEQ_FILENAME_ONLY;
    private static final int NUM_SUPPLEMENTAL_FIELDS = 3;
    private static final String LOAD_DATE = "LOAD_DATE";
    private static final String ORIG_FILE = "ORIG_FILE";
    private static final String RAW_FILE = "RAW_FILE";
    
    private final Multimap<String,NormalizedContentInterface> fields = HashMultimap.create();
    private FieldHarvester fieldHarvester;
    private long offset = 0;
    private String splitStart = null;
    private RawRecordContainer value;
    
    @Before
    public void before() {
        value = IngestTestSetup.createBasicRecord();
        value.setRawData(null); // rawData is ignored by below implementations of getEventFields
        fieldHarvester = new FieldHarvester(new Configuration());
        NDC.push(SEQ_FILENAME_AND_DIR);
    }
    
    @After
    public void after() {
        NDC.pop();
    }
    
    @Test
    public void reusableFieldHarvester() throws Exception {
        // The first call to extractFields produces an error, adding only supplemental fields
        exceptionSwallowingExtractFields(fieldHarvester, fields, null, value, offset, splitStart);
        
        // Verify error is captured (NullPointerException because null provided as the IngestHelperInterface param)
        assertExceptionCaptured(fieldHarvester, NullPointerException.class);
        assertContainsOnlySupplementalFields();
        
        // The second extractFields calls an IngestHelper that doesn't error
        // There should be no residue from the prior call (prior errors cleared, prior fields cleared)
        // field map with single field and value
        fields.clear();
        fieldHarvester.extractFields(fields, new BasicIngestHelper(createOneFieldMultiMap()), value, offset, splitStart);
        
        // Verify it contains expected fields
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME));
        assertContainsSupplementalFields(fields);
        Assert.assertEquals(fields.toString(), NUM_SUPPLEMENTAL_FIELDS + 1, fields.size());
        assertNoErrors(fieldHarvester);
        
        // The third call is just like the first call, throwing an exception
        fields.clear();
        exceptionSwallowingExtractFields(fieldHarvester, fields, null, value, offset, splitStart);
        
        // Verify error is captured (NullPointerException because null provided as IngestHelperInterface)
        assertExceptionCaptured(fieldHarvester, NullPointerException.class);
        assertContainsOnlySupplementalFields();
    }
    
    private void exceptionSwallowingExtractFields(FieldHarvester fieldHarvester, Multimap<String,NormalizedContentInterface> fields,
                    IngestHelperInterface ingestHelper, RawRecordContainer value, long offset, String splitStart) {
        try {
            fieldHarvester.extractFields(fields, ingestHelper, value, offset, splitStart);
        } catch (Exception e) {
            // expected case
            return;
        }
        Assert.fail("Expected an exception");
    }
    
    @Test
    public void disableSeqFileNameCreation() throws Exception {
        // Configuration disables seq file name creation
        Configuration config = new Configuration();
        config.setBoolean(FieldHarvester.LOAD_SEQUENCE_FILE_NAME, false);
        FieldHarvester fieldHarvester = new FieldHarvester(config);
        
        // field map with single field and value
        fieldHarvester.extractFields(fields, new BasicIngestHelper(createOneFieldMultiMap()), value, offset, splitStart);
        
        // Verify it contains expected fields
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME));
        Assert.assertTrue(fields.containsKey(LOAD_DATE));
        Assert.assertTrue(fields.containsKey(RAW_FILE));
        // excluded due to config
        Assert.assertFalse(fields.containsKey(ORIG_FILE));
        Assert.assertEquals(fields.toString(), 3, fields.size());
        
        // Verify there was no exception
        assertNoErrors(fieldHarvester);
    }
    
    @Test
    public void disableTrimmingSeqFileName() throws Exception {
        // Configuration disables trimming of seq file name
        Configuration config = new Configuration();
        config.setBoolean(FieldHarvester.TRIM_SEQUENCE_FILE_NAME, false);
        FieldHarvester fieldHarvester = new FieldHarvester(config);
        
        // field map with single field and value
        fieldHarvester.extractFields(fields, new BasicIngestHelper(createOneFieldMultiMap()), value, offset, splitStart);
        
        // Verify it contains expected fields
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME));
        assertContainsSupplementalFields(fields);
        Assert.assertEquals(fields.toString(), NUM_SUPPLEMENTAL_FIELDS + 1, fields.size());
        
        Collection<NormalizedContentInterface> result = fields.get(ORIG_FILE);
        NormalizedContentInterface fieldValue = result.iterator().next();
        Assert.assertEquals(SEQ_FILENAME_AND_DIR + "|0", fieldValue.getEventFieldValue());
        Assert.assertEquals(SEQ_FILENAME_AND_DIR + "|0", fieldValue.getIndexedFieldValue());
        
        // Verify there was no exception
        assertNoErrors(fieldHarvester);
    }
    
    @Test
    public void enableTrimmingSeqFileName() throws Exception {
        // Default configuration enables trimming of seq file name
        
        // field map with single field and value
        fieldHarvester.extractFields(fields, new BasicIngestHelper(createOneFieldMultiMap()), value, offset, splitStart);
        
        // Verify it contains expected fields
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME));
        assertContainsSupplementalFields(fields);
        Assert.assertEquals(fields.toString(), NUM_SUPPLEMENTAL_FIELDS + 1, fields.size());
        
        Collection<NormalizedContentInterface> result = fields.get(ORIG_FILE);
        NormalizedContentInterface fieldValue = result.iterator().next();
        Assert.assertEquals(SEQ_FILENAME_ONLY + "|0", fieldValue.getEventFieldValue());
        Assert.assertEquals(SEQ_FILENAME_ONLY + "|0", fieldValue.getIndexedFieldValue());
        
        // Verify there was no exception
        assertNoErrors(fieldHarvester);
    }
    
    @Test
    public void disableRawFileName() throws Exception {
        // Configuration disables raw file name creation
        Configuration config = new Configuration();
        config.setBoolean(FieldHarvester.LOAD_RAW_FILE_NAME, false);
        FieldHarvester fieldHarvester = new FieldHarvester(config);
        
        // field map with single field and value
        fieldHarvester.extractFields(fields, new BasicIngestHelper(createOneFieldMultiMap()), value, offset, splitStart);
        
        // Verify it contains expected fields
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME));
        Assert.assertTrue(fields.containsKey(LOAD_DATE));
        Assert.assertTrue(fields.containsKey(ORIG_FILE));
        // excluded due to config
        Assert.assertFalse(fields.containsKey(RAW_FILE));
        Assert.assertEquals(fields.toString(), 3, fields.size());
        
        // Verify there was no exception
        assertNoErrors(fieldHarvester);
    }
    
    @Test
    public void addsVirtualFields() throws Exception {
        // Ensure that a virtual field is added
        Multimap<String,NormalizedContentInterface> fields = createOneFieldMultiMap();
        IngestHelperInterface ingestHelper = new BasicWithVirtualFieldsIngestHelper(fields);
        
        // field map with single field and value
        fieldHarvester.extractFields(this.fields, ingestHelper, value, offset, splitStart);
        
        // Verify field returned
        Assert.assertTrue(this.fields.containsKey(SAMPLE_FIELD_NAME));
        // Verify field is used for virtual field creation
        Assert.assertTrue(this.fields.containsKey(SAMPLE_FIELD_NAME + VIRTUAL_FIELD));
        assertContainsSupplementalFields(fields);
        Assert.assertEquals(this.fields.toString(), 5, this.fields.size());
        
        // Verify there was no exception
        assertNoErrors(fieldHarvester);
    }
    
    @Test
    public void addsCompositeFields() throws Exception {
        // cause exception in getEventFields, get salvaged fields and ensure they're used for composite field creation
        Multimap<String,NormalizedContentInterface> salvagableFields = createOneFieldMultiMap();
        IngestHelperInterface ingestHelper = new BasicWithCompositeFieldsIngestHelper(salvagableFields);
        
        // field map with single field and value
        fieldHarvester.extractFields(fields, ingestHelper, value, offset, splitStart);
        
        // Verify field returned
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME));
        // Verify field is used for composite
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME + COMPOSITE_FIELD));
        assertContainsSupplementalFields(fields);
        Assert.assertEquals(fields.toString(), 5, fields.size());
        
        // Verify there was no exception
        assertNoErrors(fieldHarvester);
    }
    
    @Test
    public void supplementsSalvagedFields() {
        // cause exception in getEventFields, get salvaged fields and ensure they're used for virtual and composite
        Multimap<String,NormalizedContentInterface> salvagableFields = createOneFieldMultiMap();
        ErroringSalvagableIngestHelper ingestHelper = new ErroringSalvagableIngestHelper(salvagableFields);
        
        // field map with single field and value
        exceptionSwallowingExtractFields(fieldHarvester, fields, ingestHelper, value, offset, splitStart);
        
        // Verify salvaged fields returned
        // Verify salvaged fields are used for virtual, composite
        // Not virtual field is used by composite field implementation
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME));
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME + VIRTUAL_FIELD));
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME + COMPOSITE_FIELD));
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME + VIRTUAL_FIELD + COMPOSITE_FIELD));
        assertContainsSupplementalFields(fields);
        Assert.assertEquals(fields.toString(), 7, fields.size());
        
        // Verify an exception was detected (for getEventFields)
        assertExceptionCaptured(this.fieldHarvester, UnsupportedOperationException.class);
    }
    
    @Test
    public void emptySalvagedFields() {
        // cause exception in getEventFields, causing retrieval of empty multimap of salvaged fields
        ErroringSalvagableIngestHelper ingestHelper = new ErroringSalvagableIngestHelper(HashMultimap.create());
        
        // field map with empty fields
        exceptionSwallowingExtractFields(fieldHarvester, fields, ingestHelper, value, offset, splitStart);
        
        // empty salvaged fields
        assertContainsOnlySupplementalFields();
        
        // Verify an exception was detected (for getEventFields)
        assertExceptionCaptured(this.fieldHarvester, UnsupportedOperationException.class);
    }
    
    @Test
    public void doubleException() {
        // exception in getEventFields and in salvager
        exceptionSwallowingExtractFields(fieldHarvester, fields, new DoubleErrorIngestHelper(), value, offset, splitStart);
        
        // Verify it contains expected fields
        assertContainsOnlySupplementalFields();
        
        // Verify the original exception was captured
        assertExceptionCaptured(this.fieldHarvester, UnsupportedOperationException.class);
    }
    
    @Test
    public void extractFields() throws Exception {
        // field map with single field and value
        fieldHarvester.extractFields(fields, new BasicIngestHelper(createOneFieldMultiMap()), value, offset, splitStart);
        
        // Verify it contains expected fields
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME));
        assertContainsSupplementalFields(fields);
        Assert.assertEquals(fields.toString(), NUM_SUPPLEMENTAL_FIELDS + 1, fields.size());
        
        assertNoErrors(fieldHarvester);
    }
    
    @Test
    public void erroredFieldExcluded() {
        // create a field containing a field error
        NormalizedFieldAndValueTest.NonGroupedInstance fieldWithError = new NormalizedFieldAndValueTest.NonGroupedInstance();
        fieldWithError.setError(new Exception());
        
        // field map contains a field containing a field error
        Multimap<String,NormalizedContentInterface> multiMap = HashMultimap.create();
        multiMap.put(SAMPLE_FIELD_NAME, fieldWithError);
        
        exceptionSwallowingExtractFields(fieldHarvester, fields, new BasicIngestHelper(multiMap), this.value, offset, splitStart);
        
        // Verify fields contains expected fields
        Assert.assertTrue(fields.containsKey(SAMPLE_FIELD_NAME));
        assertContainsSupplementalFields(fields);
        Assert.assertEquals(fields.toString(), NUM_SUPPLEMENTAL_FIELDS + 1, fields.size());
        
        // Verify there was a FieldNormalizationError due to the field error
        assertExceptionCaptured(this.fieldHarvester, FieldHarvester.FieldNormalizationError.class);
    }
    
    @Test
    public void nullIngestHelper() {
        // field map with single field and value
        exceptionSwallowingExtractFields(fieldHarvester, fields, null, value, offset, splitStart);
        
        // Verify it contains expected fields
        assertContainsOnlySupplementalFields();
        
        // Verify it captured the null pointer exception
        assertExceptionCaptured(this.fieldHarvester, NullPointerException.class);
    }
    
    private void assertNoErrors(FieldHarvester fieldHarvester) {
        Assert.assertFalse("Unexpected exception: " + fieldHarvester.getOriginalException(), fieldHarvester.hasError());
        Assert.assertNull(fieldHarvester.getOriginalException());
    }
    
    private void assertExceptionCaptured(FieldHarvester fieldHarvester, Class exceptionClass) {
        Assert.assertTrue(fieldHarvester.hasError());
        Assert.assertEquals(exceptionClass, fieldHarvester.getOriginalException().getClass());
    }
    
    private Multimap<String,NormalizedContentInterface> createOneFieldMultiMap() {
        Multimap<String,NormalizedContentInterface> multiMap = HashMultimap.create();
        multiMap.put(SAMPLE_FIELD_NAME, new NormalizedFieldAndValueTest.NonGroupedInstance());
        return multiMap;
    }
    
    private void assertContainsSupplementalFields(Multimap<String,NormalizedContentInterface> fields) {
        Assert.assertTrue(fields.containsKey(LOAD_DATE));
        Assert.assertTrue(fields.containsKey(ORIG_FILE));
        Assert.assertTrue(fields.containsKey(RAW_FILE));
    }
    
    private void assertContainsOnlySupplementalFields() {
        assertContainsSupplementalFields(fields);
        // and only supplemental
        Assert.assertEquals(fields.toString(), NUM_SUPPLEMENTAL_FIELDS, fields.size());
    }
    
    private static class DoubleErrorIngestHelper extends MinimalistIngestHelperImpl implements FieldSalvager {
        @Override
        public Multimap<String,NormalizedContentInterface> getSalvageableEventFields(RawRecordContainer rawRecordContainer) {
            throw new RuntimeException();
        }
    }
    
    private static class BasicIngestHelper extends MinimalistIngestHelperImpl {
        private final Multimap<String,NormalizedContentInterface> multiMap;
        
        public BasicIngestHelper(Multimap<String,NormalizedContentInterface> multiMap) {
            this.multiMap = multiMap;
        }
        
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
            return multiMap;
        }
    }
    
    private static class BasicWithCompositeFieldsIngestHelper extends BasicIngestHelper implements CompositeIngest {
        public BasicWithCompositeFieldsIngestHelper(Multimap<String,NormalizedContentInterface> multiMap) {
            super(multiMap);
        }
        
        @Override
        public void setCompositeFieldDefinitions(Multimap<String,String> compositeFieldDefinitions) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public Multimap<String,NormalizedContentInterface> getCompositeFields(Multimap<String,NormalizedContentInterface> fields) {
            Multimap<String,NormalizedContentInterface> compositeFields = HashMultimap.create();
            for (Map.Entry<String,NormalizedContentInterface> entry : fields.entries()) {
                compositeFields.put(entry.getKey() + COMPOSITE_FIELD, entry.getValue());
            }
            return compositeFields;
        }
    }
    
    private static class BasicWithVirtualFieldsIngestHelper extends BasicIngestHelper implements VirtualIngest {
        public BasicWithVirtualFieldsIngestHelper(Multimap<String,NormalizedContentInterface> multiMap) {
            super(multiMap);
        }
        
        @Override
        public Map<String,String[]> getVirtualFieldDefinitions() {
            return null;
        }
        
        @Override
        public void setVirtualFieldDefinitions(Map<String,String[]> virtualFieldDefinitions) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public String getDefaultVirtualFieldSeparator() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void setDefaultVirtualFieldSeparator(String separator) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public Multimap<String,NormalizedContentInterface> getVirtualFields(Multimap<String,NormalizedContentInterface> fields) {
            Multimap<String,NormalizedContentInterface> compositeFields = HashMultimap.create();
            for (Map.Entry<String,NormalizedContentInterface> entry : fields.entries()) {
                compositeFields.put(entry.getKey() + VIRTUAL_FIELD, entry.getValue());
            }
            return compositeFields;
        }
    }
    
    private static class ErroringSalvagableIngestHelper extends MinimalistIngestHelperImpl implements VirtualIngest, CompositeIngest, FieldSalvager {
        private final Multimap<String,NormalizedContentInterface> multiMap;
        
        ErroringSalvagableIngestHelper(Multimap<String,NormalizedContentInterface> multiMap) {
            this.multiMap = multiMap;
        }
        
        @Override
        public Multimap<String,NormalizedContentInterface> getSalvageableEventFields(RawRecordContainer rawRecordContainer) {
            return this.multiMap;
        }
        
        @Override
        public void setCompositeFieldDefinitions(Multimap<String,String> compositeFieldDefinitions) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public Multimap<String,NormalizedContentInterface> getCompositeFields(Multimap<String,NormalizedContentInterface> fields) {
            Multimap<String,NormalizedContentInterface> compositeFields = HashMultimap.create();
            for (Map.Entry<String,NormalizedContentInterface> entry : fields.entries()) {
                compositeFields.put(entry.getKey() + COMPOSITE_FIELD, entry.getValue());
            }
            return compositeFields;
        }
        
        @Override
        public Map<String,String[]> getVirtualFieldDefinitions() {
            return null;
        }
        
        @Override
        public void setVirtualFieldDefinitions(Map<String,String[]> virtualFieldDefinitions) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public String getDefaultVirtualFieldSeparator() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void setDefaultVirtualFieldSeparator(String separator) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public Multimap<String,NormalizedContentInterface> getVirtualFields(Multimap<String,NormalizedContentInterface> fields) {
            Multimap<String,NormalizedContentInterface> compositeFields = HashMultimap.create();
            for (Map.Entry<String,NormalizedContentInterface> entry : fields.entries()) {
                compositeFields.put(entry.getKey() + VIRTUAL_FIELD, entry.getValue());
            }
            return compositeFields;
        }
    }
}
