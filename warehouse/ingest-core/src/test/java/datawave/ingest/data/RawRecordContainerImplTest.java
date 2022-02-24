package datawave.ingest.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import datawave.data.hash.UID;
import datawave.ingest.config.IngestConfigurationFactory;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.config.MarkingsHelper;

import datawave.util.TypeRegistryTestSetup;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class RawRecordContainerImplTest {
    
    public class TestCSVIngestHelper {
        
    }
    
    public class TestCSVReader {
        
    }
    
    private Configuration conf = null;
    
    private String field1 = "field1";
    private String field2 = "field2";
    private String uuid = UUID.randomUUID().toString();
    private String rawFileName = "testFile1.dat";
    private long rawRecordNumber = 32;
    private Type dataType = null;
    private Date now = new Date();
    
    private String csv = "20150101121500,field2,field3";
    
    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource("config/all-config.xml"));
        
        conf.set("samplecsv" + TypeRegistry.INGEST_HELPER, TestCSVIngestHelper.class.getName());
        conf.set("samplecsv.reader.class", TestCSVReader.class.getName());
        conf.set("samplecsv" + MarkingsHelper.DEFAULT_MARKING, "PUBLIC|PRIVATE");
        TypeRegistryTestSetup.resetTypeRegistry(conf);
        dataType = TypeRegistry.getType("samplecsv");
    }
    
    private ValidatingRawRecordContainerImpl create() {
        ValidatingRawRecordContainerImpl event = new ValidatingRawRecordContainerImpl();
        event.setConf(conf);
        event.setDate(now.getTime());
        event.setDataType(dataType);
        event.setRawFileName(rawFileName);
        event.setRawRecordNumber(rawRecordNumber);
        event.getIds().add(uuid);
        return event;
    }
    
    @Test
    public void testPopulateAll() {
        ValidatingRawRecordContainerImpl event = create();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertTrue(event.getErrors().isEmpty());
        assertEquals(0, event.getErrors().size());
        
        event = create();
        event.setVisibility("TESTVIS1&TESTVIS2");
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertTrue(event.getErrors().isEmpty());
        assertEquals(0, event.getErrors().size());
    }
    
    @Test
    public void testEquals() {
        ValidatingRawRecordContainerImpl e1 = create();
        e1.setRawData(csv.getBytes());
        e1.generateId(null);
        e1.validate();
        
        ValidatingRawRecordContainerImpl e2 = create();
        e2.setRawData(csv.getBytes());
        e2.generateId(null);
        e2.validate();
        
        assertTrue(e1.equals(e2));
        assertTrue(e2.equals(e1));
        
        e1 = create();
        e1.setRawData(csv.getBytes());
        e1.generateId(null);
        e1.validate();
        
        e2 = create();
        e2.setRawData(csv.getBytes());
        e2.generateId(null);
        e2.validate();
        
        assertTrue(e1.equals(e2));
        assertTrue(e2.equals(e1));
    }
    
    @Test
    public void testNotEquals() {
        ValidatingRawRecordContainerImpl e1 = create();
        e1.setRawData(csv.getBytes());
        e1.generateId(field1);
        e1.validate();
        
        ValidatingRawRecordContainerImpl e2 = create();
        e2.setRawData(csv.getBytes());
        e2.generateId(field2);
        e2.validate();
        
        assertTrue(!e1.equals(e2));
        assertTrue(!e2.equals(e1));
        
        e2 = create();
        e2.setRawData(csv.getBytes());
        e2.generateId(null);
        e2.validate();
        
        assertTrue(!e1.equals(e2));
        assertTrue(!e2.equals(e1));
    }
    
    @Test
    public void testSerialization() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        
        // Write out the event
        ValidatingRawRecordContainerImpl e1 = create();
        e1.setRawData(csv.getBytes());
        e1.generateId(null);
        e1.validate();
        e1.write(out);
        
        out.close();
        
        // Read in the event
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes.toByteArray());
        DataInputStream in = new DataInputStream(bytesIn);
        
        // Read in the event
        ValidatingRawRecordContainerImpl e2 = new ValidatingRawRecordContainerImpl();
        e2.setConf(conf);
        e2.readFields(in);
        
        // Check to see if they are equal
        assertEquals(e1, e2);
        assertEquals(e2, e1);
        
        bytes = new ByteArrayOutputStream();
        out = new DataOutputStream(bytes);
        
        // Write out the event
        e1 = create();
        e1.setRawData(csv.getBytes());
        e1.generateId(null);
        e1.validate();
        e1.write(out);
        
        out.close();
        
        // Read in the event
        bytesIn = new ByteArrayInputStream(bytes.toByteArray());
        in = new DataInputStream(bytesIn);
        
        // Read in the event
        e2 = new ValidatingRawRecordContainerImpl();
        e2.setConf(conf);
        e2.readFields(in);
        
        // Check to see if they are equal
        assertEquals(e1, e2);
        assertEquals(e2, e1);
        
    }
    
    @Test
    public void testMissingRecordNumber() {
        ValidatingRawRecordContainerImpl event = new ValidatingRawRecordContainerImpl();
        event.setConf(conf);
        event.setDate(now.getTime());
        event.setDataType(dataType);
        event.setRawFileName(rawFileName);
        event.getIds().add(uuid);
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertFalse(event.fatalError());
        assertTrue(event.ignorableError());
        Collection<String> errors = event.getErrors();
        System.out.println("# of errors = " + errors.size());
        assertTrue(errors.contains(RawDataErrorNames.INVALID_RECORD_NUMBER));
    }
    
    @Test
    public void testMissingUuids() {
        ValidatingRawRecordContainerImpl event = create();
        event.getIds().clear();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertTrue(event.fatalError());
        assertFalse(event.ignorableError());
        Collection<String> errors = event.getErrors();
        assertTrue(errors.contains(RawDataErrorNames.UUID_MISSING));
        
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertTrue(event.fatalError());
        errors = event.getErrors();
        assertFalse(errors.isEmpty());
        
    }
    
    @Test
    public void testCopy() {
        ValidatingRawRecordContainerImpl event = create();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        RawRecordContainerImpl copy = event.copy();
        assertEquals(event, copy);
    }
    
    @Test
    public void testUID() {
        ValidatingRawRecordContainerImpl event = create();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        UID uid = event.getUid();
        assertEquals(-1, uid.getTime());
        
        conf.set(RawRecordContainerImpl.USE_TIME_IN_UID, "true");
        event.reloadConfiguration();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        uid = event.getUid();
        assertNotSame(-1, uid.getTime());
        
        conf.set(RawRecordContainerImpl.USE_TIME_IN_UID, "false");
        event.reloadConfiguration();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        uid = event.getUid();
        assertEquals(-1, uid.getTime());
        
        conf.set(event.getDataType().typeName() + '.' + RawRecordContainerImpl.USE_TIME_IN_UID, "false");
        event.reloadConfiguration();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        uid = event.getUid();
        assertEquals(-1, uid.getTime());
        
        conf.set(event.getDataType().typeName() + '.' + RawRecordContainerImpl.USE_TIME_IN_UID, "true");
        event.reloadConfiguration();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        uid = event.getUid();
        assertNotSame(-1, uid.getTime());
    }
    
    @Test
    public void testCopiedObjectAfterClear() {
        // Create original
        RawRecordContainerImpl event = create();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        ((ValidatingRawRecordContainerImpl) event).validate();
        // Create copy
        RawRecordContainerImpl copy = event.copy();
        assertEquals(event, copy);
        // Create copy of copy
        RawRecordContainerImpl copy2 = copy.copy();
        assertEquals(event, copy);
        assertEquals(event, copy2);
        // Clear the original
        event.clear();
        // Test
        assertEquals(copy, copy2);
    }
    
    @Test
    public void testFatalErrors() {
        conf.set(RawRecordContainerImpl.IGNORABLE_ERROR_HELPERS, TestIgnorableHelper.class.getName());
        
        ValidatingRawRecordContainerImpl event = new ValidatingRawRecordContainerImpl();
        event.setConf(conf);
        event.setDataType(dataType);
        event.setRawFileName(rawFileName);
        event.setRawRecordNumber(rawRecordNumber);
        event.getIds().add(uuid);
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertTrue(event.fatalError());
        assertFalse(event.ignorableError());
        
        event = new ValidatingRawRecordContainerImpl();
        event.setConf(conf);
        event.setDate(now.getTime());
        event.setDataType(dataType);
        event.setRawFileName(rawFileName);
        event.setRawRecordNumber(rawRecordNumber);
        event.getIds().add(uuid);
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertFalse(event.fatalError());
        assertTrue(event.ignorableError());
        
        event.clearErrors();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertFalse(event.fatalError());
        assertTrue(event.ignorableError());
        
        event.clearErrors();
        // Need to create the type directly as we cannot generate this type from the registry
        event.setDataType(TypeRegistry.getType("samplecsv"));
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertFalse(event.fatalError());
        assertTrue(event.ignorableError());
        
        event.reloadConfiguration();
        event.getIds().clear();
        event.setRawData(csv.getBytes());
        event.generateId(null);
        event.validate();
        assertTrue(event.ignorableError());
    }
    
    public static class ValidatingRawRecordContainerImpl extends RawRecordContainerImpl {
        
        private MarkingsHelper markingsHelper;
        
        private void initializeMarkingsHelper() throws IOException {
            if (getDataType() != null && getConf() != null) {
                markingsHelper = IngestConfigurationFactory.getIngestConfiguration().getMarkingsHelper(getConf(), getDataType());
            } else {
                throw new IllegalStateException("Data type or Configuration is NULL!");
            }
        }
        
        public void validate() {
            if (getAltIds() == null || getAltIds().isEmpty()) {
                addError(RawDataErrorNames.UUID_MISSING);
            }
            if (Long.MIN_VALUE == getDate()) {
                addError(RawDataErrorNames.EVENT_DATE_MISSING);
            }
            if (0 == getRawRecordNumber()) {
                addError(RawDataErrorNames.INVALID_RECORD_NUMBER);
            }
            
            if (0L == getRawFileTimestamp()) {
                setRawFileTimestamp(getDate());
            }
            
            try {
                initializeMarkingsHelper();
            } catch (Throwable t) {
                addError(RawDataErrorNames.RUNTIME_EXCEPTION);
            }
            
            if (getVisibility() == null && getSecurityMarkings() == null) {
                if (markingsHelper != null) {
                    setSecurityMarkings(markingsHelper.getDefaultMarkings());
                } else {}
            }
            
            if (getSecurityMarkings() == null) {
                addError(RawDataErrorNames.MISSING_DATA_ERROR);
            }
        }
        
        @Override
        public ValidatingRawRecordContainerImpl copy() {
            return (ValidatingRawRecordContainerImpl) copyInto(new ValidatingRawRecordContainerImpl());
        }
    }
}
