package datawave.ingest.mapreduce;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.util.TypeRegistryTestSetup;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Test class containing some reusable test code for creating a test Type and test records.
 */
public class IngestTestSetup {
    
    static SimpleRawRecord createRecord(String dataTypeHandler, Configuration conf) {
        Type type = setupTypeAndTypeRegistry(dataTypeHandler, conf);
        long eventTime = System.currentTimeMillis();
        return createBasicRecord(eventTime, type);
    }
    
    public static RawRecordContainer createBasicRecord() {
        Type type = IngestTestSetup.createBasicType();
        long eventTime = System.currentTimeMillis();
        return createBasicRecord(eventTime, type);
    }
    
    static Type setupTypeAndTypeRegistry(String dataTypeHandler, Configuration conf) {
        String[] defaultDataTypeHandlers = {dataTypeHandler};
        Type type = new Type("file", null, null, defaultDataTypeHandlers, 10, null);
        
        String[] errorDataTypeHandlers = {SimpleDataTypeHandler.class.getName()};
        Type errorType = new Type(TypeRegistry.ERROR_PREFIX, null, null, errorDataTypeHandlers, 20, null);
        
        TypeRegistryTestSetup.resetTypeRegistryWithTypes(conf, type, errorType);
        return type;
    }
    
    static Type createBasicType() {
        return createBasicType(new String[] {});
    }
    
    static Type createBasicType(String[] defaultDataTypeHandlers) {
        return new Type("file", null, null, defaultDataTypeHandlers, 10, null);
    }
    
    static SimpleRawRecord createBasicRecord(long eventTime, Type type) {
        SimpleRawRecord result = new SimpleRawRecord();
        result.setDate(eventTime);
        result.setRawFileTimestamp(eventTime);
        result.setDataType(type);
        result.setRawFileName("/some/filename");
        result.setRawData("some data".getBytes());
        result.generateId(null);
        return result;
    }
    
    static byte[] objectToRawBytes(Object map) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        new ObjectOutputStream(outputStream).writeObject(map);
        return outputStream.toByteArray();
    }
}
