package datawave.webservice.query.exception;

import datawave.webservice.ProtobufSerializationTestBase;
import datawave.webservice.query.exception.QueryExceptionType;

import org.junit.Test;

public class QueryExceptionTypeTest extends ProtobufSerializationTestBase {
    @Test
    public void testFieldConfiguration() {
        String[] expecteds = new String[] {"SCHEMA", "cause", "message", "code", "serialVersionUID"};
        testFieldNames(expecteds, QueryExceptionType.class);
    }
    
    @Test
    public void testSerialization() throws Exception {
        testRoundTrip(QueryExceptionType.class, new String[] {"cause", "message", "code"}, new String[] {"expectedCause", "expectedMessage", "expectedCode"});
    }
}
