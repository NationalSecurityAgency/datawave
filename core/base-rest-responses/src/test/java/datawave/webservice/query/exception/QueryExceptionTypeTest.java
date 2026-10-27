package datawave.webservice.query.exception;

import org.junit.jupiter.api.Test;

import datawave.webservice.ProtobufSerializationTestBaseJUnit5;

public class QueryExceptionTypeTest extends ProtobufSerializationTestBaseJUnit5 {
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
