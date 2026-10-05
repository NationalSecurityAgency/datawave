package datawave.webservice.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.QueryImpl.Parameter;

public class TestQueryImpl {

    QueryImpl q = null;

    public static final String QUERY_SYNTAX = "query.syntax";
    public static final String NON_EVENT_KEY_PREFIXES = "non.event.key.prefixes";
    public static final String DISALLOWLISTED_FIELDS = "disallowlisted.fields";
    public static final String RETURN_FIELDS = "return.fields";
    public static final String INCLUDE_DATATYPE_AS_FIELD = "include.datatype.as.field";

    @BeforeEach
    public void setup() {
        q = new QueryImpl();
    }

    @Test
    public void testForNPEinAddParam() {

        // when no previous parameters were added using setParameters, addParameter used to throw a NPE
        // test passes by not
        try {
            q.addParameter(QUERY_SYNTAX, "LUCENE");
        } catch (NullPointerException e) {
            fail();
        }
    }

    @Test
    public void testFindParameter() {
        Set<Parameter> parameters = new HashSet<Parameter>();
        parameters.add(new Parameter(QUERY_SYNTAX, "LUCENE"));
        parameters.add(new Parameter(NON_EVENT_KEY_PREFIXES, "value2"));
        parameters.add(new Parameter(DISALLOWLISTED_FIELDS, "value3"));
        parameters.add(new Parameter(RETURN_FIELDS, "value4"));
        parameters.add(new Parameter(INCLUDE_DATATYPE_AS_FIELD, "value5"));
        q.setParameters(parameters);

        q.addParameter(QUERY_SYNTAX, "JEXL");
        assertEquals("JEXL", q.findParameter(QUERY_SYNTAX).getParameterValue());
    }

}
