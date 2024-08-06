package datawave.core.query.logic.composite;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

public class CompositeLogicExceptionTest {

    @Test
    public void testSingleNonQueryExceptionCause() {
        IllegalArgumentException cause = new IllegalArgumentException("illegal argument");
        CompositeLogicException exception = new CompositeLogicException("composite error occurred", "LogicName", cause);
        assertEquals("composite error occurred:\nLogicName: illegal argument", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void testSingleQueryExceptionCause() {
        QueryException cause = new QueryException(DatawaveErrorCode.MODEL_FETCH_ERROR, "connection failed");
        CompositeLogicException exception = new CompositeLogicException("composite error occurred", "LogicName", cause);

        assertEquals("composite error occurred:\nLogicName: Could not get model. connection failed", exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals(DatawaveErrorCode.MODEL_FETCH_ERROR.getErrorCode(), ((QueryException) exception.getCause()).getErrorCode());
    }

    @Test
    public void testNestedSingleQueryExceptionCause() {
        QueryException nestedCause = new QueryException(DatawaveErrorCode.MODEL_FETCH_ERROR, "connection failed");
        IllegalArgumentException cause = new IllegalArgumentException("illegal argument", nestedCause);
        CompositeLogicException exception = new CompositeLogicException("composite error occurred", "LogicName", cause);
        assertEquals("composite error occurred:\nLogicName: illegal argument", exception.getMessage());
        assertEquals(CompositeRaisedQueryException.class, exception.getCause().getClass());
        assertEquals(DatawaveErrorCode.MODEL_FETCH_ERROR.getErrorCode(), ((CompositeRaisedQueryException) exception.getCause()).getErrorCode());
    }

    @Test
    public void testMultipleNonQueryExceptionCauses() {
        IllegalArgumentException expectedCause = new IllegalArgumentException("illegal name");
        Map<String,Exception> exceptions = new LinkedHashMap<>();
        exceptions.put("logic1", expectedCause);
        exceptions.put("logic2", new NullPointerException("null value"));
        exceptions.put("logic3", new IllegalStateException("bad state"));

        CompositeLogicException exception = new CompositeLogicException("failed to complete", exceptions);
        assertEquals("failed to complete:\nlogic1: illegal name\nlogic2: null value\nlogic3: bad state", exception.getMessage());
        assertEquals(expectedCause, exception.getCause());
    }

    @Test
    public void testMultipleExceptionWithOneTopLevelQueryException() {
        QueryException expectedCause = new QueryException(DatawaveErrorCode.MODEL_FETCH_ERROR, "connection failed");
        Map<String,Exception> exceptions = new LinkedHashMap<>();
        exceptions.put("logic1", new IllegalArgumentException("illegal name"));
        exceptions.put("logic2", new NullPointerException("null value"));
        exceptions.put("logic3", expectedCause);
        exceptions.put("logic4", new IllegalStateException("bad state"));

        CompositeLogicException exception = new CompositeLogicException("failed to complete", exceptions);
        assertEquals("failed to complete:\nlogic1: illegal name\nlogic2: null value\nlogic3: Could not get model. connection failed\nlogic4: bad state",
                        exception.getMessage());
        assertEquals(expectedCause, exception.getCause());
    }

    @Test
    public void testMultipleExceptionWithOneNestedQueryException() {
        QueryException nestedCause = new QueryException(DatawaveErrorCode.MODEL_FETCH_ERROR, "connection failed");
        IllegalStateException topCause = new IllegalStateException("bad state", nestedCause);
        Map<String,Exception> exceptions = new LinkedHashMap<>();
        exceptions.put("logic1", new IllegalArgumentException("illegal name"));
        exceptions.put("logic2", topCause);
        exceptions.put("logic3", new NullPointerException("null value"));

        CompositeLogicException exception = new CompositeLogicException("failed to complete", exceptions);
        assertEquals("failed to complete:\nlogic1: illegal name\nlogic2: bad state\nlogic3: null value", exception.getMessage());
        assertEquals(CompositeRaisedQueryException.class, exception.getCause().getClass());
        assertEquals(DatawaveErrorCode.MODEL_FETCH_ERROR.getErrorCode(), ((CompositeRaisedQueryException) exception.getCause()).getErrorCode());
    }

    @Test
    public void testMultipleExceptionWithNestedQueryExceptionSeenFirst() {
        QueryException nestedCause = new QueryException(DatawaveErrorCode.MODEL_FETCH_ERROR, "connection failed");
        IllegalStateException topCause = new IllegalStateException("bad state", nestedCause);
        Map<String,Exception> exceptions = new LinkedHashMap<>();
        exceptions.put("logic1", topCause);
        exceptions.put("logic2", new IllegalArgumentException("illegal name"));
        exceptions.put("logic3", new NullPointerException("null value"));

        CompositeLogicException exception = new CompositeLogicException("failed to complete", exceptions);
        assertEquals("failed to complete:\nlogic1: bad state\nlogic2: illegal name\nlogic3: null value", exception.getMessage());
        assertEquals(CompositeRaisedQueryException.class, exception.getCause().getClass());
        assertEquals(DatawaveErrorCode.MODEL_FETCH_ERROR.getErrorCode(), ((CompositeRaisedQueryException) exception.getCause()).getErrorCode());
    }
}
