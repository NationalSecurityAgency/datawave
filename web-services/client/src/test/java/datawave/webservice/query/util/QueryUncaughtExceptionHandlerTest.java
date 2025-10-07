package datawave.webservice.query.util;

import org.junit.Assert;
import org.junit.Test;

public class QueryUncaughtExceptionHandlerTest {

    private QueryUncaughtExceptionHandler queh;
    private String tName = "THREADNAME_FOR_TESTS";
    private String throwMsg = "THROWS_FOR_TESTS";

    @Test
    public void testEntireClass() {
        Thread t = new Thread();
        t.setName(tName);
        Throwable throwable = new Throwable(throwMsg);

        queh = new QueryUncaughtExceptionHandler();
        queh.uncaughtException(t, throwable);

        Assert.assertEquals(tName, queh.getThread().getName());
        Assert.assertEquals(throwMsg, queh.getThrowable().getMessage());

    }
}
