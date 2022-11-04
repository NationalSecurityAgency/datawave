package datawave.webservice.query.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        
        Assertions.assertEquals(tName, queh.getThread().getName());
        Assertions.assertEquals(throwMsg, queh.getThrowable().getMessage());
        
    }
}
