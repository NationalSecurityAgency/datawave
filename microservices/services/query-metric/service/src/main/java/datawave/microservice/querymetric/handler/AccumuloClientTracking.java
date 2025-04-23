package datawave.microservice.querymetric.handler;

import java.util.HashMap;
import java.util.Map;

public class AccumuloClientTracking {
    
    public static Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
        HashMap<String,String> trackingMap = new HashMap<>();
        if (stackTrace != null) {
            StackTraceElement ste = stackTrace[1];
            trackingMap.put("request.location", ste.getClassName() + "." + ste.getMethodName() + ":" + ste.getLineNumber());
        }
        return trackingMap;
    }
}
