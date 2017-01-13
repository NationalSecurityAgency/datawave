package nsa.datawave.metrics.web.stats;

import java.util.HashMap;
import java.util.Map;

import nsa.datawave.metrics.web.CloudContext;
import org.apache.log4j.Logger;

import com.google.gson.JsonElement;

/**
 * Class to cache statistics.
 */
public abstract class CachedStatistic extends Statistic implements Runnable {
    
    private static final Logger log = Logger.getLogger(CachedStatistic.class);
    
    protected static final Map<String,CachedElement> objectMap = new HashMap<>();
    
    protected static final Map<Class<? extends CachedStatistic>,Thread> runners = new HashMap<>();
    
    public CachedStatistic(CloudContext ctx) {
        super(ctx);
    }
    
    public CachedElement getCachedElement(String subName) {
        
        if (log.isDebugEnabled())
            log.debug("Getting class for " + getClass() + subName);
        
        return objectMap.get(getClass() + subName);
        
    }
    
    protected void cacheElement(JsonElement element, String subName) {
        objectMap.put(this.getClass() + subName, new CachedElement(System.currentTimeMillis(), element));
    }
    
    public CachedElement getCachedElement() {
        
        return getCachedElement("");
        
    }
    
    protected void cacheElement(JsonElement element) {
        this.cacheElement(element, "");
    }
    
    public static class CachedElement {
        protected long timestamp;
        protected JsonElement element;
        
        protected CachedElement(long ts, JsonElement element) {
            timestamp = ts;
            this.element = element;
        }
        
        public long getTimeStamp() {
            return timestamp;
        }
        
        public JsonElement toJson() {
            return element;
        }
        
    }
    
    @Override
    public void run() {
        JsonElement element = null;
        Thread t = runners.get(this.getClass());
        if (null != t) {
            if (t != Thread.currentThread()) {
                while (t.isAlive()) {
                    // sleep a second
                    try {
                        Thread.sleep(1000);
                        
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    
                }
            }
            runners.remove(this.getClass());
        }
        
        element = toJson(null);
        
        cacheElement(element);
    }
    
}
