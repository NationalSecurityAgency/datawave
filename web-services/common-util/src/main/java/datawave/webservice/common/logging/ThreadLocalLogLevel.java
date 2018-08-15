package datawave.webservice.common.logging;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;

public class ThreadLocalLogLevel {
    
    static private ThreadLocal<Map<String,Level>> logToLevelMap = ThreadLocal.withInitial(HashMap::new);
    
    static public void setLevel(String name, Level level) {
        
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.put(name, level);
        }
    }
    
    static public Level getLevel(String name) {
        
        Level level = null;
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            level = levelMap.get(name);
        }
        return level;
    }
    
    static public void clear() {
        
        Map<String,Level> levelMap = logToLevelMap.get();
        if (levelMap != null) {
            levelMap.clear();
        }
    }
}
