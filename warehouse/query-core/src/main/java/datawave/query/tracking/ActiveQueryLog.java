package datawave.query.tracking;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class ActiveQueryLog {
    
    private static Logger LOG = LoggerFactory.getLogger(ActiveQueryLog.class);
    private static ActiveQueryLog instance = null;
    private static AccumuloConfiguration conf = null;
    
    // Accumulo properties
    public static final String MAX_IDLE = "datawave.query.active.maxIdleMs";
    public static final String LOG_PERIOD = "datawave.query.active.logPeriodMs";
    public static final String LOG_MAX_QUERIES = "datawave.query.active.logMaxQueries";
    public static final String WINDOW_SIZE = "datawave.query.active.windowSize";
    
    // Changeable via Accumulo properties
    private long maxIdle = 900000l;
    private long logPeriod = 60000l;
    private int logMaxQueries = 5;
    private int windowSize = 10;
    
    private Cache<String,ActiveQuery> CACHE = null;
    private Timer timer = new Timer("ActiveQueryLog");
    
    synchronized public static void setConfig(AccumuloConfiguration conf) {
        if (conf != null) {
            if (ActiveQueryLog.conf == null || conf.getUpdateCount() > ActiveQueryLog.conf.getUpdateCount()) {
                ActiveQueryLog.getInstance().checkSettings(conf, false);
                ActiveQueryLog.conf = conf;
            }
        }
    }
    
    public static ActiveQueryLog getInstance() {
        
        if (ActiveQueryLog.instance == null) {
            synchronized (ActiveQueryLog.class) {
                if (ActiveQueryLog.instance == null) {
                    ActiveQueryLog.instance = new ActiveQueryLog(conf);
                }
            }
        }
        return ActiveQueryLog.instance;
    }
    
    private ActiveQueryLog(AccumuloConfiguration conf) {
        if (conf != null) {
            checkSettings(conf, true);
        } else {
            // use the default values
            setLogPeriod(this.logPeriod);
            setMaxIdle(this.maxIdle);
        }
    }
    
    public void setLogPeriod(long logPeriod) {
        if (logPeriod > 0) {
            if (logPeriod != this.logPeriod || this.timer == null) {
                if (this.timer != null) {
                    this.timer.cancel();
                    this.timer = new Timer("ActiveQueryLog");
                }
                this.logPeriod = logPeriod;
                this.timer.schedule(new ActiveQueryTimerTask(), this.logPeriod, this.logPeriod);
            }
        } else {
            LOG.error("Bad value: (" + logPeriod + ") for logPeriod");
        }
        
    }

    synchronized public void setLogMaxQueries(int logMaxQueries) {
        this.logMaxQueries = logMaxQueries;
    }

    synchronized public void setWindowSize(int windowSize) {
        if (windowSize > 0) {
            this.windowSize = windowSize;
        } else {
            LOG.error("Bad value: (" + windowSize + ") for windowSize");
        }
    }
    
    synchronized public void setMaxIdle(long maxIdle) {
        if (this.maxIdle != maxIdle || this.CACHE == null) {
            if (maxIdle > 0) {
                Cache<String,ActiveQuery> newCache = setupCache(maxIdle);
                if (this.CACHE == null) {
                    this.CACHE = newCache;
                } else {
                    Cache<String,ActiveQuery> oldCache = this.CACHE;
                    this.CACHE = newCache;
                    this.CACHE.putAll(oldCache.asMap());
                }
            } else {
                LOG.error("Bad value: (" + maxIdle + ") for maxIdle");
            }
        }
    }
    
    private void checkSettings(AccumuloConfiguration conf, boolean useDefaults) {
        
        String maxIdleStr = conf.get(MAX_IDLE);
        if (maxIdleStr != null) {
            try {
                setMaxIdle(Long.valueOf(maxIdleStr));
            } catch (NumberFormatException e) {
                LOG.error("Bad value: (" + maxIdleStr + ") in " + MAX_IDLE + " : " + e.getMessage());
            }
        } else if (useDefaults) {
            setMaxIdle(this.maxIdle);
        }
        
        String logPeriodStr = conf.get(LOG_PERIOD);
        if (logPeriodStr != null) {
            try {
                setLogPeriod(Long.valueOf(logPeriodStr));
            } catch (NumberFormatException e) {
                LOG.error("Bad value: (" + logPeriodStr + ") in " + LOG_PERIOD + " : " + e.getMessage());
            }
        } else if (useDefaults) {
            setLogPeriod(this.logPeriod);
        }

        String logMaxQueriesStr = conf.get(LOG_MAX_QUERIES);
        if (logMaxQueriesStr != null) {
            try {
                setLogMaxQueries(Integer.valueOf(logMaxQueriesStr));
            } catch (NumberFormatException e) {
                LOG.error("Bad value: (" + logMaxQueriesStr + ") in " + LOG_MAX_QUERIES + " : " + e.getMessage());
            }
        }

        String windowSizeStr = conf.get(WINDOW_SIZE);
        if (windowSizeStr != null) {
            try {
                setWindowSize(Integer.valueOf(windowSizeStr));
            } catch (NumberFormatException e) {
                LOG.error("Bad value: (" + windowSizeStr + ") in " + WINDOW_SIZE + " : " + e.getMessage());
            }
        }
    }
    
    private Cache<String,ActiveQuery> setupCache(long maxIdle) {
        Caffeine<Object,Object> caffeine = Caffeine.newBuilder();
        caffeine.expireAfterAccess(maxIdle, TimeUnit.MILLISECONDS);
        Cache<String,ActiveQuery> cache = caffeine.build();
        return cache;
    }
    
    synchronized public void remove(String queryId, Range range) {
        ActiveQuery activeQuery = get(queryId);
        activeQuery.finishRange(range);
        if (activeQuery.getNumActiveRanges() == 0) {
            this.CACHE.invalidate(queryId);
        }
    }
    
    synchronized public ActiveQuery get(String queryId) {
        return this.CACHE.get(queryId, s -> new ActiveQuery(queryId, this.windowSize));
    }
    
    class ActiveQueryTimerTask extends TimerTask {
        @Override
        public void run() {
            synchronized (this) {
                List<ActiveQuery> activeQueryList = new ArrayList<>();
                for (ActiveQuery q : CACHE.asMap().values()) {
                    activeQueryList.add(q);
                }
                activeQueryList.sort(activeQueryList, new TotalTimeComparator());

                for (ActiveQuery q : CACHE.asMap().values()) {
                    if (q.isInCall()) {
                        LOG.debug(q.toString());
                    }
                }

            }
        }
    };

    public class TotalTimeComparator implements Comparator<ActiveQuery> {
        @Override
        public int compare(ActiveQuery o1, ActiveQuery o2) {
            return Long.compare(o1.totalElapsedTime(), o2.totalElapsedTime());
        }
    }
}
