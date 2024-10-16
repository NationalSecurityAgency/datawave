package datawave.query.tracking;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class ActiveQueryLog {

    public static final String DEFAULT_NAME = "default_log";
    private static final String DEFAULT_EMPTY_QUERY_ID = new UUID(0, 0).toString();
    private static final Logger log = Logger.getLogger(ActiveQueryLog.class);
    private static Cache<String,ActiveQueryLog> logCache = null;
    private static final Object logCacheLock = new Object();
    private static PluginEnvironment.Configuration conf = null;

    // Accumulo properties
    public static final String MAX_IDLE = "datawave.query.active.maxIdleMs";
    public static final String LOG_PERIOD = "datawave.query.active.logPeriodMs";
    public static final String LOG_MAX_QUERIES = "datawave.query.active.logMaxQueries";
    public static final String WINDOW_SIZE = "datawave.query.active.windowSize";
    private static final long HOURS_24_MS = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
    private static final long MINUTES_15_MS = TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);
    private static final long MINUTES_1_MS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

    // Changeable via Accumulo properties
    volatile private long maxIdle = MINUTES_15_MS;
    volatile private long logPeriod = MINUTES_1_MS;
    volatile private int logMaxQueries = 5;
    volatile private int windowSize = 10;
    private final AtomicLong lastAccess = new AtomicLong(System.currentTimeMillis());

    private Cache<String,ActiveQuery> CACHE = null;
    private final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
    private Timer timer = null;
    private TimerTask timerTask = null;

    private final String name;

    synchronized public static void setConfig(final PluginEnvironment.Configuration conf) {
        if (conf != null) {
            if (ActiveQueryLog.conf == null) {
                ActiveQueryLog.conf = conf;
            }
            // Do not allow access to the cache while updating each log's settings.
            if (logCache != null) {
                synchronized (ActiveQueryLog.logCacheLock) {
                    ConcurrentMap<String,ActiveQueryLog> logMap = logCache.asMap();
                    logMap.values().forEach(log -> log.checkSettings(conf, false));
                }
            }
        }
    }

    /**
     * Return the default {@link ActiveQueryLog} instance. The time the instance was last accessed will be updated to the current time in milliseconds, and if
     * the instance's timer was cancelled, it will be restarted.
     *
     * @return the default {@link ActiveQueryLog} instance
     */
    public static ActiveQueryLog getInstance() {
        return getInstance(DEFAULT_NAME);
    }

    /**
     * Return the {@link ActiveQueryLog} instance associated with the specified name. If one does not exist, it will be created.
     * <p>
     * If the specified name is null or blank, the default instance with the name '{@value #DEFAULT_NAME}' will be returned. Additionally, the time the log was
     * last accessed will be updated to the current time in milliseconds, and if the log's timer was cancelled, it will be restarted.
     *
     * @param name
     *            the associated name by which to look up the desired {@link ActiveQueryLog}. This will typically be the name of a table or query logic.
     * @return the existing or new {@link ActiveQueryLog} for the name
     */
    public static ActiveQueryLog getInstance(String name) {
        // Return the default instance if the name is blank.
        if (StringUtils.isBlank(name)) {
            name = DEFAULT_NAME;
        }

        // Initialize the log cache if necessary.
        if (ActiveQueryLog.logCache == null) {
            synchronized (ActiveQueryLog.logCacheLock) {
                if (ActiveQueryLog.logCache == null) {
                    ActiveQueryLog.logCache = Caffeine.newBuilder().build();
                }
            }
        }

        // If no log currently exists for the name, create one.
        ActiveQueryLog log;
        synchronized (ActiveQueryLog.logCacheLock) {
            log = ActiveQueryLog.logCache.getIfPresent(name);
            if (log == null) {
                log = new ActiveQueryLog(conf, name);
                ActiveQueryLog.logCache.put(name, log);
            }
        }

        updateLastAccessedAndRestartCancelledTimer(log);

        return log;
    }

    private static void updateLastAccessedAndRestartCancelledTimer(ActiveQueryLog log) {
        log.touch();
        if (log.timer == null) {
            log.setLogPeriod(log.logPeriod);
        }
    }

    private ActiveQueryLog() {
        this(null, null);
    }

    private ActiveQueryLog(PluginEnvironment.Configuration conf, String name) {
        if (conf != null) {
            checkSettings(conf, true);
        } else {
            // use the default values
            setLogPeriod(this.logPeriod);
            setMaxIdle(this.maxIdle);
        }
        this.name = name;
    }

    private void touch() {
        this.lastAccess.set(System.currentTimeMillis());
    }

    long getLastAccess() {
        return this.lastAccess.get();
    }

    synchronized private void cancelTimer() {
        this.timer.cancel();
        this.timer = null;
    }

    synchronized public void setLogPeriod(long logPeriod) {
        if (logPeriod > 0) {
            if (logPeriod != this.logPeriod || this.timer == null) {
                // this.timer will be null on initial call within a classloader
                // this.timer will be null if it was shut down due to inactivity
                // create timer and schedule a periodic check for inactivity
                if (this.timer == null) {
                    this.timer = new Timer("ActiveQueryLog");
                    this.timer.schedule(new TimerTask() {
                        @Override
                        public void run() {
                            long elapsed = System.currentTimeMillis() - ActiveQueryLog.this.getLastAccess();
                            // if no queries have been logged in a
                            // long time then inactivate the timer
                            if (elapsed > HOURS_24_MS) {
                                ActiveQueryLog.this.cancelTimer();
                            }
                        }
                    }, MINUTES_15_MS, MINUTES_15_MS);
                }
                if (this.timerTask != null) {
                    this.timerTask.cancel();
                }
                this.timerTask = new ActiveQueryTimerTask();
                this.logPeriod = logPeriod;
                this.timer.schedule(this.timerTask, this.logPeriod, this.logPeriod);
            }
        } else {
            log.error("Bad value: (" + logPeriod + ") for logPeriod");
        }
    }

    synchronized public void setLogMaxQueries(int logMaxQueries) {
        this.logMaxQueries = logMaxQueries;
    }

    synchronized public void setWindowSize(int windowSize) {
        if (windowSize > 0) {
            this.windowSize = windowSize;
        } else {
            log.error("Bad value: (" + windowSize + ") for windowSize");
        }
    }

    public void setMaxIdle(long maxIdle) {
        if (this.maxIdle != maxIdle || this.CACHE == null) {
            if (maxIdle > 0) {
                cacheLock.writeLock().lock();
                try {
                    Cache<String,ActiveQuery> newCache = setupCache(maxIdle);
                    if (this.CACHE == null) {
                        this.CACHE = newCache;
                    } else {
                        Cache<String,ActiveQuery> oldCache = this.CACHE;
                        this.CACHE = newCache;
                        this.CACHE.putAll(oldCache.asMap());
                    }
                } finally {
                    cacheLock.writeLock().unlock();
                }
                this.maxIdle = maxIdle;
            } else {
                log.error("Bad value: (" + maxIdle + ") for maxIdle");
            }
        }
    }

    private void checkSettings(PluginEnvironment.Configuration conf, boolean useDefaults) {

        String maxIdleStr = conf.get(MAX_IDLE);
        if (maxIdleStr != null) {
            try {
                setMaxIdle(Long.parseLong(maxIdleStr));
            } catch (NumberFormatException e) {
                log.error("Bad value: (" + maxIdleStr + ") in " + MAX_IDLE + " : " + e.getMessage());
            }
        } else if (useDefaults) {
            setMaxIdle(this.maxIdle);
        }

        String logPeriodStr = conf.get(LOG_PERIOD);
        if (logPeriodStr != null) {
            try {
                setLogPeriod(Long.parseLong(logPeriodStr));
            } catch (NumberFormatException e) {
                log.error("Bad value: (" + logPeriodStr + ") in " + LOG_PERIOD + " : " + e.getMessage());
            }
        } else if (useDefaults) {
            setLogPeriod(this.logPeriod);
        }

        String logMaxQueriesStr = conf.get(LOG_MAX_QUERIES);
        if (logMaxQueriesStr != null) {
            try {
                setLogMaxQueries(Integer.parseInt(logMaxQueriesStr));
            } catch (NumberFormatException e) {
                log.error("Bad value: (" + logMaxQueriesStr + ") in " + LOG_MAX_QUERIES + " : " + e.getMessage());
            }
        }

        String windowSizeStr = conf.get(WINDOW_SIZE);
        if (windowSizeStr != null) {
            try {
                setWindowSize(Integer.parseInt(windowSizeStr));
            } catch (NumberFormatException e) {
                log.error("Bad value: (" + windowSizeStr + ") in " + WINDOW_SIZE + " : " + e.getMessage());
            }
        }
    }

    private Cache<String,ActiveQuery> setupCache(long maxIdle) {
        Caffeine<Object,Object> caffeine = Caffeine.newBuilder();
        caffeine.expireAfterAccess(maxIdle, TimeUnit.MILLISECONDS);
        return caffeine.build();
    }

    public void remove(String queryId, Range range) {
        ActiveQuery activeQuery = get(queryId);
        int numActiveRanges = activeQuery.removeRange(range);
        if (numActiveRanges == 0) {
            cacheLock.readLock().lock();
            try {
                this.CACHE.invalidate(queryIdFor(queryId));
            } finally {
                cacheLock.readLock().unlock();
            }
        }
    }

    private String queryIdFor(String queryId) {
        return (queryId == null ? DEFAULT_EMPTY_QUERY_ID : queryId);
    }

    public ActiveQuery get(String queryId) {
        ActiveQuery activeQuery;
        cacheLock.readLock().lock();
        try {
            activeQuery = this.CACHE.get(queryIdFor(queryId), s -> new ActiveQuery(queryIdFor(queryId), this.windowSize, name));
        } finally {
            cacheLock.readLock().unlock();
        }
        return activeQuery;
    }

    class ActiveQueryTimerTask extends TimerTask {

        public ActiveQueryTimerTask() {}

        @Override
        public void run() {
            List<ActiveQuerySnapshot> activeQueryList = new ArrayList<>();
            cacheLock.readLock().lock();
            try {
                for (ActiveQuery q : ActiveQueryLog.this.CACHE.asMap().values()) {
                    activeQueryList.add(q.snapshot());
                }
            } finally {
                cacheLock.readLock().unlock();
            }

            activeQueryList.sort(ActiveQuerySnapshot.greatestElapsedTime);

            List<ActiveQuerySnapshot> sublist = activeQueryList;
            if (ActiveQueryLog.this.logMaxQueries > 0) {
                sublist = activeQueryList.subList(0, Math.min(ActiveQueryLog.this.logMaxQueries, activeQueryList.size()));
            }

            // ensure that the timer doesn't get cancelled if we have queries being logged
            if (sublist.size() > 0) {
                ActiveQueryLog.this.touch();
            }

            for (ActiveQuerySnapshot q : sublist) {
                log.debug(q.toString());
            }
        }
    }
}
