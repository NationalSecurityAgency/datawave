package datawave.ingest.time;

import java.util.Date;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class that has close to 1 second precision and should alleviate waits on System.currentTimeMillis();
 *
 *
 *
 */
public class Now {

    /**
     * Task for the Now timer. Updates the AtomicLong every second. This should alleviate calls to System.currentTimeMillis() and give us close to a 1 second
     * precision.
     *
     *
     *
     */
    private class CurrentTimeTimerTask extends TimerTask {
        @Override
        public void run() {
            now.set(System.currentTimeMillis());
        }
    }

    private Timer timer = null;

    private AtomicLong now = new AtomicLong();

    private static Now instance;

    public static Now getInstance() {
        if (instance == null) {
            synchronized (Now.class) {
                if (instance == null) {
                    instance = new Now(false);
                }
            }
        }
        return instance;
    }

    public Now() {
        this(true);
    }

    private Now(boolean delegate) {
        if (delegate) {
            this.now = getInstance().now;
            this.timer = getInstance().timer;
        } else {
            now.set(System.currentTimeMillis());
            timer = new Timer("EventMapperIntervalTimer", true);
            timer.schedule(new CurrentTimeTimerTask(), new Date(), 1000L);
        }
    }

    public long get() {
        return now.get();
    }

    @Override
    public boolean equals(Object o2) {
        if (o2 instanceof Now) {
            return timer == ((Now) o2).timer && now == ((Now) o2).now;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timer, now);
    }
}
