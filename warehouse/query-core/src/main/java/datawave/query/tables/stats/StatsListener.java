package datawave.query.tables.stats;

import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;

import datawave.query.tables.stats.ScanSessionStats.TIMERS;

public class StatsListener extends Service.Listener {

    protected ScanSessionStats stats;
    private ExecutorService statsListener;

    public StatsListener(ScanSessionStats stats, ExecutorService statsListener) {

        this.stats = stats;
        this.statsListener = statsListener;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.Service.Listener#starting()
     */
    @Override
    public void starting() {
        if (null != stats) {
            stats.getTimer(TIMERS.RUNTIME).start();
        }
        /**
         * Nothing to do here
         */

    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.Service.Listener#running()
     */
    @Override
    public void running() {
        /**
         * Nothing to do here
         */

    }

    /*
     * (non-Javadoc) QueryIterator
     *
     * @see com.google.common.util.concurrent.Service.Listener#stopping(com.google.common.util.concurrent.Service.State)
     */
    @Override
    public void stopping(State from) {
        stats.getTimer(TIMERS.RUNTIME).stop();

        switch (from) {
            case NEW:
            case RUNNING:
            case STARTING:
                statsListener.shutdownNow();
                break;
            default:
                break;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.Service.Listener#terminated(com.google.common.util.concurrent.Service.State)
     */
    @Override
    public void terminated(State from) {
        statsListener.shutdownNow();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.Service.Listener#failed(com.google.common.util.concurrent.Service.State, java.lang.Throwable)
     */
    @Override
    public void failed(State from, Throwable failure) {
        stats.stopOnFailure();
        statsListener.shutdownNow();
    }
}
