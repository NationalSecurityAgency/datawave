package datawave.core.iterators.querylock;

import java.util.List;

/**
 * Created on 2/8/17.
 */
public class CombinedQueryLock implements QueryLock {
    private QueryLock[] locks;

    public CombinedQueryLock(List<QueryLock> locks) {
        this.locks = locks.toArray(new QueryLock[locks.size()]);
    }

    @Override
    public void startQuery() throws Exception {
        for (QueryLock lock : locks) {
            lock.startQuery();
        }
    }

    @Override
    public boolean isQueryRunning() {
        // if any lock considers the query running, then consider it running
        for (QueryLock lock : locks) {
            if (lock.isQueryRunning()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void stopQuery() throws Exception {
        for (QueryLock lock : locks) {
            lock.stopQuery();
        }
    }

    @Override
    public void cleanup() throws Exception {
        for (QueryLock lock : locks) {
            lock.cleanup();
        }
    }
}
