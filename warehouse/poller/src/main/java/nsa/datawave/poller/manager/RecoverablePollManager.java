package nsa.datawave.poller.manager;

import java.io.File;

import org.sadun.util.polling.PollManager;

/**
 * Poll Manager that can recover when restarted
 * 
 */
public interface RecoverablePollManager extends PollManager {
    /**
     * Called when the poller is started after it is configured but before polling starts. The queue directory passed in is the location where files are placed
     * initially. If this poll manager ever moved files out of the queue that may need to be reprocessed, then this is the change to put those files back into
     * the queue.
     * 
     * @param queueDir
     */
    public void recover(File queueDir);
}
