package nsa.datawave.poller.manager;

import nsa.datawave.poller.metric.FlowStatistics;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.sadun.util.polling.CycleEndEvent;
import org.sadun.util.polling.CycleStartEvent;
import org.sadun.util.polling.DirectoryLookupEndEvent;
import org.sadun.util.polling.DirectoryLookupStartEvent;
import org.sadun.util.polling.FileFoundEvent;
import org.sadun.util.polling.FileMovedEvent;
import org.sadun.util.polling.FileSetFoundEvent;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This manager will manage a set of managers and will process the fileFound calls concurrently in a thread pool allowing us to process multiple input files at
 * once. Requirements for the delegate configured poll manager: 1) Avoid static members as multiple instances will exist in the same JVM 2) Ensure that any
 * output file name is unique 3) Assume that any method can be called concurrently with fileFound except for fileFound itself
 */
public class MultiThreadedConfiguredPollManager extends ConfiguredPollManager implements RecoverablePollManager {
    
    private static final Logger log = Logger.getLogger(MultiThreadedConfiguredPollManager.class);
    protected ThreadPoolExecutor executor = null;
    // fatalError can be set my multiple threads and read from another....making this volatile to avoid caching issues
    volatile protected Error fatalError = null;
    protected BlockingQueue<Runnable> workQueue = null;
    protected int threads = 1;
    protected int queueSize = 1;
    protected ConcurrentConfiguredPollManager[] managers = null;
    protected FlowStatistics inputFileStats = new FlowStatistics("fileMoved");
    protected FlowStatistics processFileStats = new FlowStatistics("fileFound");
    
    @Override
    public Options getConfigurationOptions() {
        Options opts = null;
        // need to call on each manager even though results of only the last one are returned
        for (int i = 0; i < threads; i++) {
            opts = managers[i].getConfigurationOptions();
        }
        return opts;
    }
    
    @Override
    public void configure(CommandLine cl) throws Exception {
        super.configure(cl);
        inputFileStats.showStats(5L * 60L * 1000L);
        processFileStats.showStats(5L * 60L * 1000L);
        
        workQueue = new ArrayBlockingQueue<>(queueSize);
        executor = new ThreadPoolExecutor(threads, threads, 500, TimeUnit.MILLISECONDS, workQueue);
        executor.prestartAllCoreThreads();
        for (int i = 0; i < threads; i++) {
            managers[i].configure(cl);
        }
    }
    
    @Override
    public void recover(File queueDir) {
        failOnError();
        for (int i = 0; i < threads; i++) {
            managers[i].recover(queueDir);
        }
    }
    
    @Override
    public void cycleEnded(CycleEndEvent arg0) {
        failOnError();
        for (int i = 0; i < threads; i++) {
            managers[i].cycleEnded(arg0);
        }
    }
    
    @Override
    public void cycleStarted(CycleStartEvent arg0) {
        failOnError();
        for (int i = 0; i < threads; i++) {
            managers[i].cycleStarted(arg0);
        }
    }
    
    @Override
    public void directoryLookupEnded(DirectoryLookupEndEvent arg0) {
        failOnError();
        for (int i = 0; i < threads; i++) {
            managers[i].directoryLookupEnded(arg0);
        }
    }
    
    @Override
    public void directoryLookupStarted(DirectoryLookupStartEvent arg0) {
        failOnError();
        for (int i = 0; i < threads; i++) {
            managers[i].directoryLookupStarted(arg0);
        }
    }
    
    @Override
    public void exceptionDeletingTargetFile(File arg0) {
        failOnError();
        for (int i = 0; i < threads; i++) {
            managers[i].exceptionDeletingTargetFile(arg0);
        }
    }
    
    @Override
    public void exceptionMovingFile(File arg0, File arg1) {
        failOnError();
        for (int i = 0; i < threads; i++) {
            managers[i].exceptionMovingFile(arg0, arg1);
        }
    }
    
    @Override
    public void fileFound(FileFoundEvent arg0) {
        failOnError();
        boolean processed = false;
        while (!processed) {
            for (int i = 0; i < threads && !processed; i++) {
                if (managers[i].lock()) {
                    log.info("Delegating " + arg0.getFile() + " to manager #" + (i + 1));
                    managers[i].fileFound(arg0);
                    if (!workQueue.offer(managers[i])) {
                        managers[i].unlock();
                        throw new RuntimeException("Failed to process file found event for " + arg0.getFile());
                    }
                    processed = true;
                }
            }
            if (!processed) {
                try {
                    Thread.sleep(100);
                } catch (Exception ie) {
                    throw new RuntimeException("FileFound was interrupted trying to find a manager for " + arg0.getFile());
                }
            }
        }
        failOnError();
    }
    
    @Override
    public void fileMoved(FileMovedEvent arg0) {
        failOnError();
        for (int i = 0; i < threads; i++) {
            managers[i].fileMoved(arg0);
        }
        inputFileStats.incrementStats(1);
    }
    
    @Override
    public void fileSetFound(FileSetFoundEvent arg0) {
        failOnError();
        for (int i = 0; i < threads; i++) {
            managers[i].fileSetFound(arg0);
        }
    }
    
    @Override
    public String getDatatype() {
        return managers[0].getDatatype();
    }
    
    public void failOnError() {
        if (fatalError != null) {
            throw new FileProcessingError(fatalError);
        }
    }
    
    public int getActiveManagers() {
        return executor.getActiveCount();
    }
    
    public long getCompletedTasks() {
        return executor.getCompletedTaskCount();
    }
    
    @Override
    public void close() throws IOException {
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.warn("Continuing close() even though threads are still active.");
        }
        for (int i = 0; i < threads; i++) {
            try {
                managers[i].close();
            } catch (Exception e2) {
                log.warn("Error caught while trying to close " + managers[i], e2);
            }
        }
        inputFileStats.shutdownStats();
        processFileStats.shutdownStats();
        writeProvenanceReports();
        
        super.close();
        NDC.pop();
    }
    
    public MultiThreadedConfiguredPollManager(int threads, String delegateClassName) throws ClassNotFoundException, InstantiationException,
                    IllegalAccessException {
        this.threads = threads;
        this.queueSize = threads;
        managers = new ConcurrentConfiguredPollManager[threads];
        for (int i = 0; i < threads; i++) {
            Class<?> c = Class.forName(delegateClassName);
            Object o = c.newInstance();
            if (o instanceof ConfiguredPollManager) {
                final ConfiguredPollManager manager = (ConfiguredPollManager) o;
                manager.setThreadIndex(i);
                managers[i] = new ConcurrentConfiguredPollManager(manager);
            } else {
                throw new InstantiationException("Class " + c.getName() + " is not an instance of ConfiguredPollManager");
            }
        }
    }
    
    protected class ConcurrentConfiguredPollManager extends ConfiguredPollManager implements RecoverablePollManager, Runnable {
        protected ConfiguredPollManager delegate;
        protected AtomicBoolean working = new AtomicBoolean();
        protected FileFoundEvent event;
        
        public ConcurrentConfiguredPollManager(ConfiguredPollManager delegate) {
            this.delegate = delegate;
            if (null != delegate) {
                this.delegate.setThreadIndex(delegate.getThreadIndex());
            }
        }
        
        public boolean locked() {
            return working.get();
        }
        
        public boolean lock() {
            return working.compareAndSet(false, true);
        }
        
        public void unlock() {
            working.set(false);
        }
        
        public void configure(CommandLine cl) throws Exception {
            delegate.configure(cl);
        }
        
        public void recover(File queueDir) {
            if (delegate instanceof RecoverablePollManager) {
                log.info("Calling recoverable poll manager " + delegate.getClass());
                ((RecoverablePollManager) delegate).recover(queueDir);
            }
        }
        
        public void close() throws IOException {
            delegate.close();
        }
        
        public void cycleEnded(CycleEndEvent arg0) {
            delegate.cycleEnded(arg0);
        }
        
        public void cycleStarted(CycleStartEvent arg0) {
            delegate.cycleStarted(arg0);
        }
        
        public void directoryLookupEnded(DirectoryLookupEndEvent arg0) {
            delegate.directoryLookupEnded(arg0);
        }
        
        public void directoryLookupStarted(DirectoryLookupStartEvent arg0) {
            delegate.directoryLookupStarted(arg0);
        }
        
        public void exceptionDeletingTargetFile(File arg0) {
            delegate.exceptionDeletingTargetFile(arg0);
        }
        
        public void exceptionMovingFile(File arg0, File arg1) {
            delegate.exceptionMovingFile(arg0, arg1);
        }
        
        public void fileFound(FileFoundEvent arg0) {
            if (locked()) {
                this.event = arg0;
            }
        }
        
        @Override
        public void run() {
            if (locked()) {
                try {
                    delegate.fileFound(event);
                    processFileStats.incrementStats(1);
                } catch (Exception e) {
                    log.error("Unable to process file " + event.getFile(), e);
                } catch (Error e) {
                    log.error("Fatal error while processing file " + event.getFile(), e);
                    fatalError = e;
                    throw new FileProcessingError(e);
                } finally {
                    unlock();
                }
            }
        }
        
        public void fileMoved(FileMovedEvent arg0) {
            delegate.fileMoved(arg0);
        }
        
        public void fileSetFound(FileSetFoundEvent arg0) {
            delegate.fileSetFound(arg0);
        }
        
        public String getDatatype() {
            return delegate.getDatatype();
        }
        
        public Options getConfigurationOptions() {
            return delegate.getConfigurationOptions();
        }
    }
    
}
