package nsa.datawave.poller.manager;

import com.google.common.collect.Maps;
import org.sadun.util.polling.FileFoundEvent;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileCombiningPollManagerProxy extends FileCombiningPollManager {
    private static final ReentrantReadWriteLock workFilesLock = new ReentrantReadWriteLock();
    private static final HashMap<File,File> workFiles = new HashMap<>();
    private static final AtomicInteger filesProcessed = new AtomicInteger();
    private static final AtomicInteger filesCompleted = new AtomicInteger();
    
    public FileCombiningPollManagerProxy() {}
    
    public static void resetProxy() {
        FileCombiningPollManagerProxy.resetFilesCompleted();
        FileCombiningPollManagerProxy.resetFilesProcessed();
        FileCombiningPollManagerProxy.resetWorkFiles();
    }
    
    public static int resetFilesProcessed() {
        return filesProcessed.getAndSet(0);
    }
    
    public static int getFilesProcessed() {
        return filesProcessed.get();
    }
    
    public static int resetFilesCompleted() {
        return filesCompleted.getAndSet(0);
    }
    
    public static int getFilesCompleted() {
        return filesCompleted.get();
    }
    
    public static Map<File,File> resetWorkFiles() {
        final Map<File,File> retVal;
        
        workFilesLock.writeLock().lock();
        try {
            retVal = Maps.newHashMap(workFiles);
            workFiles.clear();
        } finally {
            workFilesLock.writeLock().unlock();
        }
        
        return retVal;
    }
    
    public static Map<File,File> getWorkFiles() {
        final Map<File,File> retVal;
        
        workFilesLock.readLock().lock();
        try {
            retVal = Maps.newHashMap(workFiles);
        } finally {
            workFilesLock.readLock().unlock();
        }
        
        return retVal;
    }
    
    public static int getNumWorkFiles() {
        final int retVal;
        
        workFilesLock.readLock().lock();
        try {
            retVal = workFiles.size();
        } finally {
            workFilesLock.readLock().unlock();
        }
        
        return retVal;
    }
    
    @Override
    public void fileFound(FileFoundEvent event) {
        super.fileFound(event);
        filesProcessed.getAndIncrement();
    }
    
    @Override
    public File moveToWorkFile(File queuedFile) throws IOException {
        final File workFile = super.moveToWorkFile(queuedFile);
        
        if (workFile != null) {
            workFilesLock.writeLock().lock();
            try {
                workFiles.put(workFile, queuedFile);
            } finally {
                workFilesLock.writeLock().unlock();
            }
        }
        return workFile;
    }
    
    @Override
    protected void finishCurrentFile(final boolean closing) throws IOException {
        if (out != null && counting.getByteCount() > 0)
            filesCompleted.getAndIncrement();
        
        super.finishCurrentFile(closing);
    }
}
