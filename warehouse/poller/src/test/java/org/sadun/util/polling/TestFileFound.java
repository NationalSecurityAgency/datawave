package org.sadun.util.polling;

import java.io.File;

/** */
public class TestFileFound extends FileFoundEvent {
    public static class TestDirectoryPoller extends DirectoryPoller {
        public boolean wasShutdown;
        
        @Override
        public void shutdown() {
            wasShutdown = true;
        }
        
        public void reset() {
            wasShutdown = false;
        }
    }
    
    private final File _file;
    
    public TestFileFound(TestDirectoryPoller poller, File file) {
        super(poller, file);
        _file = file;
    }
    
    @Override
    public File getDirectory() {
        return this._file.getParentFile();
    }
    
    @Override
    public File getFile() {
        return this._file;
    }
}
