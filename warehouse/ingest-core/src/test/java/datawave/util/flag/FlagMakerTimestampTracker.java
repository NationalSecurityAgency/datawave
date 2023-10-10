package datawave.util.flag;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Captures min/max timestamps for file modification and folder creation as well as a full list of file modification times
 */
public class FlagMakerTimestampTracker {

    final Collection<Long> fileLastModifiedTimes = new ArrayList<>();

    long minLastModified = Long.MAX_VALUE;
    long maxLastModified = Long.MIN_VALUE;
    long minFolderTime = Long.MAX_VALUE;
    long maxFolderTime = Long.MIN_VALUE;

    public void reportFileLastModified(long lastModified) {
        fileLastModifiedTimes.add(lastModified);
        minLastModified = Math.min(lastModified, minLastModified);
        maxLastModified = Math.max(lastModified, maxLastModified);
    }

    public void reportDateForFolder(long folderTimeInMillis) {
        this.minFolderTime = Math.min(folderTimeInMillis, minFolderTime);
        this.maxFolderTime = Math.max(folderTimeInMillis, maxFolderTime);
    }
}
