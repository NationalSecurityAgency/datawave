package datawave.microservice.fileProvider.downloaders;

import java.io.IOException;

/**
 * Provide a simplistic interface for downloading a file. This can be implemented for various methods of fetching a file.
 */
public interface Downloader {



    // Relevant statuses for file downloads: PENDING, IN_PROGRESS, COMPLETE, ERROR
    
    // May want to add some methods definitions:
    // - abort(); < -- abort download, clean up any temporary files and/or the partial downloaded file
    // - getProgress() < -- Return some kind of DownloadStatus that has details about the status of the progress of the download
    // - isDone() < --- Return true if status == COMPLETE or ERROR
    // The above methods only apply if we decide to spawn the downloader as another task within the task so that we can loop and ping the
    // status of the file. Possibly not necessary.

    DownloadResult download();
}
