package datawave.microservice.fileProvider.downloaders;

public class HttpsDownloader implements Downloader {

    public static String HTTPS_CODE = "https_code";

    private Status status = Status.PENDING;

    // some sort of token that can be started / stopped like in C#

    // Need properties for:
    // - url to file
    // - file destination path
    // - file name

    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public void startDownload() { // ???This will run async??? Right??? And then abort and stuff can be called on this Downloader instance while this funciton is running to check what's going on.
        // download via https url

        //set status to IN_PROGRESS

        //attempt connection
            //not connected = set status to ERROR
            //connected = continue

        //attempt download
            //downloading in progress = wait to finish download
            //downloading error = set status to ERROR
                //depending on the error, handle stuff differently
                //remove the temp file

        //download aborted
            //set status to ABORTED

        //finished download
            //status = COMPLETE
    }

    @Override
    public void abort() {

    }

    @Override
    public boolean isDone() {
        return status == Status.COMPLETE || status == Status.ABORTED;
    }
}
