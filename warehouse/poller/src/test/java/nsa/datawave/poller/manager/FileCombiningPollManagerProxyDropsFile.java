package nsa.datawave.poller.manager;

import java.io.File;
import java.io.IOException;

public class FileCombiningPollManagerProxyDropsFile extends FileCombiningPollManagerProxy {
    @Override
    public File moveToWorkFile(File queuedFile) throws IOException {
        return super.moveToWorkFile(new File("someFileThatDoesntExist.gz"));
    }
}
