package datawave.util.flag;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;

public class FlagFileContentCreator {
    private static final char NEWLINE = '\n';
    private static final char SPACE = ' ';
    private static final char COMMA = ',';

    // See execute-ingest.sh. It prepares a command from the flag file,
    // replacing
    // the ${JOB_FILE} variable with the *-ingest-server.sh provided flag file
    // name (after stripping .inprogress)
    private static final String PLACEHOLDER_VARIABLE = "${JOB_FILE}";

    private final FlagMakerConfig flagMakerConfig;
    private FlagMetrics metrics;

    public FlagFileContentCreator(FlagMakerConfig flagMakerConfig) {
        this.flagMakerConfig = flagMakerConfig;
    }

    void writeFlagFileContents(FileOutputStream flagOutputStream, Collection<InputFile> inputFiles, FlagDataTypeConfig fc) throws IOException {
        String content = createContent(inputFiles, fc);
        flagOutputStream.write(content.getBytes());

        writeFileNamesToMetrics(inputFiles);
    }

    int calculateSize(Collection<InputFile> inputFiles, FlagDataTypeConfig fc) {
        return createContent(inputFiles, fc).length();
    }

    private String createContent(Collection<InputFile> inputFiles, FlagDataTypeConfig fc) {
        StringBuilder sb = new StringBuilder(flagMakerConfig.getDatawaveHome() + File.separator + fc.getScript());

        if (fc.getFileListMarker() == null) {
            char sep = SPACE;
            for (InputFile inFile : inputFiles) {
                sb.append(sep).append(inFile.getFlagged().toUri());
                sep = COMMA;
            }
        } else {
            sb.append(" ");
            // add a placeholder variable which will later resolve to the flag
            // file .inprogress. The baseName could change by then.
            sb.append(PLACEHOLDER_VARIABLE);
        }

        sb.append(SPACE).append(fc.getReducers()).append(" -inputFormat ").append(fc.getInputFormat().getName()).append(SPACE);

        if (fc.getFileListMarker() != null) {
            sb.append("-inputFileLists -inputFileListMarker ").append(fc.getFileListMarker()).append(SPACE);
        }
        if (fc.getExtraIngestArgs() != null) {
            sb.append(fc.getExtraIngestArgs());
        }
        sb.append(NEWLINE);
        if (fc.getFileListMarker() != null) {
            sb.append(fc.getFileListMarker()).append(NEWLINE);
            for (InputFile inFile : inputFiles) {
                sb.append(inFile.getFlagged().toUri()).append(NEWLINE);
            }
        }
        return sb.toString();
    }

    public void withMetrics(FlagMetrics metrics) {
        this.metrics = metrics;
    }

    private void writeFileNamesToMetrics(Collection<InputFile> inputFiles) {
        if (metrics != null) {
            boolean first = true;
            for (InputFile inFile : inputFiles) {
                // todo - add a test where this fails, then consider refactoring
                // such that this class doesn't know about metrics
                if (first) {
                    first = false;
                } else {
                    metrics.updateCounter(InputFile.class.getSimpleName(), inFile.getFileName(), inFile.getTimestamp());
                }
            }
        }
    }
}
