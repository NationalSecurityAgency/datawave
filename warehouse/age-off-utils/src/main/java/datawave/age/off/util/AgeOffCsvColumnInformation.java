package datawave.age.off.util;

import java.text.MessageFormat;
import java.util.Arrays;

public class AgeOffCsvColumnInformation {

    int patternColumnNumber = -1;
    int durationColumnNumber = -1;
    int labelColumnNumber = -1;
    int overrideColumnNumber = -1;

    // required
    private static final String PATTERN_COLUMN_HEADER = "pattern";
    // required
    private static final String DURATION_COLUMN_HEADER = "duration";
    // optional
    private static final String LABEL_COLUMN_NUMBER = "label";
    // optional - conditionally override duration
    private static final String DURATION_OVERRIDE_COLUMN_HEADER = "override";

    public void parseHeader(String[] headerTokens) {
        int columnNumber = 0;
        for (String headerToken : headerTokens) {
            switch (headerToken.trim().toLowerCase()) {
                case DURATION_COLUMN_HEADER:
                    this.durationColumnNumber = columnNumber;
                    break;
                case LABEL_COLUMN_NUMBER:
                    this.labelColumnNumber = columnNumber;
                    break;
                case PATTERN_COLUMN_HEADER:
                    this.patternColumnNumber = columnNumber;
                    break;
                case DURATION_OVERRIDE_COLUMN_HEADER:
                    this.overrideColumnNumber = columnNumber;
                    break;
            }
            columnNumber++;
        }
        if (this.durationColumnNumber == -1 || this.patternColumnNumber == -1) {
            throw new IllegalStateException(MessageFormat.format("Unable to find {0} or {1} in {2}", DURATION_COLUMN_HEADER, PATTERN_COLUMN_HEADER,
                            Arrays.toString(headerTokens)));
        }
    }
}
