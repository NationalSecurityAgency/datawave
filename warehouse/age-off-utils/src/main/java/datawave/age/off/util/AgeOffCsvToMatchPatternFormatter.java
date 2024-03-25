package datawave.age.off.util;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reformats csv input into an age off match pattern. Expects a header to appear as the first line that's not a comment or whitespace-only. See
 * ConfigurableAgeOffFilter.
 */
public class AgeOffCsvToMatchPatternFormatter {
    private static final Logger log = LoggerFactory.getLogger(AgeOffCsvToMatchPatternFormatter.class);

    private static final String COMMA = ",";
    private static final char COLON = ':';
    private static final char EQUALS = '=';
    private static final char NEW_LINE = '\n';
    private static final char SPACE = ' ';
    private final AgeOffCsvToMatchPatternFormatterConfiguration configuration;
    private final Writer writer;
    private AgeOffCsvColumnInformation columnInformation;

    public AgeOffCsvToMatchPatternFormatter(AgeOffCsvToMatchPatternFormatterConfiguration configuration) {
        this.configuration = configuration;
        this.writer = this.configuration.getWriter();
    }

    /**
     * Reformats each line from reader and outputs them to writer. Closes both reader and writer when finished.
     *
     * @throws IOException
     *             i/o exception with reader or writer
     */
    public void write() throws IOException {
        String inputLine = configuration.getReader().readLine();

        while (null != inputLine) {
            reformat(inputLine);
            inputLine = configuration.getReader().readLine();
        }
        configuration.getReader().close();

        if (configuration.shouldCloseWriter()) {
            this.writer.close();
        }
    }

    private void reformat(String inputLine) throws IOException {
        String trimmedLine = inputLine.trim();

        if (isWhitespaceOnly(trimmedLine)) {
            propagateWhitespace(inputLine);
        } else if (isComment(trimmedLine)) {
            writeComment(trimmedLine);
        } else {
            // Use -1 to prevent chopping of empty tokens
            String[] tokens = inputLine.split(COMMA, -1);

            if (columnInformation == null) {
                log.debug("Attempting to parse header: {}", inputLine);
                initializeHeader(tokens);
            } else {
                reformatLine(tokens);
            }
        }
    }

    private boolean isWhitespaceOnly(String trimmedLine) {
        return trimmedLine.equals("");
    }

    private void propagateWhitespace(String inputLine) throws IOException {
        this.writer.write(inputLine + "\n");
    }

    private void initializeHeader(String[] tokens) {
        columnInformation = new AgeOffCsvColumnInformation();
        columnInformation.parseHeader(tokens);
    }

    private boolean isComment(String trimmedLine) {
        return trimmedLine.startsWith("#");
    }

    private void writeComment(String trimmedLine) throws IOException {
        this.writer.write("<!--" + trimmedLine.substring(1) + "-->\n");
    }

    private void reformatLine(String[] tokens) throws IOException {
        StringBuilder sb = new StringBuilder();

        appendLabel(tokens, sb);

        appendLiteral(tokens, sb);

        appendEquivalenceSymbol(sb);

        appendValue(sb, tokens);

        sb.append(NEW_LINE);

        this.writer.write(sb.toString());
    }

    private void appendValue(StringBuilder sb, String[] tokens) {
        String value = "";

        // use override value if it exists for this line (it might be empty)
        if (configuration.useOverrides()) {
            if (tokens.length <= columnInformation.overrideColumnNumber) {
                log.error("Unable to process override {}", Arrays.toString(tokens));
                throw new IllegalStateException("Not enough tokens");
            }
            value = tokens[columnInformation.overrideColumnNumber].trim();
        }

        // if overrides are disabled or override was missing
        if (value.length() == 0) {
            if (tokens.length <= columnInformation.durationColumnNumber) {
                log.error("Unable to process duration {}", Arrays.toString(tokens));
                throw new IllegalStateException("Not enough tokens");
            }
            value = tokens[columnInformation.durationColumnNumber].trim();
        }

        if (value.length() == 0) {
            log.error("Unable to find non-empty override or duration {}", Arrays.toString(tokens));
            throw new IllegalStateException("Empty token");
        }
        sb.append(attemptValueMapping(value));
    }

    private String attemptValueMapping(String originalValue) {
        if (null == configuration.getValueMapping()) {
            return originalValue;
        }

        String replacementValue = configuration.getValueMapping().get(originalValue);
        if (null == replacementValue) {
            return originalValue;
        }
        return replacementValue;
    }

    private void appendLabel(String[] tokens, StringBuilder sb) {
        if (configuration.shouldDisableLabel()) {
            return;
        }

        if (tokens.length <= columnInformation.labelColumnNumber) {
            log.error("Unable to process label {}", Arrays.toString(tokens));
            throw new IllegalStateException("Not enough tokens");
        }

        String label = "";

        if (null != configuration.getStaticLabel()) {
            label = configuration.getStaticLabel();
        } else if (columnInformation.labelColumnNumber != -1) {
            label = tokens[columnInformation.labelColumnNumber].trim();
        }

        if (label.length() == 0) {
            log.error("Unable to apply non-empty label {}", Arrays.toString(tokens));
            throw new IllegalStateException("Empty token");
        }
        sb.append(label).append(SPACE);
    }

    private void appendLiteral(String[] tokens, StringBuilder sb) {
        if (tokens.length <= columnInformation.patternColumnNumber) {
            log.error("Unable to process literal {}", Arrays.toString(tokens));
            throw new IllegalStateException("Not enough tokens");
        }

        if (configuration.shouldQuoteLiteral()) {
            sb.append(configuration.getQuoteCharacter());
        }

        String literal = tokens[columnInformation.patternColumnNumber].trim();
        if (literal.length() == 0) {
            log.error("Unable to find non-empty pattern {}", Arrays.toString(tokens));
            throw new IllegalStateException("Empty token");
        }

        if (configuration.shouldUpperCaseLiterals()) {
            literal = literal.toUpperCase();
        } else if (configuration.shouldLowerCaseLiterals()) {
            literal = literal.toLowerCase();
        }
        sb.append(literal);

        if (configuration.shouldQuoteLiteral()) {
            sb.append(configuration.getQuoteCharacter());
        }
    }

    private void appendEquivalenceSymbol(StringBuilder sb) {
        if (configuration.shouldPadEquivalence()) {
            sb.append(SPACE);
        }

        sb.append(configuration.useColons() ? COLON : EQUALS);

        if (configuration.shouldPadEquivalence()) {
            sb.append(SPACE);
        }
    }
}
