package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.junit.Assert;

import com.google.common.io.Files;

/**
 * Defines expectations for the contents of a flag file, e.g. contains a file marker and asserts those expectations are met for a given file.
 */
public class FlagFileContentExpectations {
    private static final char NEWLINE = '\n';
    private String beginning = "";
    private String ending = "";
    private Collection<InputFile> inputFiles = new HashSet<>();
    private boolean isSorted = false;
    private boolean includesFileMarker = false;
    private String actualFileContents;

    public FlagFileContentExpectations withBeginning(String beginning) {
        this.beginning = beginning;
        return this;
    }

    public FlagFileContentExpectations withEnding(String ending) {
        this.ending = ending;
        return this;
    }

    public FlagFileContentExpectations withFiles(Collection<InputFile> inputFiles) {
        this.inputFiles = inputFiles;
        return this;
    }

    public FlagFileContentExpectations withFileOrdering(boolean isSorted) {
        this.isSorted = isSorted;
        return this;
    }

    public FlagFileContentExpectations withFileMarker(boolean includesFileMarker) {
        this.includesFileMarker = includesFileMarker;
        return this;
    }

    public void assertFlagFileContents(String flagContents) {
        this.actualFileContents = flagContents;

        if (this.isSorted) {
            verifySortedFileContents();
        } else {
            verifyUnsortedFileContents();
        }
    }

    public void assertFlagFileContents(File flagFile) {
        try {
            String fileContents = Files.asCharSource(flagFile, Charset.defaultCharset()).read();
            assertFlagFileContents(fileContents);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void verifySortedFileContents() {
        String fileDelimiter = this.includesFileMarker ? String.valueOf(NEWLINE) : ",";

		String expectedFileContents = this.beginning + joinInputFileNames(fileDelimiter) + this.ending;

        Assert.assertEquals(expectedFileContents, actualFileContents);
    }

    private void verifyUnsortedFileContents() {
        assertBeginning(this.beginning, this.actualFileContents);
        assertEnding(this.ending, this.actualFileContents);

        int expectedNumberOfCharacters = this.beginning.length() + this.ending.length();

        for (InputFile inFile : inputFiles) {
            String expectedFileName = inFile.getFlagged().toUri().toString();
            assertTrue("Expected " + expectedFileName + "\nTo be in " + actualFileContents, actualFileContents.contains(expectedFileName));
            // including a delimiter
            expectedNumberOfCharacters += expectedFileName.length() + 1;
        }

        assertEquals("Unexpected number of characters in actual file:" + actualFileContents, expectedNumberOfCharacters, actualFileContents.length());
    }

    private String joinInputFileNames(String delimiter) {
        return this.inputFiles.stream().map(inputFile -> inputFile.getFlagged().toUri().toString()).collect(Collectors.joining(delimiter));
    }

    private void assertBeginning(String expectedBeginning, String actual) {
        assertEquals(expectedBeginning, actual.substring(0, expectedBeginning.length()));
    }

    private void assertEnding(String expectedEnding, String actual) {
        int indexOfExpectedEnding = actual.length() - expectedEnding.length();
        if (indexOfExpectedEnding < 0) {
            fail("Expected ending:\n" + expectedEnding + "\nActual file contents are too short:\n" + actual);
        }
        String actualEnding = actual.substring(indexOfExpectedEnding);
        assertEquals(expectedEnding, actualEnding);
    }
}
