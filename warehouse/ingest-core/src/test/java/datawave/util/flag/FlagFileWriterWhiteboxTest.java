package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;

// @formatter:off
/**
 * Test index
 *
 ***
 *** Verify transition states during normal operations (without failures):
 ***
 *
 * movesFilesFromInputDirToFlaggingDir - Verify that before the move to flagged,
 * all files are in the flagging directory
 *
 * movesFilesFromFlaggingDirToFlaggedDirBeforeRename - Verify files are moved
 * from flagging to flagged before the flag file rename to drop the .generated
 * extension
 *
 * firstCreatesAGeneratingFlagFile - Verify that a .generating file is created
 * just before the move from flagging to flagged
 *
 ***
 *** Verify behavior when a file goes missing
 ***
 *
 * throwsErrorIfInputFileGoesMissing - Before the move to flagging, delete an
 * input file - Expect an IOException
 *
 * throwsErrorIfFlaggingFileGoesMissing - Before the move to flagged, delete a
 * flagging file - Expect an IOException
 *
 * movesOtherFilesBackAfterInputFileGoesMissing - Before the move to flagging,
 * delete an input file - Verify the other 9 files return to input directory
 *
 * movesOtherFilesBackAfterFlaggingFileGoesMissing - Before the move to flagged,
 * delete a flagging file - Verify the other 9 files return to input directory
 *
 ***
 *** Verify cleanup when unfinished futures remain
 ***
 *
 * residualFutureSuccessfulUponRetry - After the move to flagged, add a Future
 * that returns a valid InputFile - Verify that this unexpected state causes
 * cleanup to occur and retries waiting for the Future
 *
 * residualFutureFailsRetry - After the move to flagged, add a Future that
 * sleeps beyond retry timeout - Verify that this unexpected state causes
 * cleanup to occur and it skips the unresponsive Future
 *
 ***
 *** Verify cleanup after forcing an exception during normal operations
 ***
 *
 * cleanupRemovesGeneratingFlagFile - Verify that the .generating file exists
 * before the move to flagged - Throw an exception instead of doing the move -
 * Verify the .generating file is cleaned up after the failure
 *
 * cleanupMovesFilesBeforeRemovingFlagFile - Throw an exception - Ensure all 10
 * files are moved back to input directory before removing the flag file
 *
 * cleanupMovesFilesFromFlaggingToInputDir - Throw an exception before the move
 * to flagged - Verify all 10 files are in flagging before the exception -
 * Verify all 10 files are in the input directory after clean up
 *
 * cleanupMovesFilesFromFlaggedToInputDir - Throw an exception after the move to
 * flagged - Verify all 10 files are in flagged before the exception - Verify
 * all 10 files are in the input directory after clean up
 *
 ***
 *** Verify that failure recovery is fault tolerant
 ***
 *
 * toleratesMissingFileDuringFailureScenario - Throw an exception when files are
 * moved to flagged, triggering failure recovery. - During failure recovery,
 * just before files are moved back to the input directory, delete a file -
 * Verify the other 9 files are moved back to the input directory and flag file
 * is deleted
 *
 * toleratesFailedFutureDuringFailureScenario - Throw an exception when files
 * are moved to flagged, triggering failure recovery. - During failure recovery,
 * just before files are moved back to the input directory, insert a future that
 * throws an exception - Verify the files are moved back to the input directory
 * regardless of the failure and flag file is deleted
 *
 * toleratesMissingFileAndFailedFutureDuringFailureScenario - Throw an exception
 * when files are moved to flagged, triggering failure recovery. - During
 * failure recovery, just before files are moved back to the input directory,
 * delete a file - During failure recovery, just before files are moved back to
 * the input directory, insert a future that throws an exception - Verify the
 * other 9 files are moved back to the input directory and flag file is deleted
 *
 * toleratesCancellationInFailureScenario - Throw an exception when files are
 * moved to flagged, triggering failure recovery. - Call cancel on one of the
 * futures that exist to move files back to the input directory - Verify the
 * other 9 files are moved back to the input directory - Verify that one of the
 * flagging files remains in flagging - Verify that flag file is deleted
 *
 * prematurelyMovedFileIgnoredDuringCleanup - Intercept one of the futures that
 * moves files from input to flagging - Wait for the file to move to flagging
 * and then manually move it to flagged - This will cause an exception when the
 * code tries to move the file from flagging to flagged - Verify that the other
 * 9 files are correctly moved to input and the flag file is removed - Verify
 * that the one file remains in flagged
 *
 * handleSystemExit - Verify that an expected System.exit will properly run
 * cleanup
 */
// @formatter:on
public class FlagFileWriterWhiteboxTest {
    private static final int EXPECTED_NUM_FILES = 10;
    private static final int FILES_PER_DAY = 5;

    private FlagFileTestSetup flagFileTestSetup;
    private FlagMakerConfig flagMakerConfig;
    private FlagDataTypeConfig dataTypeConfig;
    private Collection<InputFile> inputFiles;
    private FileSystem fs;

    @Rule
    public TestName testName = new TestName();

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void before() throws Exception {
        String subdirectoryName = this.getClass().getName() + "_" + this.testName.getMethodName();
        flagFileTestSetup = new FlagFileTestSetup().withTestFlagMakerConfig().withTestNameForDirectories(subdirectoryName);
        flagMakerConfig = flagFileTestSetup.getFlagMakerConfig();
        fs = flagFileTestSetup.getFileSystem();
        this.dataTypeConfig = flagFileTestSetup.getInheritedDataTypeConfig();
        this.inputFiles = createInputFiles();
    }

    @After
    public void cleanup() throws IOException {
        flagFileTestSetup.deleteTestDirectories();
    }

    @Test
    public void movesFilesFromInputDirToFlaggingDir() throws IOException {
        // pre-condition: no files in flagging
        boolean doesFlaggingDirExist = Files.exists(Paths.get(flagMakerConfig.getBaseHDFSDir() + "/flagging"));
        assertFalse("Flagging shouldn't exist until writeFlagFile is called", doesFlaggingDirExist);

        // post-condition: files now in flagging
        new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeMoveToFlagged((files, futures) -> {
            try {
                assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
                assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
                assertEquals(0, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).writeFlagFile(dataTypeConfig, inputFiles);
    }

    @Test
    public void movesFilesFromFlaggingDirToFlaggedDirBeforeRename() throws IOException {
        // pre-condition: no files in flagged
        assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
        boolean doesFlaggedDirExist = Files.exists(Paths.get(flagMakerConfig.getBaseHDFSDir() + "/flagged"));
        assertFalse("Flagged shouldn't exist until writeFlagFile is called", doesFlaggedDirExist);

        // post-condition: files now in flagging
        new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeRemoveGeneratingExtension((file) -> {
            try {
                assertEquals(0, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
                assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
                assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
            } catch (IOException e) {
                fail("The expected Exception should not occur here.");
            }
        }).writeFlagFile(dataTypeConfig, inputFiles);
    }

    @Test
    public void firstCreatesAGeneratingFlagFile() throws IOException {
        // pre-condition: no flag files at all
        assertEquals(0, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());

        new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeMoveToFlagged((files, futures) -> {
            String flagFileName = FlagFileTestInspector.getPathStringForOnlyFlagFile(flagMakerConfig);

            assertTrue("Expected it to be a file, not a directory", new File(flagFileName).isFile());

            String expectedExtension = ".flag.generating";
            assertTrue("Did not contain proper file extension", flagFileName.endsWith(expectedExtension));

            String expectedFlagFileDirectory = flagMakerConfig.getFlagFileDirectory();
            assertEquals("Wrote to unexpected location", expectedFlagFileDirectory, flagFileName.substring(0, flagFileName.lastIndexOf('/')));

            // FlagFileWriterNamingTests verifies more
            // information about the flag file name
            assertTrue("Non trivial name", expectedExtension.length() + expectedFlagFileDirectory.length() < flagFileName.length());

            // FlagFileWriterContentTest verifies more
            // information about the contents of the flag file
            try {
                assertTrue("Flag file is unexpectedly empty", Files.size(new File(flagFileName).toPath()) > 0);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).writeFlagFile(dataTypeConfig, inputFiles);
    }

    @Test
    public void throwsErrorIfInputFileGoesMissing() throws Exception {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Failure during move");

        new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeMoveToFlagging((files, futures) -> {
            try {
                // delete one of the files in the flagging directory
                InputFile firstInputFile = inputFiles.iterator().next();
                Path inputFile = firstInputFile.getPath(); // Path under
                                                           // input
                                                           // directory
                assertTrue(fs.delete(inputFile, false));
            } catch (IOException e) {
                fail("The expected Exception should not occur here.");
            }

        }).writeFlagFile(dataTypeConfig, inputFiles);
    }

    @Test
    public void throwsErrorIfFlaggingFileGoesMissing() throws Exception {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Failure during move");

        // pre-condition: no files in flagging
        boolean doesFlaggingDirExist = Files.exists(Paths.get(flagMakerConfig.getBaseHDFSDir() + "/flagging"));
        assertFalse("Flagging shouldn't exist until writeFlagFile is called", doesFlaggingDirExist);

        // delete one of the files that has a futures entry in
        // moveInputsFromFlaggingToFlagged
        new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeMoveToFlagged((files, futures) -> {
            try {
                // delete one of the files in the flagging directory
                InputFile firstInputFile = inputFiles.iterator().next();
                Path fileInFlaggingDir = firstInputFile.getFlagging(); // Path
                                                                       // under
                                                                       // flagging
                assertTrue(fs.delete(fileInFlaggingDir, false));
            } catch (IOException e) {
                fail("The expected Exception should not occur here.");
            }
        }).writeFlagFile(dataTypeConfig, inputFiles);
    }

    @Test
    public void movesOtherFilesBackAfterInputFileGoesMissing() throws Exception {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Failure during move");

        // count number of files in the input directory before processing
        assertEquals(10, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeMoveToFlagging((files, futures) -> {
                try {
                    // delete one of the files in the input directory
                    InputFile firstInputFile = inputFiles.iterator().next();
                    Path inputFile = firstInputFile.getPath(); // Path
                                                               // under
                                                               // input
                                                               // directory
                    assertTrue(fs.delete(inputFile, false));
                } catch (IOException e) {
                    fail("The expected Exception should not occur here.");
                }

            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            // expect all but one of the files to be back in the input directory
            assertEquals(9, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
        }
    }

    @Test
    public void movesOtherFilesBackAfterFlaggingFileGoesMissing() throws Exception {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Failure during move");

        // count number of files in the input directory before processing
        assertEquals(10, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());

        try {
            // delete one of the files that has a futures entry in
            // moveInputsFromFlaggingToFlagged
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeMoveToFlagged((files, futures) -> {
                try {
                    // delete one of the files in the flagging directory
                    InputFile firstInputFile = inputFiles.iterator().next();
                    Path fileInFlaggingDir = firstInputFile.getFlagging(); // Path under flagging
                    assertTrue(fs.delete(fileInFlaggingDir, false));
                } catch (IOException e) {
                    fail("The expected Exception should not occur here.");
                }
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            // expect all but one of the files to be back in the input directory
            assertEquals(9, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
        }
    }

    @Test
    public void residualFutureSuccessfulUponRetry() throws IOException {
        exceptionRule.expect(AssertionError.class);
        exceptionRule.expectMessage("1");

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig)
                            .injectAfterMoveToFlagged(
                                            (files, futures) -> futures.add(Executors.newSingleThreadExecutor().submit(() -> files.iterator().next())))
                            .writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals("The flag file was not cleaned up.", 0, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
            assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
        }
    }

    @Test
    public void residualFutureFailsRetry() throws IOException {
        exceptionRule.expect(AssertionError.class);
        exceptionRule.expectMessage("1");

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectAfterMoveToFlagged((files, futures) -> {
                futures.add(Executors.newSingleThreadExecutor().submit(() -> {
                    try {
                        Thread.sleep(10L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException("Expected Future to get cancelled before waking from sleep");
                }));
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals("The flag file was not cleaned up.", 0, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
            assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
        }
    }

    @Test
    public void cleanupRemovesGeneratingFlagFile() throws IOException {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Throw exception to test clean up of .flag.generating file");

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeMoveToFlagged((files, futures) -> {
                // verify .generating file exists before throwing an
                // exception to trigger cleanup
                List<File> flagFiles = FlagFileTestInspector.listFlagFiles(flagMakerConfig);
                String flagFileName = flagFiles.get(0).getName();
                String expectedExtension = ".flag.generating";
                assertTrue("Did not contain proper file extension", flagFileName.endsWith(expectedExtension));

                throw new RuntimeException("Throw exception to test clean up of .flag.generating file");
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals("The flag file was not cleaned up.", 0, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());
        }
    }

    @Test
    public void cleanupMovesFilesBeforeRemovingFlagFile() throws IOException {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Throw an exception to cause cleanup to occur");

        // moves files first
        // cleanup flag.generating second
        new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeRemoveFlagFile((file) -> {
            // all files should be back in the input directory at this
            // point
            try {
                assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());

                assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
                assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
            } catch (IOException e) {
                fail("The expected Exception should not occur here.");
            }
            // flag.generating should still exist
            String flagFileName = FlagFileTestInspector.getPathStringForOnlyFlagFile(flagMakerConfig);
            String expectedExtension = ".flag.generating";
            assertTrue("flag.generating file should exist until all the files are back to the input directory", flagFileName.endsWith(expectedExtension));
        }).writeFlagFile(dataTypeConfig, inputFiles);
    }

    @Test
    public void cleanupMovesFilesFromFlaggingToInputDir() throws IOException {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Throw exception to test clean up of flagging files");

        assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());

        try {
            // post-condition: files now in flagging
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeMoveToFlagged((files, futures) -> {
                try {
                    assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
                } catch (IOException e) {
                    fail("The expected Exception should not occur here.");
                }

                throw new RuntimeException("Throw exception to test clean up of flagging files");
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
            assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
        }
    }

    @Test
    public void cleanupMovesFilesFromFlaggedToInputDir() throws IOException {
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage("Throw exception to test clean up of flagged files");

        assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());

        try {
            // post-condition: files now in flagging
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeRemoveGeneratingExtension((file) -> {
                try {
                    assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
                } catch (IOException e) {
                    fail("The expected Exception should not occur here.");
                }

                throw new RuntimeException("Throw exception to test clean up of flagged files");
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
            assertEquals(EXPECTED_NUM_FILES, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
        }
    }

    @Test
    public void toleratesMissingFileDuringFailureScenario() throws Exception {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Throw an exception to cause cleanup to occur");
        assertEquals(10, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectAtMoveFilesBack((files, moveOperations) -> {
                // expect at least some files to be in flagging
                try {
                    assertEquals(1, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());
                    assertTrue(0 < FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
                } catch (Throwable e) {
                    fail("Incorrect preconditions within lambda" + e.getMessage());
                }

                try {
                    InputFile inputFile = moveOperations.get(0).get();
                    assertTrue(fs.delete(inputFile.getPath(), false));
                } catch (Throwable e) {
                    fail(e.getMessage());
                }
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            // expect all but one of the files to be back in the input directory
            assertEquals(9, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
        }
    }

    @Test
    public void toleratesFailedFutureDuringFailureScenario() throws Exception {

        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Throw an exception to cause cleanup to occur");
        assertEquals(10, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectAtMoveFilesBack((files, moveOperations) -> {

                // expect at least some files to be in flagging
                try {
                    assertEquals(1, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());
                    assertTrue(0 < FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
                } catch (Throwable e) {
                    fail("Incorrect preconditions within lambda" + e.getMessage());
                }

                // don't touch the existing 10 file movers. Insert
                // an additional future that simply throws an
                // exception
                moveOperations.add(0, Executors.newSingleThreadExecutor().submit(() -> {
                    throw new RuntimeException("Add a future that will throw an exception");
                }));
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals(0, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());

            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(10, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
        }
    }

    @Test
    public void toleratesMissingFileAndFailedFutureDuringFailureScenario() throws Exception {

        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Throw an exception to cause cleanup to occur");
        assertEquals(10, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectAtMoveFilesBack((files, moveOperations) -> {
                // expect at least some files to be in flagging
                try {
                    assertEquals(1, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());
                    assertTrue(0 < FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
                } catch (Throwable e) {
                    fail("Incorrect preconditions within lambda" + e.getMessage());
                }

                // add a future that will fail
                moveOperations.add(0, Executors.newSingleThreadExecutor().submit(() -> {
                    throw new RuntimeException("Add a future that will throw an exception");
                }));

                // delete one of the files that was moved
                try {
                    InputFile inputFile = moveOperations.get(1).get();
                    assertTrue(fs.delete(inputFile.getPath(), false));
                } catch (Throwable e) {
                    fail(e.getMessage());
                }
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals(0, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());

            // expect all but one of the files to be back in the input directory
            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(9, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
        }
    }

    @Test
    public void toleratesCancellationInFailureScenario() throws Exception {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Throw an exception to cause cleanup to occur");
        assertEquals(10, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectAtMoveFilesBack((files, moveOperations) -> {
                // Replace one of the intercepted futures with one that
                // throws a Cancellation Exception
                // This will result in a later failure when the flag
                // file attempts to move it from flagging to flagged
                moveOperations.get(9).cancel(true);
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals(0, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());

            // expect all but one of the files to be back in the input directory
            assertEquals(9, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
            assertEquals(1, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
        }
    }

    @Test
    public void prematurelyMovedFileIgnoredDuringCleanup() throws Exception {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Throw an exception to cause cleanup to occur");

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeWaitForMove((files, moveOperations) -> {
                InputFile flaggingFile = null;
                try {
                    flaggingFile = moveOperations.get(9).get();
                } catch (Exception e) {
                    fail("Failed to capture an InputFile from futures");
                }
                // move to unexpected location
                try {
                    fs.rename(flaggingFile.getFlagging(), flaggingFile.getFlagged());
                } catch (IOException e) {
                    fail("Failed to move an InputFile to flagged");
                }
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals(0, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());

            // expect all but one of the files to be back in the input directory
            assertEquals(9, FlagFileTestInspector.listFilesInInputDirectory(flagMakerConfig).size());
            assertEquals(0, FlagFileTestInspector.listFlaggingFiles(flagMakerConfig).size());
            assertEquals(1, FlagFileTestInspector.listFlaggedFiles(flagMakerConfig).size());
        }
    }

    @Test
    public void handleSystemExit() throws Exception {
        exit.expectSystemExit();

        try {
            new FlagFileWriterWithCodeInject(flagMakerConfig).injectBeforeMoveToFlagged((files, futures) -> {
                System.exit(0);
            }).writeFlagFile(dataTypeConfig, inputFiles);
        } finally {
            assertEquals("The flag file was not cleaned up.", 0, FlagFileTestInspector.listFlagFiles(flagMakerConfig).size());
        }
    }

    private TreeSet<InputFile> createInputFiles() throws IOException {
        // creates 2 * filesPerDay number of files: filesPerDay in foo,
        // filesPerDay in bar
        flagFileTestSetup.withPredicableInputFilenames().withFilesPerDay(FILES_PER_DAY).withNumDays(1).createTestFiles();
        TreeSet<InputFile> sortedFiles = new TreeSet<>(InputFile.FIFO);
        sortedFiles.addAll(FlagFileTestInspector.listSortedInputFiles(flagMakerConfig, fs));
        // verify file creation
        assertNotNull(sortedFiles);
        assertEquals(2 * FILES_PER_DAY, sortedFiles.size());
        return sortedFiles;
    }
}
