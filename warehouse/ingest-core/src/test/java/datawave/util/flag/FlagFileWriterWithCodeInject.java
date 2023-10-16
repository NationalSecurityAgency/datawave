package datawave.util.flag;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;

/**
 *
 * Assists with verifying the state of FlagFileWriter at various stages in the flag file writing process: before moving files to flagging and flagged; in error
 * handling conditions, and more.
 *
 * Lambdas can be provided to verify state at these stages.
 *
 * AtomicBooleans are used to verify that the lambda was executed during the call to writeFlagFile.
 */
public class FlagFileWriterWithCodeInject extends FlagFileWriter {

    private BiConsumer<Collection<InputFile>,List<Future<InputFile>>> beforeMoveToFlagged;
    private final AtomicBoolean wasBeforeMoveToFlaggedExecuted = new AtomicBoolean(false);

    private BiConsumer<Collection<InputFile>,List<Future<InputFile>>> afterMoveToFlagged;
    private final AtomicBoolean wasAfterMoveToFlaggedExecuted = new AtomicBoolean(false);

    private Consumer<File> beforeRemoveGeneratingExtension;
    private final AtomicBoolean wasBeforeRemoveGeneratingExtensionCalled = new AtomicBoolean(false);

    private BiConsumer<Collection<InputFile>,List<Future<InputFile>>> beforeMoveFiles;
    private final AtomicBoolean wasBeforeMoveFilesExecuted = new AtomicBoolean(false);

    private BiConsumer<Collection<InputFile>,List<Future<InputFile>>> beforeWaitForMoves;
    private final AtomicBoolean wasBeforeWaitForMovesExecuted = new AtomicBoolean(false);

    private Consumer<File> beforeRemoveFlagFile;
    private final AtomicBoolean wasBeforeRemoveFlagFileExecuted = new AtomicBoolean(false);

    private boolean shouldVerifyAfterMoveFilesBackIsCalled = false;
    private final AtomicBoolean wasMoveFilesBackExecuted = new AtomicBoolean(false);

    public FlagFileWriterWithCodeInject(FlagMakerConfig flagMakerConfig) throws IOException {
        super(flagMakerConfig);
    }

    FlagFileWriterWithCodeInject injectBeforeRemoveFlagFile(Consumer<File> desiredVerification) {
        this.beforeRemoveFlagFile = desiredVerification;
        return this;
    }

    FlagFileWriterWithCodeInject injectBeforeRemoveGeneratingExtension(Consumer<File> desiredVerification) {
        this.beforeRemoveGeneratingExtension = desiredVerification;
        return this;
    }

    FlagFileWriterWithCodeInject injectBeforeMoveToFlagged(BiConsumer<Collection<InputFile>,List<Future<InputFile>>> desiredVerification) {
        this.beforeMoveToFlagged = desiredVerification;
        return this;
    }

    FlagFileWriterWithCodeInject injectAfterMoveToFlagged(BiConsumer<Collection<InputFile>,List<Future<InputFile>>> desiredVerification) {
        this.afterMoveToFlagged = desiredVerification;
        return this;
    }

    FlagFileWriterWithCodeInject injectBeforeMoveToFlagging(BiConsumer<Collection<InputFile>,List<Future<InputFile>>> desiredVerification) {
        this.beforeMoveFiles = desiredVerification;
        return this;
    }

    FlagFileWriterWithCodeInject injectBeforeWaitForMove(BiConsumer<Collection<InputFile>,List<Future<InputFile>>> injectedCode) {
        if (beforeWaitForMoves != null) {
            throw new IllegalStateException("Cannot use both injectBeforeWaitForMove and injectAtMoveFilesBack");
        }
        this.beforeWaitForMoves = injectedCode;
        return this;
    }

    FlagFileWriterWithCodeInject injectAtMoveFilesBack(BiConsumer<Collection<InputFile>,List<Future<InputFile>>> injectedCode) {
        if (beforeWaitForMoves != null) {
            throw new IllegalStateException("Cannot use both injectBeforeWaitForMove and injectAtMoveFilesBack");
        }
        this.shouldVerifyAfterMoveFilesBackIsCalled = true;
        this.beforeWaitForMoves = injectedCode;
        return this;
    }

    @Override
    void writeFlagFile(final FlagDataTypeConfig flagDataTypeConfig, Collection<InputFile> inputFiles) throws IOException {
        try {
            // call writeFlagFile which will test subject
            super.writeFlagFile(flagDataTypeConfig, inputFiles);
        } finally {
            // fail if the override didn't get called
            // Note that this is deliberately not in a finally block to allow
            // exceptions in override
            if (beforeRemoveFlagFile != null && !wasBeforeRemoveFlagFileExecuted.get()) {
                fail("The beforeRemoveFlagFile code was not executed.");
            }
            if (beforeWaitForMoves != null && !wasBeforeWaitForMovesExecuted.get()) {
                fail("The beforeWaitForMoves code was not executed.");
            }
            if (beforeRemoveGeneratingExtension != null && !wasBeforeRemoveGeneratingExtensionCalled.get()) {
                fail("The beforeRemoveGeneratingExtension code was not executed.");
            }
            if (beforeMoveFiles != null && !wasBeforeMoveFilesExecuted.get()) {
                fail("The beforeMoveFiles code was not executed.");
            }
            if (beforeMoveToFlagged != null && !wasBeforeMoveToFlaggedExecuted.get()) {
                fail("The beforeMoveToFlagged code was not executed.");
            }
            if (afterMoveToFlagged != null && !wasAfterMoveToFlaggedExecuted.get()) {
                fail("The afterMoveToFlagged code was not executed.");
            }
        }
    }

    @Override
    Collection<InputFile> moveFiles(Collection<InputFile> files, List<Future<InputFile>> futures, Function<InputFile,SimpleMover> moverFactory, String label)
                    throws IOException {
        if (beforeMoveFiles != null) {
            wasBeforeMoveFilesExecuted.set(true);
            beforeMoveFiles.accept(files, futures);
        }
        return super.moveFiles(files, futures, moverFactory, label);
    }

    @Override
    void moveFilesToFlagged(FlagDataTypeConfig fc, FlagMetrics metrics, Collection<InputFile> flaggingFiles, List<Future<InputFile>> futures)
                    throws IOException {
        if (beforeWaitForMoves != null || beforeRemoveFlagFile != null) {
            throw new IOException("Throw an exception to cause cleanup to occur");
        } else if (beforeMoveToFlagged != null) {
            wasBeforeMoveToFlaggedExecuted.set(true);
            beforeMoveToFlagged.accept(flaggingFiles, futures);
        }
        super.moveFilesToFlagged(fc, metrics, flaggingFiles, futures);

        if (afterMoveToFlagged != null) {
            wasAfterMoveToFlaggedExecuted.set(true);
            afterMoveToFlagged.accept(flaggingFiles, futures);
        }
    }

    @Override
    void removeFlagFile(File flagFile) {
        if (beforeRemoveFlagFile != null) {
            wasBeforeRemoveFlagFileExecuted.set(true);
            try {
                beforeRemoveFlagFile.accept(flagFile);
            } catch (Exception e) {
                throw new RuntimeException();
            }
        }

        super.removeFlagFile(flagFile);
    }

    @Override
    void moveFilesBack(Collection<InputFile> files, List<Future<InputFile>> moveOperations) throws IOException {
        wasMoveFilesBackExecuted.set(true);
        super.moveFilesBack(files, moveOperations);
    }

    @Override
    boolean waitForMoves(Collection<InputFile> movedInputFiles, List<Future<InputFile>> moveOperations) throws IOException {
        if (beforeWaitForMoves != null) {
            // only run the injected code when waitForMoves is called by
            // moveFilesBack
            boolean notWaitingForMoveFileBackExecution = !shouldVerifyAfterMoveFilesBackIsCalled || wasMoveFilesBackExecuted.get();
            boolean wasNotAlreadyExecuted = !wasBeforeWaitForMovesExecuted.get();
            if (wasNotAlreadyExecuted && notWaitingForMoveFileBackExecution) {
                wasBeforeWaitForMovesExecuted.set(true);
                beforeWaitForMoves.accept(movedInputFiles, moveOperations);
            }
        }
        return super.waitForMoves(movedInputFiles, moveOperations);
    }

    @Override
    File removeGeneratingExtension(File flagFile, String baseName) throws IOException {
        if (beforeRemoveGeneratingExtension != null) {
            wasBeforeRemoveGeneratingExtensionCalled.set(true);
            beforeRemoveGeneratingExtension.accept(flagFile);
        }
        return super.removeGeneratingExtension(flagFile, baseName);
    }
}
