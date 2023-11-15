package datawave.util.flag;

import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.config.FlagMakerConfigUtility;
import datawave.util.flag.processor.FlagDistributor;
import datawave.util.flag.processor.SizeValidator;

/**
 * Watches input file directories to create flag files. Flag files serve as a set of instructions for starting an ingest job, containing a command and a list of
 * input files. See start-ingest-servers.sh.
 */
public class FlagMaker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FlagMaker.class);

    private final FlagMakerConfig flagMakerConfig;
    final FlagDistributor fileDistributor;
    private final FileSystem fs;
    private final SizeValidator sizeValidator;
    private volatile boolean running = true;

    protected final JobConf config;
    private final FlagFileWriter flagFileWriter;
    private final LinkedBlockingQueue<String> socketMessageQueue;

    public FlagMaker(FlagMakerConfig flagMakerConfig) throws IOException {
        this.flagMakerConfig = flagMakerConfig;
        LOG.info("Flag Maker Config (pre-validate): {}", this.flagMakerConfig);

        this.config = new JobConf(new Configuration());

        this.flagMakerConfig.validate();
        LOG.info("Flag Maker Config (post-validate): {}", this.flagMakerConfig);

        this.fileDistributor = createFlagDistributor(this.flagMakerConfig);
        this.sizeValidator = new SizeValidatorImpl(this.config, this.flagMakerConfig);
        this.flagFileWriter = new FlagFileWriter(this.flagMakerConfig);
        this.fs = FlagMakerConfigUtility.getHadoopFS(flagMakerConfig);

        this.socketMessageQueue = new LinkedBlockingQueue<>();
    }

    public static void main(String... args) throws Exception {
        FlagMakerConfig flagMakerConfig = FlagMakerConfigUtility.parseArgs(args);

        boolean shutdown = false;
        for (String arg : args) {
            if ("-shutdown".equals(arg)) {
                shutdown = true;
                break;
            }
        }

        if (shutdown) {
            shutdown(flagMakerConfig.getSocketPort());
            System.exit(0);
        }

        try {
            FlagMaker m = createFlagMaker(flagMakerConfig);
            m.run();
        } catch (IllegalArgumentException ex) {
            System.err.println("" + ex.getMessage());
            printUsage();
            System.exit(1);
        }

    }

    private static FlagMaker createFlagMaker(FlagMakerConfig fc) {
        try {
            Class<? extends FlagMaker> c = Class.forName(fc.getFlagMakerClass()).asSubclass(FlagMaker.class);
            Constructor<? extends FlagMaker> constructor = c.getConstructor(FlagMakerConfig.class);
            return constructor.newInstance(fc);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Subclasses of FlagMaker must implement a constructor that takes a FlagMakerConfig", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate FlagMaker of type " + fc.getFlagMakerClass(), e);
        }
    }

    private static FlagDistributor createFlagDistributor(FlagMakerConfig fc) {
        String flagDistributorClassName = fc.getFlagDistributorClass();
        try {
            Class<? extends FlagDistributor> c = Class.forName(flagDistributorClassName).asSubclass(FlagDistributor.class);
            Constructor<? extends FlagDistributor> constructor = c.getConstructor(FlagMakerConfig.class);
            return constructor.newInstance(fc);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Subclasses of FlagDistributor must implement a constructor that takes a FlagMakerConfig", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate FlagDistributor of type " + flagDistributorClassName, e);
        }
    }

    private static void shutdown(int port) throws IOException {
        try (Socket s = new Socket("localhost", port); PrintWriter pw = new PrintWriter(s.getOutputStream(), true)) {
            pw.write(FlagSocket.SHUTDOWN_MESSAGE);
            pw.flush();
        }
    }

    private static void printUsage() {
        System.out.println("To run the Flag Maker: ");
        System.out.println("datawave.ingest.flag.FlagMaker -flagConfig [path to xml config]");
        System.out.println("Optional arguments:");
        System.out.println("\t\t-shutdown\tDescription: shuts down the flag maker using configured socketPort");
        System.out.println("\t\t-baseHDFSDirOverride [HDFS Path]\tDescription: overrides baseHDFSDir in xml");
        System.out.println("\t\t-extraIngestArgsOverride [extra ingest args]\tDescription: overrides extraIngestArgs value in xml config");
        System.out.println("\t\t-flagFileDirectoryOverride [local path]\tDescription: overrides flagFileDirectory value in xml config");
    }

    @Override
    public void run() {
        LOG.trace("{} run() starting", this.getClass().getSimpleName());
        startSocketAndReceiver();
        try {
            while (running) {
                try {
                    processFlags();
                    // noinspection BusyWait
                    Thread.sleep(flagMakerConfig.getSleepMilliSecs());
                } catch (Exception ex) {
                    LOG.error("An unexpected exception occurred. Exiting", ex);
                    running = false;
                }
            }
        } finally {
            this.flagFileWriter.close();
        }
        LOG.trace("{} Exiting.", this.getClass().getSimpleName());
    }

    /**
     * @throws IOException
     *             unable to load files for distributor
     */
    protected void processFlags() throws IOException {
        LOG.trace("Querying for files on {}", fs.getUri().toString());

        for (FlagDataTypeConfig flagDataTypeConfig : flagMakerConfig.getFlagConfigs()) {
            fileDistributor.loadFiles(flagDataTypeConfig);
            while (fileDistributor.hasNext(shouldOnlyCreateFullFlags(flagDataTypeConfig)) && running) {
                Collection<InputFile> inFiles = fileDistributor.next(this.sizeValidator);
                if (null == inFiles || inFiles.isEmpty()) {
                    throw new IllegalStateException(
                                    fileDistributor.getClass().getName() + " has input files but returned zero candidates for flagging. Please validate configuration");
                }

                flagFileWriter.writeFlagFile(flagDataTypeConfig, inFiles);
            }

        }
    }

    private boolean shouldOnlyCreateFullFlags(FlagDataTypeConfig fc) {
        return !hasTimeoutOccurred(fc) || isBacklogExcessive(fc);
    }

    private boolean isBacklogExcessive(FlagDataTypeConfig fc) {
        if (fc.getFlagCountThreshold() == FlagMakerConfig.UNSET) {
            LOG.trace("Not evaluating flag file backlog.  getFlagCountThreshold = {}", FlagMakerConfig.UNSET);
            return false;
        }
        int sizeOfFlagFileBacklog = countFlagFileBacklog(fc);
        if (sizeOfFlagFileBacklog >= fc.getFlagCountThreshold()) {
            LOG.debug("Flag file backlog is excessive: sizeOfFlagFileBacklog: {}, flagCountThreshold: {}", sizeOfFlagFileBacklog, fc.getFlagCountThreshold());
            return true;
        }
        return false;
    }

    private boolean hasTimeoutOccurred(FlagDataTypeConfig fc) {
        long now = System.currentTimeMillis();
        // fc.getLast indicates when the flag file creation timeout will occur
        boolean hasTimeoutOccurred = (now >= fc.getLast());

        if (!hasTimeoutOccurred) {
            LOG.debug("Still waiting for timeout for {}, now={}, last={}, (now-last)={}", fc.getDataName(), now, fc.getLast(), (now - fc.getLast()));
        }
        return hasTimeoutOccurred;
    }

    /**
     * Determine the number of unprocessed flag files in the flag directory
     *
     * @param fc
     *            configuration for datatype
     * @return the flag found for this ingest pool
     */
    private int countFlagFileBacklog(final FlagDataTypeConfig fc) {
        final MutableInt fileCounter = new MutableInt(0);
        final FileFilter fileFilter = new WildcardFileFilter("*_" + fc.getIngestPool() + "_" + fc.getDataName() + "_*.flag");
        final FileVisitor<java.nio.file.Path> visitor = new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(java.nio.file.Path path, BasicFileAttributes attrs) throws IOException {
                if (fileFilter.accept(path.toFile())) {
                    fileCounter.increment();
                }
                return super.visitFile(path, attrs);
            }
        };
        try {
            Files.walkFileTree(Paths.get(flagMakerConfig.getFlagFileDirectory()), visitor);
        } catch (IOException e) {
            LOG.error("Unable to get flag file count", e);
            return -1;
        }
        return fileCounter.intValue();
    }

    public void receiveActionableMessage(String message) {
        if (FlagSocket.SHUTDOWN_MESSAGE.equals(message)) {
            LOG.info("Received " + FlagSocket.SHUTDOWN_MESSAGE);
            running = false;
        } else if (message.startsWith(FlagSocket.KICK_MESSAGE)) {
            LOG.info("Received " + FlagSocket.KICK_MESSAGE);
            executeKickAction(message);
        } else {
            LOG.info("Received unknown message " + message);
        }
    }

    private void executeKickAction(String message) {
        String dataType = message.substring(FlagSocket.KICK_MESSAGE.length()).trim();
        if (dataType.isEmpty()) {
            LOG.warn("'" + FlagSocket.KICK_MESSAGE + "' message received without a dataTypeName.  Try 'kick dataTypeName'");
            return;
        }
        LOG.info("Attempting '" + FlagSocket.KICK_MESSAGE + "' for " + dataType);
        for (FlagDataTypeConfig cfg : flagMakerConfig.getFlagConfigs()) {
            if (cfg.getDataName().equals(dataType)) {
                LOG.info("Forcing {} to generate flag file", dataType);
                cfg.setLast(System.currentTimeMillis() - cfg.getTimeoutMilliSecs());
                return;
            }
        }
        List<String> dataTypeNames = flagMakerConfig.getFlagConfigs().stream().map(FlagDataTypeConfig::getDataName).collect(Collectors.toList());
        LOG.info("No match found for '" + dataType + "' among " + dataTypeNames.toString());

    }

    private void startSocketAndReceiver() {
        try {
            FlagSocket flagSocket = new FlagSocket(flagMakerConfig.getSocketPort(), socketMessageQueue);
            Thread socketThread = new Thread(flagSocket, "Flag_Socket_Thread");
            socketThread.setDaemon(true);
            socketThread.start();

            Thread messageHandlerThread = new Thread(() -> {
                while (running) {
                    try {
                        receiveActionableMessage(socketMessageQueue.take());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, "Flag_Message_Reader_Thread");
            messageHandlerThread.setDaemon(true);
            messageHandlerThread.start();
        } catch (IOException ex) {
            LOG.error("Error occurred while starting socket. Exiting.", ex);
            running = false;
        }
    }
}