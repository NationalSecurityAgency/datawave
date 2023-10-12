package datawave.util.flag;

import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintStream;
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
import java.util.concurrent.LinkedBlockingQueue;

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

    private final FlagMakerConfig fmc;
    final FlagDistributor fd;
    private final FileSystem fs;
    private final SizeValidator sizeValidator;
    private volatile boolean running = true;
    private FlagSocket flagSocket;

    protected JobConf config;
    final FlagFileWriter flagFileWriter;// todo make private again
    private LinkedBlockingQueue<String> socketMessageQueue;

    public FlagMaker(FlagMakerConfig flagMakerConfig) throws IOException {
        this.fmc = flagMakerConfig;
        LOG.info("Flag Maker Config (pre-validate):{}", this.fmc.toString());

        this.config = new JobConf(new Configuration());

        this.fmc.validate();
        LOG.info("Flag Maker Config (post-validate):{}", this.fmc.toString());

        this.fd = createFlagDistributor(this.fmc);
        this.sizeValidator = new SizeValidatorImpl(this.config, fmc);
        this.flagFileWriter = new FlagFileWriter(fmc);
        this.fs = FlagMakerConfigUtility.getHadoopFS(flagMakerConfig);

        socketMessageQueue = new LinkedBlockingQueue<>();
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
            printUsage(System.out);
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
            pw.write("shutdown");
            pw.flush();
        }
    }

    private static void printUsage(PrintStream out) {
        out.println("To run the Flag Maker: ");
        out.println("datawave.ingest.flag.FlagMaker -flagConfig [path to xml config]");
        out.println("Optional arguments:");
        out.println("\t\t-shutdown\tDescription: shuts down the flag maker using configured socketPort");
        out.println("\t\t-baseHDFSDirOverride [HDFS Path]\tDescription: overrides baseHDFSDir in xml");
        out.println("\t\t-extraIngestArgsOverride [extra ingest args]\tDescription: overrides extraIngestArgs value in xml config");
        out.println("\t\t-flagFileDirectoryOverride [local path]\tDescription: overrides flagFileDirectory value in xml config");
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
                    Thread.sleep(fmc.getSleepMilliSecs());
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

        for (FlagDataTypeConfig fc : fmc.getFlagConfigs()) {
            loadFilesForDistributor(fc);
            // todo - test what will happen if we load files for one directory, then add another source,
            // then load files again. will we backtrack?
            while (fd.hasNext(shouldOnlyCreateFullFlags(fc)) && running) {
                Collection<InputFile> inFiles = fd.next(this.sizeValidator);
                if (null == inFiles || inFiles.isEmpty()) {
                    throw new IllegalStateException(
                                    fd.getClass().getName() + " has input files but returned zero candidates for flagging. Please validate configuration");
                }

                flagFileWriter.writeFlagFile(fc, inFiles);
            }

        }
    }

    /**
     * Adds all input files for the data type to the {@link FlagDistributor}.
     *
     * @param fc
     *            flag datatype configuration data
     * @throws IOException
     *             error condition finding files in hadoop
     */
    void loadFilesForDistributor(FlagDataTypeConfig fc) throws IOException {
        fd.loadFiles(fc);
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
        final FileVisitor<java.nio.file.Path> visitor = new SimpleFileVisitor<java.nio.file.Path>() {

            @Override
            public FileVisitResult visitFile(java.nio.file.Path path, BasicFileAttributes attrs) throws IOException {
                if (fileFilter.accept(path.toFile())) {
                    fileCounter.increment();
                }
                return super.visitFile(path, attrs);
            }
        };
        try {
            Files.walkFileTree(Paths.get(fmc.getFlagFileDirectory()), visitor);
        } catch (IOException e) {
            // unable to get a flag count....
            LOG.error("Unable to get flag file count", e);
            return -1;
        }
        return fileCounter.intValue();
    }

    public void update(String s) {
        if ("shutdown".equals(s)) {
            LOG.info("FlagMaker.update received shutdown");
            running = false;
        } else if (s.startsWith("kick")) {
            LOG.info("FlagMaker.update received kick");
            String dataType = s.substring(4).trim();
            for (FlagDataTypeConfig cfg : fmc.getFlagConfigs()) {
                if (cfg.getDataName().equals(dataType)) {
                    LOG.info("Forcing {} to generate flag file", dataType);
                    cfg.setLast(System.currentTimeMillis() - cfg.getTimeoutMilliSecs());
                    break;
                }
            }
        } else {
            LOG.info("FlagMaker.update received unknown command");
        }
    }

    private void startSocketAndReceiver() {
        try {
            flagSocket = new FlagSocket(fmc.getSocketPort(), socketMessageQueue);
            Thread socketThread = new Thread(flagSocket, "Flag_Socket_Thread");
            socketThread.setDaemon(true);
            socketThread.start();

            Thread messageHandlerThread = new Thread(() -> {
                while (running) {
                    try {
                        update(socketMessageQueue.take());
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
