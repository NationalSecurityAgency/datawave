package nsa.datawave.poller.manager;

import nsa.datawave.common.cl.OptionBuilder;
import nsa.datawave.ingest.util.IngestFileReport;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.sadun.util.polling.PollManager;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static nsa.datawave.data.hash.UIDConstants.HOST_INDEX_OPT;
import static nsa.datawave.data.hash.UIDConstants.POLLER_INDEX_OPT;
import static nsa.datawave.data.hash.UIDConstants.UID_TYPE_OPT;

/**
 * Poll Manager that requires configuration
 */
public abstract class ConfiguredPollManager implements PollManager {
    
    private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
    private List<IngestFileReport> ingestFileReportList = new ArrayList<>();
    private int csvMaxMilliseconds = 300 * 1000;
    private long lastCsvWrite = System.currentTimeMillis();
    private String provTempDir = null;
    private boolean generateProvenanceReports = false;
    private int csvBatchSize = 1000;
    private String provenanceTransitURIPrefix = null;
    private static final String PROVCONFIG_OPT = "provConfig";
    private Logger log = Logger.getLogger(ConfiguredPollManager.class);
    private int threadIndex = -1;
    
    private class ProvenanceTimerTask implements Runnable {
        public void run() {
            long now = System.currentTimeMillis();
            if ((now - lastCsvWrite) > csvMaxMilliseconds) {
                synchronized (ingestFileReportList) {
                    if (ingestFileReportList.size() > 0) {
                        writeProvenanceReports();
                    } else {
                        lastCsvWrite = now;
                    }
                }
            }
            timer.schedule(this, 60, TimeUnit.SECONDS);
        }
    }
    
    protected void writeProvenanceReports() {
        
        synchronized (ingestFileReportList) {
            if (generateProvenanceReports && ingestFileReportList.size() > 0) {
                Date d = new Date();
                String hostname = null;
                try {
                    hostname = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    log.error(e.getMessage());
                }
                String threadName = Thread.currentThread().getName();
                String provFilename = String.format("%TY%Tm%Td.%Tk%TM%TS.%TL-%s-%s-%s", d, d, d, d, d, d, d, hostname, threadName, getDatatype());
                
                File provenanceFile = new File(provTempDir + File.separator + provFilename + ".csv");
                
                try {
                    log.info("Writing " + ingestFileReportList.size() + " provenance reports to " + provenanceFile);
                    IngestFileReport.writeCsv(provenanceFile, ingestFileReportList);
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
                ingestFileReportList.clear();
            }
            lastCsvWrite = System.currentTimeMillis();
        }
    }
    
    /**
     * @return datatype that this poller is configured for (i.e. foo, bar, etc.) Be sure to call configure first.
     */
    abstract public String getDatatype();
    
    /**
     * Returns configuration parameters for this poll manager
     *
     * @return configuration parameters
     */
    public Options getConfigurationOptions() {
        final OptionBuilder builder = new OptionBuilder();
        final Options opt = new Options();
        
        builder.args = 1;
        builder.type = String.class;
        builder.required = false;
        opt.addOption(builder.create(PROVCONFIG_OPT, "provConfig", "provenance configuration file"));
        
        Option uidType = new Option(UID_TYPE_OPT, UID_TYPE_OPT, true, "UID type configuration (default HashUID)");
        uidType.setRequired(false);
        uidType.setArgs(1);
        uidType.setType(String.class);
        opt.addOption(uidType);
        
        Option hostIndex = new Option(HOST_INDEX_OPT, HOST_INDEX_OPT, true, "Host index value from 0-255, inclusive (only required for uidType SnowflakeUID)");
        hostIndex.setRequired(false);
        hostIndex.setArgs(1);
        hostIndex.setType(String.class);
        opt.addOption(hostIndex);
        
        Option pollerIndex = new Option(POLLER_INDEX_OPT, POLLER_INDEX_OPT, true,
                        "Poller index value from 0-63, inclusive (only required for uidType SnowflakeUID)");
        pollerIndex.setRequired(false);
        pollerIndex.setArgs(1);
        pollerIndex.setType(String.class);
        opt.addOption(pollerIndex);
        
        return opt;
    }
    
    /**
     * Configures the poll manager
     *
     * @param cl
     * @throws Exception
     */
    public void configure(CommandLine cl) throws Exception {
        
        if (null != cl) {
            // configure Provenance
            String provConfigStr = cl.getOptionValue(PROVCONFIG_OPT);
            if (provConfigStr != null) {
                Properties p = new Properties();
                PropertiesConfiguration provConfig = new PropertiesConfiguration();
                provConfig.load(new File(provConfigStr));
                generateProvenanceReports = provConfig.getBoolean("provenance.generate", false);
                log.debug("sendProvenanceReports=" + generateProvenanceReports);
                if (generateProvenanceReports) {
                    provTempDir = provConfig.getString("provenance.tempDir");
                    log.debug("provTempDir=" + provTempDir);
                    csvBatchSize = provConfig.getInt("provenance.csvBatchSize", 1000);
                    log.debug("csvBatchSize=" + csvBatchSize);
                    csvMaxMilliseconds = provConfig.getInt("provenance.csvMaxSeconds", 300) * 1000;
                    log.debug("csvMaxMilliseconds=" + csvMaxMilliseconds);
                    provenanceTransitURIPrefix = provConfig.getString("provenance.provenanceTransitURIPrefix", "");
                    if (provenanceTransitURIPrefix.endsWith("/")) {
                        provenanceTransitURIPrefix = provenanceTransitURIPrefix.substring(0, provenanceTransitURIPrefix.length() - 1);
                    }
                    log.debug("provenanceTransitURIPrefix=" + provenanceTransitURIPrefix);
                    File f = new File(provTempDir);
                    if (f.exists() && !f.isDirectory()) {
                        throw new IllegalStateException("provTempDir: " + provTempDir + " exists, but is not a directory");
                    } else if (!f.exists()) {
                        f.mkdir();
                    }
                    if (csvMaxMilliseconds > 0) {
                        timer.schedule(new ProvenanceTimerTask(), 60, TimeUnit.SECONDS);
                    }
                }
                
                String directories = cl.getOptionValue("d");
                if (directories != null) {
                    String split[] = directories.split(",");
                    if (split.length > 0)
                        provenanceTransitURIPrefix = provenanceTransitURIPrefix + split[0];
                }
                
                if (provenanceTransitURIPrefix.endsWith(File.separator) == false) {
                    provenanceTransitURIPrefix = provenanceTransitURIPrefix + File.separator;
                }
            }
        }
    }
    
    protected void addReceivedFileReport(String originalFile, File workFile) {
        
        if (generateProvenanceReports) {
            synchronized (ingestFileReportList) {
                IngestFileReport receivedFileReport = new IngestFileReport();
                String transitStr = null;
                if (StringUtils.isBlank(provenanceTransitURIPrefix)) {
                    transitStr = originalFile;
                } else {
                    transitStr = provenanceTransitURIPrefix + originalFile;
                }
                
                URI transitURI = null;
                try {
                    transitURI = new URI(transitStr);
                    receivedFileReport.setTransitURI(transitURI.toString());
                    log.info("set transitURI:" + transitURI.toString());
                } catch (URISyntaxException e) {
                    log.error(e.getMessage(), e);
                }
                receivedFileReport.setFileName(new File(originalFile).getName());
                receivedFileReport.setFileSize(workFile.length());
                receivedFileReport.setEventTimestamp(new Date(workFile.lastModified()));
                receivedFileReport.setStatus(IngestFileReport.Status.RECEIVE);
                ingestFileReportList.add(receivedFileReport);
                
                if (ingestFileReportList.size() >= csvBatchSize) {
                    writeProvenanceReports();
                }
            }
        }
    }
    
    protected void addSpawnFileReport(String originalFile, File workFile, Collection<String> outputFiles) {
        
        if (generateProvenanceReports) {
            synchronized (ingestFileReportList) {
                for (String outputFile : outputFiles) {
                    IngestFileReport spawnFileReport = new IngestFileReport();
                    spawnFileReport.addSourceFile(new File(originalFile).getName());
                    spawnFileReport.setFileName(outputFile);
                    spawnFileReport.setStatus(IngestFileReport.Status.SPAWN);
                    spawnFileReport.setEventTimestamp(new Date());
                    ingestFileReportList.add(spawnFileReport);
                }
                
                if (ingestFileReportList.size() >= csvBatchSize) {
                    writeProvenanceReports();
                }
            }
        }
    }
    
    /**
     * Releases resources associated with this poll manager
     *
     * @throws IOException
     */
    public void close() throws IOException {
        timer.shutdown();
        try {
            timer.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Terminated timer with tasks still executing.");
        }
    }
    
    /**
     * Returns the assigned thread index of this manager
     *
     * @return the assigned thread index of this manager, or -1 if not assigned
     */
    public int getThreadIndex() {
        return threadIndex;
    }
    
    /**
     * Assigns the thread index of this manager. If the specified value is negative, it will be given the value of -1.
     *
     * @param threadIndex
     *            the thread index to assign to this manager
     */
    public void setThreadIndex(int threadIndex) {
        if (threadIndex < 0) {
            threadIndex = -1;
        }
        this.threadIndex = threadIndex;
    }
    
}
