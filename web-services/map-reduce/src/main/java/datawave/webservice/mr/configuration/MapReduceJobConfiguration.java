package datawave.webservice.mr.configuration;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.results.mr.MapReduceJobDescription;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class MapReduceJobConfiguration {

    private Logger log = LoggerFactory.getLogger(getClass());

    private String description = null;
    protected String hdfsUri = null;
    protected String jobTracker = null;
    protected String queueName = null;
    protected List<String> classpathJarFiles = null;
    protected String jobJarName = null;
    protected Map<String,Class<?>> requiredParameters = null;
    protected Map<String,Class<?>> requiredRuntimeParameters = null;
    protected Map<String,Class<?>> optionalRuntimeParameters = null;
    private List<String> requiredRoles = null;
    private List<String> requiredAuths = null;
    protected Map<String,Object> jobConfigurationProperties = null;
    protected Map<String,String> jobSystemProperties = null;

    protected String callbackServletURL = null;
    protected String mapReduceBaseDirectory = null;
    protected Path baseDir = null;
    protected Path jobDir = null;
    protected Path resultsDir = null;

    protected String jobType = "mapreduce";

    public String getDescription() {
        return this.description;
    }

    public String getHdfsUri() {
        return hdfsUri;
    }

    public String getJobTracker() {
        return jobTracker;
    }

    public List<String> getClasspathJarFiles() {
        return classpathJarFiles;
    }

    public String getJobJarName() {
        return jobJarName;
    }

    public Map<String,Class<?>> getRequiredRuntimeParameters() {
        return requiredRuntimeParameters;
    }

    public Map<String,Class<?>> getOptionalRuntimeParameters() {
        return optionalRuntimeParameters;
    }

    public Map<String,Object> getJobConfigurationProperties() {
        return jobConfigurationProperties;
    }

    public Map<String,String> getJobSystemProperties() {
        return jobSystemProperties;
    }

    public List<String> getRequiredRoles() {
        return this.requiredRoles;
    }

    public void setDescription(String desc) {
        this.description = desc;
    }

    public void setHdfsUri(String hdfsUri) {
        this.hdfsUri = hdfsUri;
    }

    public void setJobTracker(String jobTracker) {
        this.jobTracker = jobTracker;
    }

    public void setClasspathJarFiles(List<String> classpathJarFiles) {
        this.classpathJarFiles = classpathJarFiles;
    }

    public void setJobJarName(String jobJarName) {
        this.jobJarName = jobJarName;
    }

    public void setRequiredAuths(List<String> requiredAuths) {
        this.requiredAuths = requiredAuths;
    }

    public List<String> getRequiredAuths() {
        return requiredAuths;
    }

    public void setRequiredParameters(Map<String,Class<?>> requiredParameters) {
        this.requiredParameters = requiredParameters;
    }

    public Map<String,Class<?>> getRequiredParameters() {
        return requiredParameters;
    }

    public void setRequiredRuntimeParameters(Map<String,Class<?>> requiredRuntimeParameters) {
        this.requiredRuntimeParameters = requiredRuntimeParameters;
    }

    public void setOptionalRuntimeParameters(Map<String,Class<?>> optionalRuntimeParameters) {
        this.optionalRuntimeParameters = optionalRuntimeParameters;
    }

    public void setJobConfigurationProperties(Map<String,Object> jobConfigurationProperties) {
        this.jobConfigurationProperties = jobConfigurationProperties;
    }

    public void setJobSystemProperties(Map<String,String> jobSystemProperties) {
        this.jobSystemProperties = jobSystemProperties;
    }

    public void setRequiredRoles(List<String> roles) {
        this.requiredRoles = roles;
    }

    public String getCallbackServletURL() {
        return callbackServletURL;
    }

    public String getMapReduceBaseDirectory() {
        return mapReduceBaseDirectory;
    }

    public void setCallbackServletURL(String callbackServletURL) {
        this.callbackServletURL = callbackServletURL;
    }

    public void setMapReduceBaseDirectory(String mapReduceBaseDirectory) {
        this.mapReduceBaseDirectory = mapReduceBaseDirectory;
    }

    public String getJobDir() {
        return this.jobDir.toString();
    }

    public String getResultsDir() {
        return this.resultsDir.toString();
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getJobType() {
        return jobType;
    }

    /**
     * @param conf
     *            config
     * @return HDFS FileSystem
     * @throws IOException
     *             for IOException
     */
    protected final FileSystem getFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    /**
     * Common code to setup distributed cache and classpath for the job
     *
     * @param job
     *            the job
     * @param jobDir
     *            job directory
     * @param jobId
     *            the job id
     * @throws Exception
     *             if something goes wrong
     */
    protected void prepareClasspath(String jobId, Job job, Path jobDir) throws Exception {
        FileSystem fs = getFileSystem(job.getConfiguration());
        if (!fs.exists(baseDir))
            if (!fs.mkdirs(baseDir)) {
                throw new QueryException(DatawaveErrorCode.DFS_DIRECTORY_CREATE_ERROR, MessageFormat.format("Directory: {0}", baseDir.toString()));
            }

        // Create the directory for this job
        if (!fs.exists(jobDir))
            if (!fs.mkdirs(jobDir)) {
                throw new QueryException(DatawaveErrorCode.DFS_DIRECTORY_CREATE_ERROR, MessageFormat.format("Directory: {0}", jobDir.toString()));
            }

        // Create classpath directory for this job
        Path classpath = new Path(jobDir, "classpath");
        if (!fs.exists(classpath))
            if (!fs.mkdirs(classpath)) {
                throw new QueryException(DatawaveErrorCode.DFS_DIRECTORY_CREATE_ERROR, MessageFormat.format("Directory: {0}", classpath.toString()));
            }

        // Add all of the jars to the classpath dir and to the DistributedCache
        for (String jarFile : this.classpathJarFiles) {
            int idx = jarFile.indexOf('!');
            Pattern pattern = null;
            if (idx > 0) {
                pattern = Pattern.compile(jarFile.substring(idx + 1));
                jarFile = jarFile.substring(0, idx);
            }

            File file = new File(jarFile);
            if (pattern == null) {
                Path cachedJarPath = new Path(classpath, file.getName());
                addSingleFile(jarFile, cachedJarPath, jobId, job, fs);
            } else {
                // Jars within the deployed EAR in Wildfly use the VFS protocol, and need to be handled specially.
                if (jarFile.startsWith("vfs:")) {
                    Set<String> files = new LinkedHashSet<>();
                    try (InputStream urlInputStream = new URL(jarFile).openStream()) {
                        JarInputStream jarInputStream = (JarInputStream) urlInputStream;
                        for (JarEntry jarEntry = jarInputStream.getNextJarEntry(); jarEntry != null; jarEntry = jarInputStream.getNextJarEntry()) {
                            if (pattern.matcher(jarEntry.getName()).matches()) {
                                String name = jarEntry.getName();
                                if (name.endsWith("/"))
                                    name = name.substring(0, name.length() - 1);
                                files.add(jarFile + "/" + name);
                            }
                        }
                    }
                    for (String nestedFile : files) {
                        Path cachedJarPath = new Path(classpath, new File(nestedFile).getName());
                        addSingleFile(nestedFile, cachedJarPath, jobId, job, fs);
                    }
                } else if (jarFile.startsWith("archive:")) {
                    jarFile = jarFile.substring("archive:".length());
                    addArchivedDirectory(jarFile, pattern, classpath, jobId, job, fs);
                } else {
                    addArchiveFile(file, pattern, classpath, jobId, job, fs);
                }
            }
        }

        String homeDir = System.getProperty("jboss.home.dir");
        // Add all of the jars in the server lib directory
        File libDir = new File(homeDir, "bin/client");
        if (!(libDir.isDirectory() && libDir.canRead())) {
            throw new QueryException(DatawaveErrorCode.DFS_DIRECTORY_READ_ERROR, MessageFormat.format("directory: {0}", libDir));
        }
        FilenameFilter jarFilter = (dir, name) -> name.toLowerCase().endsWith(".jar");
        File[] jarFiles = libDir.listFiles(jarFilter);
        if (jarFiles != null) {
            for (File jar : jarFiles) {
                // remove guava classes from jboss-client.jar
                if (jar.getName().equals("jboss-client.jar")) {
                    List<Pattern> patterns = new ArrayList<>();
                    patterns.add(Pattern.compile("^com/google.*"));
                    patterns.add(Pattern.compile("^META-INF/maven/com.google.guava.*"));
                    File filteredJar = filterJar(jar, patterns);
                    addSingleFile(filteredJar, new Path(classpath, jar.getName()), jobId, job, fs);
                    filteredJar.delete();
                } else {
                    addSingleFile(jar, new Path(classpath, jar.getName()), jobId, job, fs);
                }
            }
        }

        // Add all of the jars in the server mapreduce helper lib directory
        libDir = new File(homeDir, "tools/mapreduce/lib");
        if (!(libDir.isDirectory() && libDir.canRead())) {
            throw new QueryException(DatawaveErrorCode.DFS_DIRECTORY_READ_ERROR, MessageFormat.format("directory: {0}", libDir));
        }
        jarFiles = libDir.listFiles(jarFilter);
        if (jarFiles != null) {
            for (File jar : jarFiles) {
                addSingleFile(jar, new Path(classpath, jar.getName()), jobId, job, fs);
            }
        }
        exportSystemProperties(jobId, job, fs, classpath);
    }

    private File filterJar(File source, List<Pattern> patterns) {
        File f = null;
        try {
            f = File.createTempFile(source.getName() + ".", "");
            try (FileOutputStream fos = new FileOutputStream(f); ZipOutputStream zipOutputStream = new ZipOutputStream(fos)) {
                try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(source))) {
                    for (ZipEntry zipEntry = zipInputStream.getNextEntry(); zipEntry != null; zipEntry = zipInputStream.getNextEntry()) {
                        final String entryName = zipEntry.getName();
                        if (patterns.stream().noneMatch(p -> p.matcher(entryName).matches())) {
                            zipOutputStream.putNextEntry(zipEntry);
                            ByteStreams.copy(zipInputStream, zipOutputStream);
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return f;
    }

    protected void addSingleFile(File source, Path destination, String jobId, Job job, FileSystem fs) throws IOException {
        Path jarPath = new Path(source.getAbsolutePath());
        try {
            fs.copyFromLocalFile(false, false, jarPath, destination);
        } catch (IOException e) {
            // If the file already exists, ignore error
            if (!e.getMessage().endsWith("already exists"))
                throw e;
        }
        log.trace("Adding {} to the classpath for job {}.", jarPath, jobId);
        job.addFileToClassPath(destination);
    }

    protected void addSingleFile(String source, Path destination, String jobId, Job job, FileSystem fs) throws IOException {
        try (FSDataOutputStream hadoopOutputStream = fs.create(destination, false); InputStream urlInputStream = new URL(source).openStream()) {

            // Copy raw file to hadoop
            if (!(urlInputStream instanceof JarInputStream)) {
                ByteStreams.copy(urlInputStream, hadoopOutputStream);
            }
            // Copy jar file to hadoop - Wildfly VFS returns files as JarInputStreams
            else {
                JarInputStream jarInputStream = (JarInputStream) urlInputStream;
                try (JarOutputStream jarOutputStream = new JarOutputStream(hadoopOutputStream)) {
                    for (JarEntry jarEntry = jarInputStream.getNextJarEntry(); jarEntry != null; jarEntry = jarInputStream.getNextJarEntry()) {
                        jarOutputStream.putNextEntry(jarEntry);
                        ByteStreams.copy(urlInputStream, jarOutputStream);
                    }
                }
            }

            // Add the jar to the job classpath
            log.trace("Adding {} to the classpath for job {}", source, jobId);
            job.addFileToClassPath(destination);
        } catch (IOException e) {
            // If the file already exists, ignore error
            if (!e.getMessage().endsWith("already exists"))
                throw e;
        }
    }

    private void addArchiveFile(File source, Pattern pattern, Path classpath, String jobId, Job job, FileSystem fs) throws IOException {
        if (!source.isFile() || !source.canRead()) {
            throw new IOException(source + " is not a regular file.");
        }
        try (JarInputStream jarInputStream = new JarInputStream(new FileInputStream(source))) {
            // Check each file in the archive, and if it matches the supplied pattern, then copy it to HDFS and add it to the classpath.
            for (JarEntry jarEntry = jarInputStream.getNextJarEntry(); jarEntry != null; jarEntry = jarInputStream.getNextJarEntry()) {
                if (pattern.matcher(jarEntry.getName()).matches()) {
                    log.trace("Adding {} to the classpath for job {}", jarEntry.getName(), jobId);
                    int slashIdx = jarEntry.getName().lastIndexOf('/');
                    String outputFileName = jarEntry.getName().substring(slashIdx + 1);
                    Path cachedJarPath = new Path(classpath, outputFileName);
                    try (FSDataOutputStream hadoopOutputStream = fs.create(cachedJarPath, false)) {
                        ByteStreams.copy(jarInputStream, hadoopOutputStream);
                    }
                    job.addFileToClassPath(cachedJarPath);
                } else {
                    log.trace("Skipping {} since it does not match the pattern {}", jarEntry.getName(), pattern.pattern());
                }
            }
        }
    }

    /**
     * Takes a source directory and archives the entire directory tree underneath it into a file in the job cache. This is used to bundle the configuration
     * module into the job cache so its files are available for MapReduce jobs.
     *
     * @param source
     *            a source
     * @param fs
     *            a filesystem
     * @param classpath
     *            a classpath
     * @param job
     *            a job
     * @param jobId
     *            a jobid
     * @param pattern
     *            a pattern
     * @throws IOException
     *             for input/output issues
     */
    private void addArchivedDirectory(String source, Pattern pattern, Path classpath, String jobId, Job job, FileSystem fs) throws IOException {
        File sourceDir = new File(source);
        if (!sourceDir.isDirectory() || !sourceDir.canRead()) {
            throw new IOException(source + " is not a readable directory.");
        }

        int fileCount = 0;
        String outputName = source.replaceAll("/", "_").replaceFirst("$_", "") + ".jar";
        Path destination = new Path(classpath, outputName);
        try (FSDataOutputStream hadoopOutputStream = fs.create(destination, false); JarOutputStream jarOutputStream = new JarOutputStream(hadoopOutputStream)) {
            boolean first = true;
            for (File candidate : Files.fileTraverser().depthFirstPreOrder(sourceDir)) {
                // Skip the first node since it's the root node of the tree and equates to the directory "/" in the jar, which we don't want to add.
                if (first) {
                    first = false;
                    continue;
                }
                // Add directory entries
                if (candidate.isDirectory()) {
                    String relativeDirName = candidate.getAbsolutePath().substring(source.length());
                    if (!relativeDirName.endsWith("/"))
                        relativeDirName += "/";
                    ZipEntry ze = new ZipEntry(relativeDirName);
                    ze.setTime(candidate.lastModified());
                    jarOutputStream.putNextEntry(ze);
                    jarOutputStream.closeEntry();
                } else {
                    // Add file entries
                    String relativeName = candidate.getAbsolutePath().substring(source.length());
                    if (pattern.matcher(relativeName).matches()) {
                        log.trace("Adding {} to jar file {}", candidate, outputName);
                        ZipEntry ze = new ZipEntry(relativeName);
                        ze.setTime(candidate.lastModified());
                        jarOutputStream.putNextEntry(ze);
                        Files.copy(candidate, jarOutputStream);
                        jarOutputStream.closeEntry();
                        ++fileCount;
                    } else {
                        log.trace("Skipping {} from archive {} with pattern {}", candidate, source, pattern.pattern());
                    }
                }
            }
        }
        if (fileCount > 0) {
            log.trace("Adding {} to the classpath for job {}", outputName, jobId);
            job.addFileToClassPath(destination);
        }
    }

    protected void exportSystemProperties(String jobId, Job job, FileSystem fs, Path classpath) {
        Properties systemProperties = new Properties();
        systemProperties.putAll(System.getProperties());
        if (this.jobSystemProperties != null) {
            systemProperties.putAll(this.jobSystemProperties);
        }
        writeProperties(jobId, job, fs, classpath, systemProperties);
    }

    protected void writeProperties(String jobId, Job job, FileSystem fs, Path classpath, Properties properties) {

        File f = null;
        try {
            f = File.createTempFile(jobId, ".properties");
            try (FileOutputStream fos = new FileOutputStream(f)) {
                properties.store(fos, "");
            }
            addSingleFile(f, new Path(classpath, "embedded-configuration.properties"), jobId, job, fs);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (f != null) {
                f.delete();
            }
        }
    }

    public void initializeConfiguration(String jobId, Job job, Map<String,String> runtimeParameters, DatawavePrincipal serverPrincipal) throws Exception {

        // Validate the required runtime parameters exist
        if (null != this.requiredRuntimeParameters && !this.requiredRuntimeParameters.isEmpty() && null != runtimeParameters && !runtimeParameters.isEmpty()) {
            // Loop over the required runtime parameter names and make sure an entry exists in the method parameter
            for (String parameter : this.requiredRuntimeParameters.keySet()) {
                if (!runtimeParameters.containsKey(parameter))
                    throw new IllegalArgumentException("Required runtime parameter '" + parameter + "' must be set");
            }
        }

        if (StringUtils.isEmpty(this.hdfsUri))
            throw new IllegalArgumentException("HDFS URI must be set");
        job.getConfiguration().set("fs.defaultFS", this.hdfsUri);

        if (StringUtils.isEmpty(this.jobTracker))
            throw new IllegalArgumentException("Job Tracker must be set");
        job.getConfiguration().set("mapreduce.jobtracker.address", this.jobTracker);

        if (StringUtils.isEmpty(this.jobJarName))
            throw new IllegalArgumentException("Job jar name must be set");
        job.getConfiguration().set("job.jar", this.jobJarName);

        // Set the job.end.notification.url parameter for the job. The MapReduce framework will call back to this servlet
        // when the MapReduce job succeeds/fails. For deploying on a cluster, the job.end.retry.attempts and job.end.retry.interval
        // parameters need to be set in the Hadoop mapred config file.
        job.getConfiguration().set("mapreduce.job.end-notification.url", this.callbackServletURL);

        this.baseDir = new Path(this.mapReduceBaseDirectory);

        // Create a directory path for this job
        this.jobDir = new Path(this.baseDir, jobId);

        // Create a results directory path
        this.resultsDir = new Path(this.jobDir, "results");

        // Set up the classpath
        prepareClasspath(jobId, job, jobDir);

        // Add any configuration properties set in the config
        for (Entry<String,Object> entry : this.getJobConfigurationProperties().entrySet()) {
            String key = entry.getKey();
            Object object = entry.getValue();
            if (object instanceof String) {
                job.getConfiguration().set(key, (String) object);
            } else if (object instanceof Map) {
                StringBuilder sb = new StringBuilder();
                Map<String,String> valueMap = (Map<String,String>) object;
                for (Map.Entry<String,String> valueEntry : valueMap.entrySet()) {
                    if (sb.length() > 0) {
                        sb.append(" ");
                    }
                    if (StringUtils.isBlank(valueEntry.getValue())) {
                        sb.append(valueEntry.getKey());
                    } else {
                        sb.append(valueEntry.getKey()).append("=").append(valueEntry.getValue());
                    }
                    job.getConfiguration().set(key, sb.toString());
                }
            }
        }
        _initializeConfiguration(job, jobDir, jobId, runtimeParameters, serverPrincipal);
    }

    // Subclasses will override this method
    protected void _initializeConfiguration(Job job, Path jobDir, String jobId, Map<String,String> runtimeParameters, DatawavePrincipal serverPrincipal)
                    throws Exception {

    }

    protected MapReduceJobDescription getConfigurationDescription(String name, MapReduceJobDescription desc) {

        desc.setName(name);
        desc.setJobType(this.getJobType());
        desc.setDescription(this.getDescription());
        List<String> required = new ArrayList<>();
        required.addAll(this.getRequiredRuntimeParameters().keySet());
        desc.setRequiredRuntimeParameters(required);
        List<String> optional = new ArrayList<>();
        optional.addAll(this.getOptionalRuntimeParameters().keySet());
        desc.setOptionalRuntimeParameters(optional);
        return desc;

    }

    public MapReduceJobDescription getConfigurationDescription(String name) {
        MapReduceJobDescription desc = new MapReduceJobDescription();
        return getConfigurationDescription(name, desc);
    }
}
