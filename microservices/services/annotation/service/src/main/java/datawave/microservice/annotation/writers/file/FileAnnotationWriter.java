package datawave.microservice.annotation.writers.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.util.v1.AnnotationJsonUtils;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.microservice.annotation.writers.AnnotationWriter;
import lombok.Getter;

/** Writes annotations to files */
public class FileAnnotationWriter implements AnnotationWriter {

    protected final ReentrantLock writeLock = new ReentrantLock(true);

    protected final SimpleDateFormat sdf;
    protected long maxFileLengthMB;
    protected long maxFileAgeSeconds;

    protected FileSystem fileSystem;
    protected Path path;

    protected Path currentFile = null;
    protected Date creationDate = null;

    protected FileAnnotationWriter(Builder<?> builder) throws IOException {
        this.maxFileLengthMB = builder.maxFileLengthMB;
        this.maxFileAgeSeconds = builder.maxFileAgeSeconds;

        Configuration config = new Configuration();

        if (builder.fsConfigResources != null) {
            for (String resource : builder.fsConfigResources) {
                config.addResource(new Path(resource));
            }
        }

        path = new Path(builder.path);

        if (builder.subPath != null) {
            path = new Path(path, builder.subPath);
        }

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(builder.user);
        UserGroupInformation.setLoginUser(ugi);

        fileSystem = FileSystem.get(path.toUri(), config);

        String sdfString = "yyyyMMdd_HHmmss.SSS'.json'";
        if (builder.prefix != null && !builder.prefix.isEmpty()) {
            sdfString = "'" + builder.prefix + "-'" + sdfString;
        }

        this.sdf = new SimpleDateFormat(sdfString);
    }

    @Override
    public Optional<Annotation> write(Annotation annotation) throws Exception {
        // create the audit path if it doesn't exist
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
        }

        // ensure that identifiers are assigned to the annotation, annotationSource and segments if they don't exist.
        Annotation identifiedAnnotation = AnnotationUtils.injectAllHashes(annotation);

        // convert the messages to JSON
        final String annotationJson = AnnotationJsonUtils.annotationToJsonWithIds(identifiedAnnotation) + "\n";

        writeLock.lock();
        try {
            // if the file/stream is null, doesn't exist, or the file is too old/big, create a new file & output stream
            if (currentFile == null || !fileSystem.exists(currentFile) || isFileTooOld() || isFileTooBig()) {
                createNewFile();
            }

            writeAnnotation(annotationJson);
            return Optional.of(identifiedAnnotation);
        } finally {
            writeLock.unlock();
        }
    }

    protected void writeAnnotation(String annotationJson) throws Exception {
        OutputStream appendStream = (fileSystem instanceof LocalFileSystem) ? new FileOutputStream(new File(currentFile.toUri()), true)
                        : fileSystem.append(currentFile);
        appendStream.write(annotationJson.getBytes(StandardCharsets.UTF_8));
        appendStream.close();
    }

    protected void createNewFile() throws IOException {
        // create a new file and output stream
        Date currentDate = new Date();
        currentFile = new Path(path, sdf.format(currentDate));
        FSDataOutputStream outStream = fileSystem.create(currentFile);
        outStream.close();
        creationDate = currentDate;
    }

    protected boolean isFileTooOld() {
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - creationDate.getTime()) >= maxFileAgeSeconds;
    }

    protected boolean isFileTooBig() throws IOException {
        return ((double) fileSystem.getFileStatus(currentFile).getLen() / (1024L * 1024L)) >= maxFileLengthMB;
    }

    @SuppressWarnings("unchecked")
    @Getter
    public static class Builder<T extends Builder<T>> {
        protected String user;
        protected String path;
        protected String subPath;
        protected List<String> fsConfigResources;
        protected String prefix;
        protected Long maxFileLengthMB;
        protected Long maxFileAgeSeconds;

        public Builder() {
            user = "datawave";
            prefix = "audit";
            maxFileLengthMB = 8192L;
            maxFileAgeSeconds = TimeUnit.HOURS.toSeconds(6);
        }

        public T setUser(String user) {
            if (user != null) {
                this.user = user;
            }
            return (T) this;
        }

        public T setPath(String path) {
            if (path != null) {
                this.path = path;
            }
            return (T) this;
        }

        public T setSubPath(String subPath) {
            if (subPath != null) {
                this.subPath = subPath;
            }
            return (T) this;
        }

        public T setFsConfigResources(List<String> fsConfigResources) {
            this.fsConfigResources = fsConfigResources;
            return (T) this;
        }

        public T setPrefix(String prefix) {
            if (prefix != null) {
                this.prefix = prefix;
            }
            return (T) this;
        }

        public T setMaxFileLengthMB(Long maxFileLengthMB) {
            if (maxFileLengthMB != null) {
                this.maxFileLengthMB = maxFileLengthMB;
            }
            return (T) this;
        }

        public T setMaxFileAgeSeconds(Long maxFileAgeSeconds) {
            if (maxFileAgeSeconds != null) {
                this.maxFileAgeSeconds = maxFileAgeSeconds;
            }
            return (T) this;
        }

        public FileAnnotationWriter build() throws IOException, URISyntaxException {
            return new FileAnnotationWriter(this);
        }
    }
}
