package datawave.ingest.mapreduce.job.writer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.util.RFileUtil;

/**
 * ContextWriter that supports spilling data to a work dir as an rfile, then on cleanup reads and commits all rfiles to the context in sorted order. Spills will
 * be generated on every call to commit()/flush(). All spills will be read and rewritten to the context on cleanup(). Spills will be written to the
 * {@link #WORK_DIR}
 */
public class SpillingSortedContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
    public static final String WORK_DIR = SpillingSortedContextWriter.class.getName() + ".workDir";

    private Path workDirPath;
    private FileSystem fs;

    private int flushCount = 0;

    /**
     * {@link #WORK_DIR} must be configured. The path must be an existing directory or not exist.
     *
     * @param conf
     *            the configuration
     * @param outputTableCounters
     *            flag to determine whether to output table counters
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Configuration conf, boolean outputTableCounters) throws IOException, InterruptedException {
        super.setup(conf, outputTableCounters);

        // get the work directory to store map spills
        String workDir = conf.get(WORK_DIR);

        if (workDir == null) {
            throw new IllegalStateException(WORK_DIR + " must be specified");
        }

        workDirPath = new Path(workDir);
        fs = workDirPath.getFileSystem(conf);

        if (fs.exists(workDirPath) && !fs.isDirectory(workDirPath)) {
            throw new IllegalStateException(WORK_DIR + ": " + workDirPath + " is not a directory");
        }
    }

    /**
     * Sort all entries then spill to the writer for that task attempt id and flushCount, incrementing flushCount.
     *
     * @param entries
     *            entries to flush
     * @param context
     *            the context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context)
                    throws IOException, InterruptedException {
        // return quickly if there are no entries
        if (entries.keySet().size() == 0) {
            return;
        }

        // sort the entries
        TreeMultimap<BulkIngestKey,Value> sortedEntries = TreeMultimap.create(entries);

        Map<String,RFileWriter> writerMap = new HashMap<>();

        try {
            String attempt = context.getTaskAttemptID().toString();
            for (BulkIngestKey bik : sortedEntries.keySet()) {
                String tableName = bik.getTableName().toString();
                RFileWriter writer = writerMap.get(tableName);
                if (writer == null) {
                    writer = getSpillWriter(tableName, attempt, flushCount);
                    writerMap.put(tableName, writer);
                }

                for (Value v : sortedEntries.get(bik)) {
                    writer.append(bik.getKey(), v);
                }
            }
        } finally {
            flushCount++;

            for (RFileWriter writer : writerMap.values()) {
                writer.close();
            }
        }

    }

    /**
     * Create a new spill file at {@link #WORK_DIR}/tableName/taskAttemptId/flushCount.rf
     *
     * @param table
     * @param attempt
     * @param flushCount
     * @return an RFileWriter for the new file
     * @throws IOException
     *             if the path {@link #WORK_DIR}/tableName/taskAttemptId/ cannot be created or {@link #WORK_DIR}/tableName/taskAttemptId/flushCount.rf already
     *             exists
     */
    private RFileWriter getSpillWriter(String table, String attempt, int flushCount) throws IOException {
        Path tableAttemptPath = new Path(workDirPath, table + "/" + attempt);
        if (!fs.exists(tableAttemptPath)) {
            if (!fs.mkdirs(tableAttemptPath)) {
                throw new IOException("Could not create directory for spill " + tableAttemptPath);
            }
        }

        Path spillFile = new Path(tableAttemptPath, flushCount + ".rf");
        if (fs.exists(spillFile)) {
            throw new IllegalStateException("spill file already exists " + spillFile);
        }

        return RFile.newWriter().to(spillFile.toString()).withFileSystem(fs).build();
    }

    /**
     * Read the {@link #WORK_DIR} for candidate table directories. For each directory look for the task attempt inside. If the task attempt is found create a
     * Scanner to read all files in sorted order writing them to the context.
     *
     * All files for an attempt should be deleted after writing to the context
     *
     * @param context
     *            the context writer to clean
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void cleanup(TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException, InterruptedException {
        super.cleanup(context);

        // assemble the flushed pieces and write them to the context
        String attempt = context.getTaskAttemptID().toString();
        for (FileStatus file : fs.listStatus(workDirPath)) {
            if (file.isDirectory()) {
                Text tableName = new Text(file.getPath().getName());
                // check for the attempt as a sub dir
                Path attemptDir = new Path(workDirPath, tableName + "/" + attempt);
                if (fs.exists(attemptDir)) {
                    // create a reader for this directory
                    String globPattern = attemptDir + "/*.rf";
                    FileStatus[] rfiles = fs.globStatus(new Path(globPattern));

                    Scanner scanner = RFileUtil.getRFileScanner(context.getConfiguration(),
                                    Arrays.stream(rfiles).map(rfile -> rfile.getPath().toString()).toArray(String[]::new));
                    scanner.setRange(new Range());

                    Iterator<Map.Entry<Key,Value>> itr = scanner.iterator();
                    while (itr.hasNext()) {
                        Map.Entry<Key,Value> entry = itr.next();
                        // optionally apply reducer?
                        context.write(new BulkIngestKey(tableName, entry.getKey()), entry.getValue());
                    }
                    scanner.close();

                    // cleanup the attempt dir
                    for (FileStatus rfile : rfiles) {
                        fs.delete(rfile.getPath());
                    }
                    fs.delete(attemptDir);
                }
            }
        }
    }
}
