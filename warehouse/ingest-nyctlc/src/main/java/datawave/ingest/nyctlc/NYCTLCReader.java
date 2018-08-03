package datawave.ingest.nyctlc;

import datawave.ingest.csv.mr.input.CSVReaderBase;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.input.reader.LfLineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * This is a specialized version of the CSV Reader intended to be used with the NYC Taxi & Limousine Commission dataset. This CSV reader reads the first line of
 * the CSV in order to determine what fields are present in the data. Then, each line of the CSV is modified to include a mapping of the field name (as defined
 * in the header) to each applicable value. These key/value pairings are conceptually treated as 'extra fields'.
 */
public class NYCTLCReader extends CSVReaderBase {
    
    private String rawHeader;
    
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        super.initialize(genericSplit, context);
        Configuration job = context.getConfiguration();
        
        // open the file and seek to the start
        final Path file = ((FileSplit) genericSplit).getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(file);
        
        // read the header from the first line
        Text header = new Text();
        LfLineReader in = new LfLineReader(fileIn);
        in.readLine(header);
        in.close();
        
        rawHeader = header.toString();
        ((NYCTLCHelper) helper).parseHeader(rawHeader);
    }
    
    /** Points the RecordReader to the next record. */
    @Override
    public boolean nextKeyValue() throws IOException {
        // The format of the file is such that there is a header,
        // followed by a blank line, followed by our entries
        // This is here to account for that
        boolean hasNext, completeRecord;
        do {
            hasNext = super.nextKeyValue();
            
            if (this.value != null && !this.value.toString().isEmpty() && !this.value.toString().equals(rawHeader)) {
                // update value to be list of field/value pairings
                StringBuffer fvBuf = new StringBuffer();
                String[] values = this.value.toString().split(((NYCTLCHelper) helper).getSeparator());
                if (values.length == ((NYCTLCHelper) helper).getParsedHeader().length) {
                    completeRecord = true;
                    for (int fieldIdx = 0; fieldIdx < values.length; fieldIdx++) {
                        fvBuf.append(((NYCTLCHelper) helper).getParsedHeader()[fieldIdx] + "=" + values[fieldIdx]);
                        if ((fieldIdx + 1) < values.length)
                            fvBuf.append(((NYCTLCHelper) helper).getSeparator());
                    }
                    this.value = new Text(fvBuf.toString());
                } else
                    completeRecord = false;
            } else
                completeRecord = false;
        } while (hasNext && !completeRecord);
        
        return hasNext;
    }
    
    /** Gets the Event and this RecordReader ready for reading. */
    @Override
    public void initializeEvent(Configuration conf) throws IOException {
        super.initializeEvent(conf);
    }
    
    @Override
    public RawRecordContainer getEvent() {
        RawRecordContainer e = super.getEvent();
        return e;
    }
    
    /** Creates a NYCTLCHelper for the RecordReader. */
    @Override
    protected NYCTLCHelper createHelper(Configuration conf) {
        return new NYCTLCHelper();
    }
}
