package datawave.ingest.input.reader;

import java.io.IOException;

import com.google.common.base.Preconditions;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.util.TextUtil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class aggregates Text values based on a start and end filter. An example use case for this would be XML data. This will not work with data that has
 * nested start and stop tokens
 * 
 */
public class AggregatingRecordReader extends LongLineEventRecordReader implements PositionAwareLineReader {
    
    public static final String START_TOKEN = "aggregating.token.start";
    public static final String END_TOKEN = "aggregating.token.end";
    public static final String RETURN_PARTIAL_MATCHES = "aggregating.allow.partial";
    
    private ReaderDelegate delegate = null;
    
    public AggregatingRecordReader() {
        delegate = new ReaderDelegate();
        delegate.setPositionAwareLineReader(this);
        delegate.setNextKeyValueReader(this::readSuperNextKeyValue);
        delegate.setCurrentValueReader(this::readSuperCurrentValue);
    }
    
    private Text readSuperCurrentValue() {
        return super.getCurrentValue();
    }
    
    private boolean readSuperNextKeyValue() throws IOException {
        return super.nextKeyValue();
    }
    
    @Override
    public LongWritable getCurrentKey() {
        delegate.setKeyVal(delegate.getCounter());
        return delegate.getKey();
    }
    
    @Override
    public Text getCurrentValue() {
        return delegate.getAggValue();
    }
    
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        super.initialize(genericSplit, context);
        delegate.initialize(context);
    }
    
    @Override
    public boolean nextKeyValue() throws IOException {
        return delegate.nextKeyValue();
    }
    
    /**
     * Encapsulates all the "aggregating" logic out-of-band from the established inheritance hierarchy so that clients can extend/inherit whatever suits their
     * needs
     */
    public static class ReaderDelegate {
        
        private LongWritable key = new LongWritable();
        private String startToken = null;
        private String endToken = null;
        private long counter = 0;
        private Text aggValue = new Text();
        private boolean startFound = false;
        private StringBuilder remainder = new StringBuilder(0);
        private boolean returnPartialMatches = false;
        
        private PositionAwareLineReader positionAwareLineReader;
        private KeyValueReader nextKeyValueReader;
        private ValueReader currentValueReader;
        
        public void setPositionAwareLineReader(PositionAwareLineReader positionAwareLineReader) {
            this.positionAwareLineReader = positionAwareLineReader;
        }
        
        public void setNextKeyValueReader(KeyValueReader nextKeyValueReader) {
            this.nextKeyValueReader = nextKeyValueReader;
        }
        
        public void setCurrentValueReader(ValueReader currentValueReader) {
            this.currentValueReader = currentValueReader;
        }
        
        public void incrementCounter() {
            counter++;
        }
        
        public LongWritable getKey() {
            return key;
        }
        
        public long getKeyVal() {
            return key.get();
        }
        
        public void setKey(LongWritable key) {
            this.key = key;
        }
        
        public void setKeyVal(long key) {
            this.key.set(key);
        }
        
        public String getStartToken() {
            return startToken;
        }
        
        public void setStartToken(String startToken) {
            this.startToken = startToken;
        }
        
        public String getEndToken() {
            return endToken;
        }
        
        public void setEndToken(String endToken) {
            this.endToken = endToken;
        }
        
        public long getCounter() {
            return counter;
        }
        
        public void setCounter(long counter) {
            this.counter = counter;
        }
        
        public Text getAggValue() {
            return aggValue;
        }
        
        public void setAggValue(Text aggValue) {
            this.aggValue = aggValue;
        }
        
        public boolean isStartFound() {
            return startFound;
        }
        
        public void setStartFound(boolean startFound) {
            this.startFound = startFound;
        }
        
        public StringBuilder getRemainder() {
            return remainder;
        }
        
        public void setRemainder(StringBuilder remainder) {
            this.remainder = remainder;
        }
        
        public boolean isReturnPartialMatches() {
            return returnPartialMatches;
        }
        
        public void setReturnPartialMatches(boolean returnPartialMatches) {
            this.returnPartialMatches = returnPartialMatches;
        }
        
        public void initialize(TaskAttemptContext context) {
            this.startToken = ConfigurationHelper.isNull(context.getConfiguration(), START_TOKEN, String.class);
            this.endToken = ConfigurationHelper.isNull(context.getConfiguration(), END_TOKEN, String.class);
            this.returnPartialMatches = context.getConfiguration().getBoolean(RETURN_PARTIAL_MATCHES, false);
            /*
             * Text-appending works almost exactly like the + operator on Strings- it creates a byte array exactly the size of [prefix + suffix] and dumps the
             * bytes into the new array. This module works by doing lots of little additions, one line at a time. With most XML, the documents are partitioned
             * on line boundaries, so we will generally have lots of additions. Setting a large default byte array for a text object can avoid this and give us
             * StringBuilder-like functionality for Text objects.
             */
            byte[] txtBuffer = new byte[2048];
            aggValue.set(txtBuffer);
        }
        
        public boolean nextKeyValue() throws IOException {
            Preconditions.checkNotNull(nextKeyValueReader, "nextKeyValueReader cannot be null");
            Preconditions.checkNotNull(currentValueReader, "currentValueReader cannot be null");
            
            aggValue.clear();
            boolean hasNext = false;
            boolean finished;
            // Find the start token
            while ((hasNext = nextKeyValueReader.readKeyValue()) || remainder.length() > 0) {
                if (hasNext)
                    finished = process(currentValueReader.readValue());
                else
                    finished = process(null);
                if (finished) {
                    startFound = false;
                    counter++;
                    return true;
                }
            }
            
            // Check to see if we can catch the trailing final record
            if (nextKeyValuePastBlock(hasNext)) {
                return true;
            }
            
            // If we have anything loaded in the agg value (and we found a start)
            // then we ran out of data before finding the end. Just return the
            // data we have and if it's not valid, downstream parsing of the data
            // will fail.
            if (returnPartialMatches && startFound && aggValue.getLength() > 0) {
                startFound = false;
                counter++;
                return true;
            }
            return false;
        }
        
        /**
         * Populates aggValue with the contents of the Text object.
         *
         * @param t
         *            text object to populate
         * @return true if aggValue is complete, else false and needs more data.
         */
        private boolean process(Text t) {
            
            if (null != t)
                remainder.append(t);
            while (remainder.length() > 0) {
                if (!startFound) {
                    // If found, then begin aggregating at the start offset
                    int start = remainder.indexOf(startToken);
                    if (-1 != start) {
                        // Append the start token to the aggregate value
                        TextUtil.textAppendNoNull(aggValue, remainder.substring(start, start + startToken.length()), false);
                        // Remove to the end of the start token from the remainder
                        remainder.delete(0, start + startToken.length());
                        startFound = true;
                    } else {
                        // If we are looking for the start and have not found it,
                        // then remove
                        // the bytes
                        remainder.delete(0, remainder.length());
                    }
                } else {
                    // Try to find the end
                    int end = remainder.indexOf(endToken);
                    // Also try to find the start
                    int start = remainder.indexOf(startToken);
                    if (-1 == end) {
                        if (returnPartialMatches && start >= 0) {
                            // End token not found, but another start token was
                            // found...
                            // The amount to copy is up to the beginning of the next
                            // start token
                            TextUtil.textAppendNoNull(aggValue, remainder.substring(0, start), false);
                            remainder.delete(0, start);
                            return true;
                        } else {
                            // Not found, aggregate the entire remainder
                            TextUtil.textAppendNoNull(aggValue, remainder.toString(), false);
                            // Delete all chars from remainder
                            remainder.delete(0, remainder.length());
                        }
                    } else {
                        if (returnPartialMatches && start >= 0 && start < end) {
                            // We found the end token, but found another start token
                            // first, so
                            // deal with that.
                            TextUtil.textAppendNoNull(aggValue, remainder.substring(0, start), false);
                            remainder.delete(0, start);
                            return true;
                        } else {
                            // END_TOKEN was found. Extract to the end of END_TOKEN
                            TextUtil.textAppendNoNull(aggValue, remainder.substring(0, end + endToken.length()), false);
                            // Remove from remainder up to the end of END_TOKEN
                            remainder.delete(0, end + endToken.length());
                            return true;
                        }
                    }
                }
            }
            return false;
        }
        
        protected boolean nextKeyValuePastBlock(boolean hasNext) throws IOException {
            Preconditions.checkNotNull(positionAwareLineReader, "positionAwareLineReader cannot be null");
            
            // If we're in the middle of an element
            // have "run out" of data in the current InputSplit
            // if aggValue is empty (meaning, we just cleared it out and are trying to find a new record)
            // and we got to this point, we do not want to read into the next block
            if (aggValue.getLength() > 0 && positionAwareLineReader.getPos() > positionAwareLineReader.getEnd()) {
                int end = aggValue.find(endToken);
                int prevLength = aggValue.getLength();
                Text endRecordFromNextBlock = new Text();
                
                // We want to loop until we can find an end token to match the start token we already have
                while (end == -1) {
                    endRecordFromNextBlock.clear();
                    
                    int newSize = positionAwareLineReader.getLfLineReader().readLine(endRecordFromNextBlock, positionAwareLineReader.getMaxLineLength(),
                                    Integer.MAX_VALUE);
                    if (0 == newSize) {
                        // This fails in the same manner as the process(Text) method does for
                        // self-closing XML elements.
                        return returnPartialMatches && aggValue.getLength() > 0;
                    }
                    
                    // Track the extra data read
                    positionAwareLineReader.setPos(positionAwareLineReader.getPos() + newSize);
                    
                    // Find the start and end in this next segment read
                    int newStart = endRecordFromNextBlock.find(startToken), newEnd = endRecordFromNextBlock.find(endToken);
                    
                    // We found no start, but did find an end
                    if (newStart == -1 && newEnd >= 0) {
                        // Append onto the aggValue, and we're done.
                        TextUtil.textAppendNoNull(aggValue, Text.decode(endRecordFromNextBlock.getBytes(), 0, newEnd + endToken.length()));
                        return true;
                    } else if (newStart < newEnd) {
                        // We found another start token before an endtoken which
                        // would imply malformed XML
                        
                        if (returnPartialMatches) {
                            // Let's try to be nice and throw everything up until the new start character into the aggregated value.
                            TextUtil.textAppendNoNull(aggValue, Text.decode(endRecordFromNextBlock.getBytes(), 0, newStart));
                            
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        // haven't seen a start or end, so just keep aggregating
                        TextUtil.textAppendNoNull(aggValue, endRecordFromNextBlock.toString());
                    }
                    
                    end = aggValue.find(endToken, prevLength);
                    prevLength = aggValue.getLength();
                }
                
                return true;
            }
            
            return hasNext;
        }
    }
}
