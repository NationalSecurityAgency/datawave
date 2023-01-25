package datawave.data.hash;

import static datawave.data.hash.UIDConstants.HOST_INDEX_OPT;
import static datawave.data.hash.UIDConstants.PROCESS_INDEX_OPT;
import static datawave.data.hash.UIDConstants.THREAD_INDEX_OPT;

import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.log4j.Logger;

/**
 * Builds a sequence of SnowflakeUIDs for a particular "machine" instance, which is based on a unique combination of host, process, and process thread.
 */
public class SnowflakeUIDBuilder extends AbstractUIDBuilder<SnowflakeUID> {
    
    private static final BigInteger UNDEFINED_MACHINE_ID = BigInteger.valueOf(-1);
    private static final BigInteger UNDEFINED_SNOWFLAKE = BigInteger.valueOf(-1);
    private static final Logger LOGGER = Logger.getLogger(SnowflakeUIDBuilder.class);
    
    private final BigInteger mid;
    
    private int radix;
    
    private long sid;
    
    private long tid;
    
    private long previousTid = -1;
    
    /**
     * Constructor for creating uninitialized UIDs intended only for deserialization purposes
     */
    protected SnowflakeUIDBuilder() {
        mid = UNDEFINED_MACHINE_ID;
    }
    
    /**
     * Constructor
     * 
     * @param machineId
     *            unique 20-bit machine ID between 0 and 1048575, inclusively
     */
    protected SnowflakeUIDBuilder(int machineId) {
        this(-1L, machineId, 0);
    }
    
    /**
     * Constructor
     * 
     * @param nodeId
     *            unique 8-bit node ID between 0 and 255, inclusively
     * @param processId
     *            unique 6-bit process ID between 0 and 63, inclusively
     * @param threadId
     *            unique 6-bit thread ID between 0 and 63, inclusively
     */
    protected SnowflakeUIDBuilder(int nodeId, int processId, int threadId) {
        this(-1, nodeId, processId, threadId, 0);
    }
    
    /*
     * Constructor
     * 
     * @param timestamp the initial seed value for a 52-bit timestamp (max value of 4503599627370495), or -1 to initialize the builder with a System-generated
     * value based on the current milliseconds from the epoch
     * 
     * @param machineId unique 20-bit machine ID between 0 and 1048575, inclusively
     * 
     * @param sequenceId the initial seed value for a 24-bit, one-up counter (max value of 16777215), or a negative integer to allow the builder to specify the
     * seed value
     */
    private SnowflakeUIDBuilder(long timestamp, final BigInteger machineId, int sequenceId) {
        
        // The machine ID has already been validated, so just assign it (mid)
        this.mid = machineId;
        
        // Validate and assign the timestamp ID (tid)
        this.tid = validateTimestamp(timestamp);
        
        // Validate and assign the initial sequence ID (sid)
        this.sid = validateSequenceId(sequenceId);
        
        // Set the radix
        this.radix = SnowflakeUID.DEFAULT_RADIX;
    }
    
    /**
     * Constructor
     * 
     * @param timestamp
     *            the initial seed value for a 52-bit timestamp (max value of 4503599627370495), or -1 to initialize the builder with a System-generated value
     *            based on the current milliseconds from the epoch
     * @param machineId
     *            unique 20-bit machine ID between 0 and 1048575, inclusively
     */
    protected SnowflakeUIDBuilder(long timestamp, int machineId) {
        this(timestamp, validateMachineId(machineId), 0);
    }
    
    /**
     * Constructor
     * 
     * @param timestamp
     *            the initial seed value for a 52-bit timestamp (max value of 4503599627370495), or -1 to initialize the builder with a System-generated value
     *            based on the current milliseconds from the epoch
     * @param machineId
     *            unique 20-bit machine ID between 0 and 1048575, inclusively
     * @param sequenceId
     *            the initial seed value for a 24-bit, one-up counter (max value of 16777215), or a negative integer to allow the builder to specify the seed
     *            value
     */
    protected SnowflakeUIDBuilder(long timestamp, int machineId, int sequenceId) {
        this(timestamp, validateMachineId(machineId), sequenceId);
    }
    
    /**
     * Constructor
     * 
     * @param timestamp
     *            the initial seed value for a 52-bit timestamp (max value of 4503599627370495), or -1 to initialize the builder with a System-generated value
     *            based on the current milliseconds from the epoch
     * @param nodeId
     *            unique 8-bit node ID between 0 and 255, inclusively
     * @param processId
     *            unique 6-bit process ID between 0 and 63, inclusively
     * @param threadId
     *            unique 6-bit thread ID between 0 and 63, inclusively
     */
    protected SnowflakeUIDBuilder(long timestamp, int nodeId, int processId, int threadId) {
        this(timestamp, nodeId, processId, threadId, 0);
    }
    
    /**
     * Constructor
     * 
     * @param timestamp
     *            the initial seed value for a 52-bit timestamp (max value of 4503599627370495), or -1 to initialize the builder with a System-generated value
     *            based on the current milliseconds from the epoch
     * @param nodeId
     *            unique 8-bit node ID between 0 and 255, inclusively
     * @param processId
     *            unique 6-bit process ID between 0 and 63, inclusively
     * @param threadId
     *            unique 6-bit thread ID between 0 and 63, inclusively
     * @param sequenceId
     *            the initial seed value for a 24-bit, one-up counter (max value of 16777215), or a negative integer to allow the builder to specify the seed
     *            value
     */
    protected SnowflakeUIDBuilder(long timestamp, int nodeId, int processId, int threadId, int sequenceId) {
        this(timestamp, validateMachineIds(nodeId, processId, threadId), sequenceId);
    }
    
    @Override
    public SnowflakeUID newId(final String... extras) {
        final BigInteger snowflake;
        synchronized (mid) {
            snowflake = nextSnowflake();
        }
        
        return (snowflake == UNDEFINED_SNOWFLAKE) ? new SnowflakeUID() : new SnowflakeUID(snowflake, radix, extras);
    }
    
    @Override
    public SnowflakeUID newId(final byte[] data, final String... extras) {
        return newId(extras);
    }
    
    @Override
    public SnowflakeUID newId(final Date time, final String... extras) {
        // Ignoring time to prevent uuid collisions
        return newId(extras);
    }
    
    @Override
    public SnowflakeUID newId(final byte[] data, final Date time, final String... extras) {
        return newId(time, extras);
    }
    
    protected SnowflakeUID newId(int sequenceId, final String... extras) {
        final BigInteger snowflake;
        synchronized (mid) {
            sid = validateSequenceId(sequenceId);
            snowflake = nextSnowflake();
        }
        
        return (snowflake == UNDEFINED_SNOWFLAKE) ? new SnowflakeUID() : new SnowflakeUID(snowflake, radix, extras);
    }
    
    protected SnowflakeUID newId(long timestamp, int sequenceId, final String... extras) {
        final BigInteger snowflake;
        synchronized (mid) {
            tid = validateTimestamp(timestamp);
            sid = validateSequenceId(sequenceId);
            snowflake = nextSnowflake();
        }
        
        return (snowflake == UNDEFINED_SNOWFLAKE) ? new SnowflakeUID() : new SnowflakeUID(snowflake, radix, extras);
    }
    
    protected SnowflakeUID newId(long timestamp, final String... extras) {
        final BigInteger snowflake;
        synchronized (mid) {
            tid = validateTimestamp(timestamp);
            snowflake = nextSnowflake();
        }
        
        return (snowflake == UNDEFINED_SNOWFLAKE) ? new SnowflakeUID() : new SnowflakeUID(snowflake, radix, extras);
    }
    
    protected static SnowflakeUID newId(final SnowflakeUID template, final String... extras) {
        final SnowflakeUID newId;
        if (null != template) {
            // Get the existing and new extras, if any
            final String extra1 = template.getExtra();
            final String extra2 = SnowflakeUID.mergeExtras(extras);
            
            // Create a new UID based on existing and new extras
            if ((null != extra1) && (null != extra2)) {
                newId = new SnowflakeUID(template, extra1, extra2);
            }
            // Create a new UID as a copy of the template (with existing extras)
            else if ((null != extra1) && (null == extra2)) {
                newId = new SnowflakeUID(template, extra1);
            }
            // Create a new UID based only on new extra(s)
            else if ((null == extra1) && (null != extra2)) {
                newId = new SnowflakeUID(template, extra2);
            }
            // Create a new UID as a copy of the template (without existing extras)
            else {
                newId = new SnowflakeUID(template);
            }
        } else {
            newId = null;
        }
        
        return newId;
    }
    
    /**
     * Internal utility method for generating a Snowflake machine ID from command-line options
     * 
     * @param options
     *            command line options being used
     * @return a Snowflake machine ID
     */
    protected static int newMachineId(final Map<String,Option> options) {
        int machineId = -1;
        
        int hostId = -1;
        int processId = -1;
        int threadId = -1;
        
        Option option = options.get(HOST_INDEX_OPT);
        if (null != option) {
            try {
                hostId = Integer.parseInt(option.getValue());
            } catch (final Exception e) {
                if (LOGGER.isDebugEnabled()) {
                    final String message = "Invalid " + HOST_INDEX_OPT + ": " + option;
                    LOGGER.warn(message, e);
                }
            }
        }
        
        option = options.get(PROCESS_INDEX_OPT);
        if ((hostId >= 0) && (null != option)) {
            try {
                processId = Integer.parseInt(option.getValue());
            } catch (final Exception e) {
                if (LOGGER.isDebugEnabled()) {
                    final String message = "Invalid " + PROCESS_INDEX_OPT + ": " + option;
                    LOGGER.warn(message, e);
                }
            }
        }
        
        option = options.get(THREAD_INDEX_OPT);
        if ((processId >= 0) && (null != option)) {
            try {
                threadId = Integer.parseInt(option.getValue());
            } catch (final Exception e) {
                if (LOGGER.isDebugEnabled()) {
                    final String message = "Invalid " + THREAD_INDEX_OPT + ": " + option;
                    LOGGER.warn(message, e);
                }
            }
        }
        
        try {
            machineId = validateMachineIds(hostId, processId, threadId).intValue();
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                final String message = "Unable to generate Snowflake machine ID";
                LOGGER.warn(message, e);
            }
        }
        
        return machineId;
    }
    
    private BigInteger nextSnowflake() {
        BigInteger snowflake;
        synchronized (mid) {
            if (mid == UNDEFINED_SNOWFLAKE) {
                snowflake = UNDEFINED_SNOWFLAKE;
            } else {
                snowflake = BigInteger.valueOf(tid).shiftLeft(44);
                snowflake = snowflake.or(mid.shiftLeft(24));
                snowflake = snowflake.or(BigInteger.valueOf(sid));
                
                sid++;
                if (sid > SnowflakeUID.MAX_SEQUENCE_ID) {
                    sid = 0;
                    validateTimestamp(++tid);
                }
            }
        }
        return snowflake;
    }
    
    public SnowflakeUIDBuilder setRadix(int radix) {
        this.radix = radix;
        return this;
    }
    
    @Override
    public String toString() {
        return "SnowflakeUIDBuilder [timestamp=" + tid + ", machineId=" + mid + ", sequenceId=" + sid + ", radix=" + radix + "]";
    }
    
    private static BigInteger validateMachineId(int machineId) {
        if ((machineId < 0) || (machineId > SnowflakeUID.MAX_MACHINE_ID)) {
            throw new IllegalArgumentException("Machine ID must be a value between 0 and " + SnowflakeUID.MAX_MACHINE_ID + ", inclusively");
        }
        
        return BigInteger.valueOf(machineId);
    }
    
    private static BigInteger validateMachineIds(int nodeId, int processId, int threadId) {
        // Validate each ID
        if ((nodeId < 0) || (nodeId > SnowflakeUID.MAX_NODE_ID)) {
            throw new IllegalArgumentException("Node ID must be a value between 0 and " + SnowflakeUID.MAX_NODE_ID + ", inclusively, but was " + nodeId);
        }
        
        if ((processId < 0) || (processId > SnowflakeUID.MAX_PROCESS_ID)) {
            throw new IllegalArgumentException("Process ID must be a value between 0 and " + SnowflakeUID.MAX_PROCESS_ID + ", inclusively, but was "
                            + processId);
        }
        
        if ((threadId < 0) || (threadId > SnowflakeUID.MAX_THREAD_ID)) {
            throw new IllegalArgumentException("Thread ID must be a value between 0 and " + SnowflakeUID.MAX_THREAD_ID + ", inclusively, but was " + threadId);
        }
        
        // Combine the smaller IDs to form a machine ID
        int machineId = (nodeId << 12) + (processId << 6) + threadId;
        
        // Validate and instantiate a fixed machine ID
        return validateMachineId(machineId);
    }
    
    private static int validateSequenceId(int sequenceId) {
        if (sequenceId > SnowflakeUID.MAX_SEQUENCE_ID) {
            throw new IllegalArgumentException("Max sequence ID is " + SnowflakeUID.MAX_SEQUENCE_ID);
        } else if (sequenceId < 0) {
            sequenceId = 0;
        }
        
        return sequenceId;
    }
    
    private long validateTimestamp(long timestamp) {
        if (timestamp > SnowflakeUID.MAX_TIMESTAMP || (previousTid + 1) > SnowflakeUID.MAX_TIMESTAMP) {
            throw new IllegalArgumentException("Max timestamp is " + SnowflakeUID.MAX_TIMESTAMP);
        }
        
        // removed initial check for timestamp <= 0 since we should ignore any timestamp passed in to this method
        // and assign it to either the current time or previous TID + 1
        if (this.previousTid <= 0) {
            timestamp = initializeTimestamp();
            
        } else {
            timestamp = this.previousTid + 1;
        }
        
        if (timestamp <= this.previousTid) {
            LOGGER.warn("Current tid is less than the previous.  This could cause uid collisions.\n" + "Mid: " + mid + ", Timestamp: " + timestamp
                            + ", Previous: " + previousTid + ", System Time: " + System.currentTimeMillis());
            timestamp = this.previousTid + 1;
        }
        
        // stash the timestamp in ZK
        this.previousTid = timestamp;
        
        storeTimestamp();
        
        return timestamp;
    }
    
    private void storeTimestamp() {
        if (ZkSnowflakeCache.isInitialized()) {
            try {
                ZkSnowflakeCache.store(mid, this.previousTid);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Caching ZK ts: " + this.previousTid + ", mid: " + this.mid);
                }
                
            } catch (Exception e) {
                LOGGER.error("Unable to store snowflake id from zookeeper for " + mid, e);
                throw new RuntimeException(e);
            }
        }
    }
    
    private long initializeTimestamp() {
        long timestamp;
        long lastCachedTid = 0;
        if (ZkSnowflakeCache.isInitialized()) {
            try {
                lastCachedTid = ZkSnowflakeCache.getLastCachedTid(this.mid);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Getting ZK ts: " + lastCachedTid + " mid: " + this.mid);
                }
                
            } catch (Exception e) {
                LOGGER.error("Unable to retrieve snowflake id from zookeeper for " + mid, e);
                throw new RuntimeException(e);
                
            }
        }
        if (lastCachedTid > 0) {
            timestamp = lastCachedTid + 1;
        } else {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
    
}
