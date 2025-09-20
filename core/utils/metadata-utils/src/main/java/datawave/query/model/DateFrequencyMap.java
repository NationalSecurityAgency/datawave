package datawave.query.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DateFrequencyMap implements Map<String,Frequency>, Writable {

    public enum VERSION {
        DFM1;
    }

    public enum FORMAT {
        BASIC, GZIP;
    }

    private final TreeMap<String,Frequency> dateToFrequencies;

    public DateFrequencyMap() {
        this.dateToFrequencies = new TreeMap<>();
    }

    public DateFrequencyMap(byte[] bytes) throws IOException {
        this();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        try (DataInputStream dataIn = new DataInputStream(in)) {
            readFields(dataIn);
        }
    }

    /**
     * Associates the given frequency with the given date in this {@link DateFrequencyMap}. If the map previously contained a mapping for the given date, the
     * old frequency is replaced by the new frequency.
     *
     * @param date
     *            the date
     * @param frequency
     *            the frequency
     */
    public void put(String date, long frequency) {
        put(date, new Frequency(frequency));
    }

    @Override
    public int size() {
        return dateToFrequencies.size();
    }

    @Override
    public boolean isEmpty() {
        return dateToFrequencies.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return dateToFrequencies.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return dateToFrequencies.containsValue(value);
    }

    @Override
    public Frequency get(Object key) {
        return dateToFrequencies.get(key);
    }

    /**
     * Associates the given frequency with the given date in this {@link DateFrequencyMap}. If the map previously contained a mapping for the given date, the
     * old frequency is replaced by the new frequency.
     *
     * @param date
     *            the date
     * @param frequency
     *            the frequency
     */
    public Frequency put(String date, Frequency frequency) {
        return dateToFrequencies.put(date, frequency);
    }

    @Override
    public Frequency remove(Object key) {
        return dateToFrequencies.remove(key);
    }

    @Override
    public void putAll(Map<? extends String,? extends Frequency> m) {
        dateToFrequencies.putAll(m);
    }

    /**
     * Increments the frequency associated with the given date by the given addend. If a mapping does not previously exist for the date, a new mapping will be
     * added with the given addend as the frequency.
     *
     * @param date
     *            the date
     * @param addend
     *            the addend
     */
    public void increment(String date, long addend) {
        dateToFrequencies.computeIfAbsent(date, (k) -> new Frequency()).increment(addend);
    }

    /**
     * Increment all frequencies in this {@link DateFrequencyMap} by the frequencies in the given map. If the given map contains mappings for dates not present
     * in this map, those mappings will be added to this map.
     *
     * @param map
     *            the map
     */
    public void incrementAll(DateFrequencyMap map) {
        for (Map.Entry<String,Frequency> entry : map.dateToFrequencies.entrySet()) {
            increment(entry.getKey(), entry.getValue().getValue());
        }
    }

    /**
     * Return the frequency associated with the given date, or null if no such mapping exists.
     *
     * @param date
     *            the date
     * @return the count
     */
    public Frequency get(String date) {
        return dateToFrequencies.get(date);
    }

    /**
     * Return whether this map contains a mapping for the given date.
     *
     * @param date
     *            the date
     * @return true if a mapping exists for the given date, or false otherwise
     */
    public boolean contains(String date) {
        return dateToFrequencies.containsKey(date);
    }

    /**
     * Clear all mappings in this {@link DateFrequencyMap}.
     */
    public void clear() {
        this.dateToFrequencies.clear();
    }

    @Override
    public Set<String> keySet() {
        return dateToFrequencies.keySet();
    }

    @Override
    public Collection<Frequency> values() {
        return dateToFrequencies.values();
    }

    /**
     * Returns a {@link Set} view of the mappings contained within this map, sorted in ascending by order.
     *
     * @return a {@link Set} view of the mappings
     */
    public Set<Map.Entry<String,Frequency>> entrySet() {
        return this.dateToFrequencies.entrySet();
    }

    /**
     * Returns a view of the portion of this {@link DateFrequencyMap}'s underlying map whose keys range from startDate (inclusive) to endDate (inclusive).
     *
     * @param startDate
     *            the start date
     * @param endDate
     *            the end date
     * @return the map view
     */
    public SortedMap<String,Frequency> subMap(String startDate, String endDate) {
        return dateToFrequencies.subMap(startDate, true, endDate, true);
    }

    /**
     * Returns the earliest date in this {@link DateFrequencyMap}.
     *
     * @return the earliest date
     */
    public String earliestDate() {
        return dateToFrequencies.firstKey();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // Write the version
        WritableUtils.writeEnum(dataOutput, VERSION.DFM1);

        // Write the format type depending on the data size
        // we are guessing that we break even somewhere around 1K
        FORMAT format = FORMAT.BASIC;
        if (size() > 56) {
            format = FORMAT.GZIP;
        }
        WritableUtils.writeEnum(dataOutput, format);

        switch (format) {
            case BASIC:
                writeData(dataOutput);
                break;
            case GZIP:
                // create a gzipped output
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                try (DataOutputStream out = new DataOutputStream(output)) {
                    writeData(out);
                }
                output.close();
                WritableUtils.writeCompressedByteArray(dataOutput, output.toByteArray());
                break;
            default:
                throw new UnsupportedEncodingException("Unknown output format " + format);
        }
    }

    /**
     * Write out the map fo a data output stream
     *
     * @param dataOutput
     *            the output stream
     * @throws IOException
     */
    private void writeData(DataOutput dataOutput) throws IOException {
        // Write the map's size.
        WritableUtils.writeVInt(dataOutput, dateToFrequencies.size());

        // Write each entry.
        for (Map.Entry<String,Frequency> entry : dateToFrequencies.entrySet()) {
            WritableUtils.writeString(dataOutput, entry.getKey());
            entry.getValue().write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // Clear the map.
        this.dateToFrequencies.clear();

        VERSION ver = WritableUtils.readEnum(dataInput, VERSION.class);

        switch (ver) {
            case DFM1:
                FORMAT format = WritableUtils.readEnum(dataInput, FORMAT.class);
                switch (format) {
                    case BASIC:
                        readData(dataInput);
                        break;
                    case GZIP:
                        byte[] data = WritableUtils.readCompressedByteArray(dataInput);
                        try (ByteArrayInputStream input = new ByteArrayInputStream(data)) {
                            try (DataInputStream in = new DataInputStream(input)) {
                                readData(in);
                            }
                        }
                        break;
                    default:
                        throw new UnsupportedEncodingException("Unknown format " + format);
                }
                break;
            default:
                throw new IOException("Unexpected DateFrequencyMap format: " + ver);
        }
    }

    private void readData(DataInput dataInput) throws IOException {
        // Read how many entries to expect.
        int entries = WritableUtils.readVInt(dataInput);

        // Read each entry.
        for (int i = 0; i < entries; i++) {
            // Read the date key.
            String date = WritableUtils.readString(dataInput);
            // Read the frequency value.
            Frequency value = new Frequency();
            value.readFields(dataInput);
            this.dateToFrequencies.put(date, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateFrequencyMap that = (DateFrequencyMap) o;
        return Objects.equals(dateToFrequencies, that.dateToFrequencies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateToFrequencies);
    }

    @Override
    public String toString() {
        return dateToFrequencies.toString();
    }

    public byte[] toBytes() {
        try {
            // estimated maximum size is the initial 4 bytes of size plus
            // the size of the date (4+8 bytes) and the size of a long (8 bytes) times the size of the map
            int estMaxSize = 4 + (dateToFrequencies.size() * 20);
            DataOutputBuffer out = new DataOutputBuffer(estMaxSize);
            write(out);
            out.close();
            byte[] bytes = out.getData();
            if (out.getLength() != bytes.length) {
                byte[] copy = new byte[out.getLength()];
                System.arraycopy(bytes, 0, copy, 0, out.getLength());
                bytes = copy;
            }
            return bytes;
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to convert DateFrequencyMap to bytes", ioe);
        }
    }
}
