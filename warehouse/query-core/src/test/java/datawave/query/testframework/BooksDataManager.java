package datawave.query.testframework;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.data.normalizer.Normalizer;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Creates accumulo data as grouping data. However, that does not limit additional test classes for other purposes.
 */
public class BooksDataManager extends AbstractDataManager {
    
    private static final Logger log = Logger.getLogger(BooksDataManager.class);
    
    private final String datatype;
    private final Connector accumuloConn;
    private final FieldConfig fieldIndex;
    private final ConfigData cfgData;
    
    /**
     *
     * @param datatype
     *            datatype name
     * @param conn
     *            accumulo connection for writing
     * @param indexes
     *            indexes for the datatype
     * @param data
     *            configuration data
     */
    public BooksDataManager(final String datatype, final Connector conn, final FieldConfig indexes, final ConfigData data) {
        super(data.getEventId(), data.getDateField());
        this.datatype = datatype;
        this.accumuloConn = conn;
        this.fieldIndex = indexes;
        this.cfgData = data;
        RawMetaData shardMetdata = new RawMetaData(this.cfgData.getDateField(), Normalizer.LC_NO_DIACRITICS_NORMALIZER, false);
        this.metadata = new HashMap<>();
        this.metadata.put(this.cfgData.getDateField().toLowerCase(), shardMetdata);
        for (Map.Entry<String,RawMetaData> field : data.getMetadata().entrySet()) {
            this.metadata.put(field.getKey(), field.getValue());
        }
        this.rawDataIndex.put(datatype, indexes.getIndexFields());
    }
    
    @Override
    public List<String> getHeaders() {
        return this.cfgData.headers();
    }
    
    /**
     * Loads test data as grouping data from a multimap.
     * 
     * @param data
     *            list of multimap entries of field name to values
     */
    public void loadTestData(final List<Map.Entry<Multimap<String,String>,UID>> data) {
        try {
            GroupingAccumuloWriter writer = new GroupingAccumuloWriter(this.datatype, this.accumuloConn, this.cfgData.getDateField(), this.fieldIndex,
                            this.cfgData);
            writer.addData(data);
            Set<RawData> testData = new HashSet<>();
            for (Map.Entry<Multimap<String,String>,UID> entry : data) {
                Multimap<String,String> mm = entry.getKey();
                Map<String,Collection<String>> val = mm.asMap();
                RawData rawEntry = new BooksRawData(this.datatype, this.getHeaders(), this.metadata, val);
                testData.add(rawEntry);
            }
            this.rawData.put(this.datatype, testData);
        } catch (MutationsRejectedException | TableNotFoundException me) {
            throw new AssertionError(me);
        }
    }
    
    /**
     * Loads test data from a CSV file.
     * 
     * @param file
     *            uri of CSV file
     */
    public void loadGroupingData(final URI file) {
        Assert.assertFalse("datatype has already been configured(" + this.datatype + ")", this.rawData.containsKey(this.datatype));
        
        try (final Reader reader = Files.newBufferedReader(Paths.get(file)); final CSVReader csv = new CSVReader(reader)) {
            GroupingAccumuloWriter writer = new GroupingAccumuloWriter(this.datatype, this.accumuloConn, this.cfgData.getDateField(), this.fieldIndex,
                            this.cfgData);
            
            Set<RawData> bookData = new HashSet<>();
            List<Map.Entry<Multimap<String,String>,UID>> loadData = new ArrayList<>();
            
            int count = 0;
            String[] data;
            while (null != (data = csv.readNext())) {
                RawData rawEntry = new BooksRawData(this.datatype, this.getHeaders(), this.metadata, data);
                bookData.add(rawEntry);
                
                final Multimap<String,String> mm = ((BooksRawData) rawEntry).toMultimap();
                UID uid = UID.builder().newId(mm.toString().getBytes(), "");
                loadData.add(Maps.immutableEntry(mm, uid));
                count++;
            }
            this.rawData.put(this.datatype, bookData);
            writer.addData(loadData);
        } catch (IOException | MutationsRejectedException | TableNotFoundException e) {
            throw new AssertionError(e);
        }
    }
    
    @Override
    public void addTestData(URI file, String datatype, Set<String> indexes) throws IOException {
        // ignore
        log.error("noop condition - use loadTestData method");
        Assert.fail("method not supported for books data manager");
    }
    
    /**
     * POJO for a single raw data entry for the books datatype.
     */
    private static class BooksRawData extends BaseRawData {
        
        /**
         * This allows a test to loadstatic test data in the form of a map of field name to values.
         * 
         * @param datatype
         *            datatype name
         * @param baseHeaders
         *            fields for books datatype
         * @param metaData
         *            mapping of datatype fields to metadata
         * @param rawData
         *            mapping of field to a collection of values
         */
        BooksRawData(String datatype, List<String> baseHeaders, Map<String,RawMetaData> metaData, Map<String,Collection<String>> rawData) {
            super(datatype, baseHeaders, metaData);
            processMapFormat(datatype, rawData);
        }
        
        /**
         * Loads a CSV entry in the form of a array.
         * 
         * @param datatype
         *            datatype name
         * @param baseHeaders
         *            fields for books datatype
         * @param metaData
         *            mapping of datatype fields to metadata
         * @param fields
         *            list of values for fields
         */
        BooksRawData(String datatype, List<String> baseHeaders, Map<String,RawMetaData> metaData, String[] fields) {
            super(datatype, baseHeaders, metaData);
            processFields(datatype, fields);
        }
        
        /**
         * Converts the entry to a {@link Multimap}.
         * 
         * @return populated multimap
         */
        Multimap<String,String> toMultimap() {
            Multimap<String,String> mm = HashMultimap.create();
            for (Map.Entry<String,Set<String>> e : this.event.entrySet()) {
                for (String val : e.getValue()) {
                    mm.put(e.getKey().toUpperCase(), val);
                }
            }
            
            return mm;
        }
    }
}
