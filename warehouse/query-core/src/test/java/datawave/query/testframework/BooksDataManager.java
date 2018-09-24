package datawave.query.testframework;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.data.hash.UID;
import datawave.data.normalizer.Normalizer;
import datawave.query.testframework.BooksDataType.BooksField;
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
import java.util.Date;
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
    
    // TODO - this could be read in from a CSV file
    private static final List<Map.Entry<Multimap<String,String>,UID>> TEST_DATA;
    static {
        TEST_DATA = new ArrayList<>();
        Multimap<String,String> data = HashMultimap.create();
        data.put(BooksField.BOOKS_DATE.name(), "20150707");
        data.put(BooksField.TITLE.name(), "Effective Java");
        data.put(BooksField.AUTHOR.name(), "Joshua Bloch");
        data.put(BooksField.NUM_PAGES.name(), "373");
        data.put(BooksField.SUB_TITLE.name(), "3rd Edition");
        data.put(BooksField.DATE_PUBLISHED.name(), "20171227");
        data.put(BooksField.LANGUAGE.name(), "ENGLISH");
        data.put(BooksField.ISBN_13.name(), "978-0134685991");
        data.put(BooksField.ISBN_10.name(), "0-134-68599-7");
        UID uid = UID.builder().newId(data.toString().getBytes(), "");
        TEST_DATA.add(Maps.immutableEntry(data, uid));
        
        data = HashMultimap.create();
        data.put(BooksField.BOOKS_DATE.name(), "20150808");
        data.put(BooksField.TITLE.name(), "Java Concurrency in Practice");
        data.put(BooksField.AUTHOR.name(), "Doug Lea");
        data.put(BooksField.AUTHOR.name(), "Joshua Bloch");
        data.put(BooksField.AUTHOR.name(), "Brian Goetz");
        data.put(BooksField.AUTHOR.name(), "Tim Peierls");
        data.put(BooksField.AUTHOR.name(), "Joesph Bowbeer");
        data.put(BooksField.AUTHOR.name(), "David Holmes");
        data.put(BooksField.NUM_PAGES.name(), "232");
        data.put(BooksField.SUB_TITLE.name(), "1st Edition");
        data.put(BooksField.DATE_PUBLISHED.name(), "20060509");
        data.put(BooksField.LANGUAGE.name(), "ENGLISH");
        data.put(BooksField.LANGUAGE.name(), "SPANISH");
        data.put(BooksField.ISBN_13.name(), "978-0321349606");
        data.put(BooksField.ISBN_10.name(), "0-321-34960-1");
        uid = UID.builder().newId(data.toString().getBytes(), "");
        TEST_DATA.add(Maps.immutableEntry(data, uid));
        
        data = HashMultimap.create();
        data.put(BooksField.BOOKS_DATE.name(), "20150909");
        data.put(BooksField.TITLE.name(), "Java Puzzlers");
        data.put(BooksField.AUTHOR.name(), "Joshua Bloch");
        data.put(BooksField.AUTHOR.name(), "Neal Gafter");
        data.put(BooksField.NUM_PAGES.name(), "271");
        data.put(BooksField.SUB_TITLE.name(), "Traps, Pitfalls, and Corner Cases");
        data.put(BooksField.DATE_PUBLISHED.name(), "20050624");
        data.put(BooksField.LANGUAGE.name(), "ENGLISH");
        data.put(BooksField.LANGUAGE.name(), "FRENCH");
        data.put(BooksField.ISBN_13.name(), "978-0321336781");
        data.put(BooksField.ISBN_10.name(), "0-321-33678-X");
        uid = UID.builder().newId(data.toString().getBytes(), "");
        TEST_DATA.add(Maps.immutableEntry(data, uid));
        
        data = HashMultimap.create();
        data.put(BooksField.BOOKS_DATE.name(), "20151010");
        data.put(BooksField.TITLE.name(), "Java Performance Companion");
        data.put(BooksField.AUTHOR.name(), "Charlie Hunt");
        data.put(BooksField.AUTHOR.name(), "Monica Beckwith");
        data.put(BooksField.AUTHOR.name(), "Poonam Parhar");
        data.put(BooksField.AUTHOR.name(), "Bengt Rutisson");
        data.put(BooksField.NUM_PAGES.name(), "155");
        data.put(BooksField.SUB_TITLE.name(), "");
        data.put(BooksField.DATE_PUBLISHED.name(), "20160507");
        data.put(BooksField.LANGUAGE.name(), "ENGLISH");
        data.put(BooksField.LANGUAGE.name(), "GERMAN");
        data.put(BooksField.ISBN_13.name(), "978-0-13-379682-7");
        data.put(BooksField.ISBN_10.name(), "0-13-379682-5");
        uid = UID.builder().newId(data.toString().getBytes(), "");
        TEST_DATA.add(Maps.immutableEntry(data, uid));
    }
    
    private final String datatype;
    private final Connector accumuloConn;
    private final FieldConfig fieldIndex;
    private final ConfigData cfgData;
    
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
        log.debug("noop condition - use loadTestData method");
    }
    
    @Override
    public Date[] getRandomStartEndDate() {
        return SHARD_ID_VALUES.getStartEndDates(true);
    }
    
    @Override
    public Date[] getShardStartEndDate() {
        return SHARD_ID_VALUES.getStartEndDates(false);
    }
    
    private static class BooksRawData extends BaseRawData {
        
        private final Map<String,RawMetaData> metadata;
        private List<String> headers = new ArrayList<>();
        
        // only shard date to be used for test data
        
        BooksRawData(String datatype, List<String> baseHeaders, Map<String,RawMetaData> metaDataMap, Map<String,Collection<String>> rawData) {
            super(datatype);
            this.metadata = metaDataMap;
            this.headers = baseHeaders;
            processMapFormat(datatype, rawData);
        }
        
        BooksRawData(String datatype, List<String> baseHeaders, Map<String,RawMetaData> metaDataMap, String[] fields) {
            super(datatype);
            this.metadata = metaDataMap;
            this.headers = baseHeaders;
            processFields(datatype, fields);
        }
        
        @Override
        protected List<String> getHeaders() {
            return this.headers;
        }
        
        @Override
        protected boolean containsField(String field) {
            return metadata.keySet().contains(field.toLowerCase());
        }
        
        @Override
        protected Normalizer<?> getNormalizer(String field) {
            Assert.assertTrue(containsField(field));
            return metadata.get(field.toLowerCase()).normalizer;
        }
        
        @Override
        public boolean isMultiValueField(String field) {
            Assert.assertTrue(containsField(field));
            return metadata.get(field.toLowerCase()).multiValue;
        }
        
        Multimap<String,String> toMultimap() {
            Multimap<String,String> mm = HashMultimap.create();
            for (Map.Entry<String,Set<String>> e : this.entry.entrySet()) {
                for (String val : e.getValue()) {
                    mm.put(e.getKey().toUpperCase(), val);
                }
            }
            
            return mm;
        }
    }
}
