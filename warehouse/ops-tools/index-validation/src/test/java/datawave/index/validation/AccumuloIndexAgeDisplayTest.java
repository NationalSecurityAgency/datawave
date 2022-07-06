package datawave.index.validation;

/**
 This is the test class for AccumuloIndexAgeDisplay.  It will create an instance
 of accumulo, load it with data and verify AccumuloIndexAgeDisplay shows the
 data in the proper buckets.
 */

import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AccumuloIndexAgeDisplayTest {
    private static final Logger log = Logger.getLogger(AccumuloIndexAgeDisplayTest.class);
    
    private static final long MILLIS_IN_DAY = 86400000;
    private static final long ONE_HOUR = 3600000;
    private static final long ONE_MINUTE = 6000;
    private static final long ONE_DAY = MILLIS_IN_DAY + ONE_MINUTE;
    private static final long TWO_DAYs = MILLIS_IN_DAY * 2 + ONE_MINUTE;
    private static final long EIGHT_DAYS = MILLIS_IN_DAY * 8 + ONE_MINUTE;
    private static final long FIFTEEN_DAYS = MILLIS_IN_DAY * 15 + ONE_MINUTE;
    private static final long THIRTYONE_DAYS = MILLIS_IN_DAY * 31 + ONE_MINUTE;
    
    // our fake accumulo instance
    private Instance mockInstance = null;
    private Connector conn = null;
    
    // name of the table we'll be scanning
    private final String tableName = "DatawaveMetadata";
    private final String columns = "indx";
    
    private final String userName = "datawave";
    private final PasswordToken password = new PasswordToken("");
    
    private String fileName = "/tmp/aiad.txt";
    
    private Authorizations auths = new Authorizations("X,Y,Z".split(","));
    
    private AccumuloIndexAgeDisplay aiad = null;
    
    @Before
    public void setup() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
        // set hadoop.home.dir so we don't get an IOException about it. Doesn't appear to be used though
        System.setProperty("hadoop.home.dir", "/tmp");
        // May need to replace InMemoryInstance with MiniCluster. Apparently InMemoryInstance isn't kept up as well.
        mockInstance = new InMemoryInstance();
        conn = mockInstance.getConnector(userName, password);
        conn.securityOperations().changeUserAuthorizations(userName, auths);
        conn.tableOperations().create(tableName);
    }
    
    /**
     * This method completes the setup process and was redundant in all but of the tests.
     * 
     * @param bucketsToUse
     *            - the array of buckets to use
     */
    private void completeSetup(Integer[] bucketsToUse) {
        try {
            deleteFile(fileName);
            aiad = new AccumuloIndexAgeDisplay(mockInstance, tableName, columns, userName, password, bucketsToUse);
            aiad.extractDataFromAccumulo();
            aiad.logAgeSummary();
            aiad.createAccumuloShellScript(fileName);
        } catch (AccumuloException ae) {
            log.error("Accumlo exception from our mock instance.");
            log.error(ae.getMessage());
        } catch (AccumuloSecurityException ase) {
            log.error("Accumulo security exception from our mock instance");
            log.error(ase.getMessage());
        }
    }
    
    /**
     * Delete the file specified
     * 
     * @param fn
     *            the filename to delete
     */
    private void deleteFile(String fn) {
        File file = new File(fn);
        if (file.exists()) {
            boolean result = file.delete();
            if (!result) {
                log.warn("Failed to delete " + fn);
            }
        }
    }
    
    @After
    public void tearDown() {
        deleteFile(fileName);
    }
    
    /**
     * A test verifying the buckets are sorted in reverse order.
     */
    @Test
    public void sortBucketsInReverseOrderTest() {
        Assert.assertNotNull(mockInstance);
        try {
            aiad = new AccumuloIndexAgeDisplay(mockInstance, tableName, columns, userName, password, new Integer[0]);
            aiad.setBuckets(null);
            Integer[] expected = {180, 90, 60, 30, 14, 7, 2};
            Integer[] actual = aiad.getBuckets();
            Assert.assertArrayEquals(expected, actual);
            
            Integer[] useExpectedWithTooSmallNumber = {1, 2, 3, 4, 5};
            expected = new Integer[] {5, 4, 3, 2};
            aiad.setBuckets(useExpectedWithTooSmallNumber);
            actual = aiad.getBuckets();
            Assert.assertArrayEquals(expected, actual);
            
        } catch (AccumuloException ae) {
            log.error("Accumlo exception from our mock instance.");
            log.error(ae.getMessage());
        } catch (AccumuloSecurityException ase) {
            log.error("Accumulo security exception from our mock instance");
            log.error(ase.getMessage());
        }
    }
    
    /**
     * A test that has an assorted of rowws with different timestamps
     */
    @Test
    public void assortedDataIntoDefaultBucketLogOutputTest() {
        // The data entered into accumulo: 2 - one day old, 2 two days old, two , 2 eight days old, 2 fifteen days old
        // and 2 thirty one days old. The last eight should be removed by the default buckets
        loadAssortedData();
        completeSetup(new Integer[0]);
        
        String expectedLogSummary = getAssortedSimulatedLogOutput();
        String actualLogSummary = aiad.logAgeSummary();
        Assert.assertEquals(expectedLogSummary, actualLogSummary);
    }
    
    /**
     * A test that has an assorted of rowws with different timestamps
     */
    @Test
    public void assortedDataIntoDefaultBucketFileOutputTest() {
        // The data entered into accumulo: 2 - one day old, 2 two days old, two , 2 eight days old, 2 fifteen days old
        // and two thirty one days old. The last eight should be removed by the default buckets
        loadAssortedData();
        completeSetup(new Integer[0]);
        
        String expectedFileOutput = getAssortedSimulatedFileOutput();
        String generatedOutput = readGeneratedFile();
        Assert.assertEquals(expectedFileOutput, generatedOutput);
    }
    
    /**
     * A test with data with a timestamp one hour ago. No data should be identified as being deleteable
     */
    @Test
    public void oneHourOldDataIntoDefaultBucketLogOutputTest() {
        // All the data is "new" so none should be ready to be aged-off
        loadOneHourData();
        completeSetup(new Integer[0]);
        
        String expectedLogSummary = getOneHourSimulatedLogOutput();
        String actualLogSummary = aiad.logAgeSummary();
        Assert.assertEquals(expectedLogSummary, actualLogSummary);
    }
    
    /**
     * A test with data with a timestamp one hour ago. No data should be identified as being deleteable
     */
    @Test
    public void oneHourOldDataIntoDefaultBucketFileOutputTest() {
        // All the data is "new" so none should be ready to be aged-off
        loadOneHourData();
        completeSetup(new Integer[0]);
        
        String expectedFileOutput = getOneHourOldDataSimulatedFileOutput();
        String generatedOutput = readGeneratedFile();
        Assert.assertEquals(expectedFileOutput, generatedOutput);
    }
    
    /**
     * A test using the assorted data, 2 - 31 days old, using one bucket of three days. So all day older than three days should have a delete statement.
     */
    @Test
    public void assortedDataIntoThreeDayBucketLogOutputTest() {
        // Should identify data more than three days old in the log and deletion script
        loadAssortedData();
        completeSetup(new Integer[] {3});
        
        String expectedLogSummary = getAssortedDataThreeDaySimulatedLogOutput();
        String actualLogSummary = aiad.logAgeSummary();
        Assert.assertEquals(expectedLogSummary, actualLogSummary);
    }
    
    /**
     * A test using the assorted data, 2 - 31 days old, using one bucket of three days. So all day older than three days should have a delete statement.
     */
    @Test
    public void assortedDataIntoThreeDayBucketFileOutputTest() {
        // Should identify data more than three days old in the log and deletion script
        loadAssortedData();
        completeSetup(new Integer[] {3});
        
        String expectedFileOutput = getAssortedDataThreeDayBucketSimulatedFileOutput();
        String generatedOutput = readGeneratedFile();
        Assert.assertEquals(expectedFileOutput, generatedOutput);
    }
    
    /**
     * Loads data into accumulo with timestamps of 1, 2, 8, 15, and 31 days old.
     */
    private void loadAssortedData() {
        try {
            BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(100000L)
                            .setMaxWriteThreads(4));
            
            long currentTime = System.currentTimeMillis();
            
            int ii = 0;
            Mutation m = new Mutation("Row" + ii);
            m.put(new Text(columns), new Text("D01_" + ii), new ColumnVisibility("X"), currentTime - ONE_DAY, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("D01_" + ii), new ColumnVisibility("X"), currentTime - ONE_DAY, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("D02_" + ii), new ColumnVisibility("X"), currentTime - TWO_DAYs, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("D02_" + ii), new ColumnVisibility("X"), currentTime - TWO_DAYs, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("D08_" + ii), new ColumnVisibility("X"), currentTime - EIGHT_DAYS, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("D08_" + ii), new ColumnVisibility("X"), currentTime - EIGHT_DAYS, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("D15_" + ii), new ColumnVisibility("X"), currentTime - FIFTEEN_DAYS, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("D15_" + ii), new ColumnVisibility("X"), currentTime - FIFTEEN_DAYS, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("D31_NoVis_" + ii), currentTime - THIRTYONE_DAYS, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("D31_NoVis_" + ii), currentTime - THIRTYONE_DAYS, new Value("value".getBytes()));
            bw.addMutation(m);
            
            bw.close();
        } catch (TableNotFoundException tnfe) {
            log.error("Unable to find mock accumulo table");
        } catch (MutationsRejectedException mre) {
            log.error("Unable to write to mock accumulo instance.");
        }
    }
    
    /**
     * Loads data into accumulo with a timestamp from one hour ago We shouldn't find any data from this group.
     */
    private void loadOneHourData() {
        try {
            BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(100000L)
                            .setMaxWriteThreads(4));
            
            long currentTime = System.currentTimeMillis();
            
            int ii = 0;
            Mutation m = new Mutation("Row" + ii);
            m.put(new Text(columns), new Text("H01" + ii), new ColumnVisibility("X"), currentTime - ONE_HOUR, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("H01" + ii), new ColumnVisibility("X"), currentTime - ONE_HOUR, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("H01" + ii), new ColumnVisibility("X"), currentTime - ONE_HOUR, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("H01" + ii), new ColumnVisibility("X"), currentTime - ONE_HOUR, new Value("value".getBytes()));
            bw.addMutation(m);
            
            m = new Mutation("Row" + ++ii);
            m.put(new Text(columns), new Text("H01" + ii), new ColumnVisibility("X"), currentTime - ONE_HOUR, new Value("value".getBytes()));
            bw.addMutation(m);
            
            bw.close();
        } catch (TableNotFoundException tnfe) {
            log.error("Unable to find mock accumulo table");
        } catch (MutationsRejectedException mre) {
            log.error("Unable to write to mock accumulo instance.");
        }
    }
    
    /**
     * Read the generated file containing the accummulo shell commands
     * 
     * @return A String containing the contents of the file
     */
    private String readGeneratedFile() {
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            
            br.close();
        } catch (FileNotFoundException nfne) {
            System.err.println("Error: Could not find " + fileName);
        } catch (IOException ioe) {
            System.err.println("Error: Could not read file " + fileName);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    log.error("Failed to close " + fileName);
                }
            }
        }
        
        return (sb.toString());
    }
    
    public String getAssortedSimulatedFileOutput() {
        return ("# Run the following commands in an accummulo shell.\n" + "# The lines starting with '#' will be ignored.\n" + "\n"
                        + "table DatawaveMetadata\n" + "# Indexes older than 180 days:\n" + "# Indexes older than 90 days:\n"
                        + "# Indexes older than 60 days:\n" + "# Indexes older than 30 days:\n" + "    delete Row8 indx D31_NoVis_8\n"
                        + "    delete Row9 indx D31_NoVis_9\n" + "# Indexes older than 14 days:\n" + "    delete Row6 indx D15_6 -l X\n"
                        + "    delete Row7 indx D15_7 -l X\n" + "# Indexes older than 7 days:\n" + "    delete Row4 indx D08_4 -l X\n"
                        + "    delete Row5 indx D08_5 -l X\n" + "# Indexes older than 2 days:\n" + "    delete Row2 indx D02_2 -l X\n"
                        + "    delete Row3 indx D02_3 -l X\n" + "\n");
    }
    
    private String getOneHourOldDataSimulatedFileOutput() {
        return ("# Run the following commands in an accummulo shell.\n" + "# The lines starting with '#' will be ignored.\n" + "\n"
                        + "table DatawaveMetadata\n" + "# Indexes older than 180 days:\n" + "# Indexes older than 90 days:\n"
                        + "# Indexes older than 60 days:\n" + "# Indexes older than 30 days:\n" + "# Indexes older than 14 days:\n"
                        + "# Indexes older than 7 days:\n" + "# Indexes older than 2 days:\n" + "\n");
    }
    
    public String getAssortedDataThreeDayBucketSimulatedFileOutput() {
        return ("# Run the following commands in an accummulo shell.\n" + "# The lines starting with '#' will be ignored.\n" + "\n"
                        + "table DatawaveMetadata\n" + "# Indexes older than 3 days:\n" + "    delete Row4 indx D08_4 -l X\n"
                        + "    delete Row5 indx D08_5 -l X\n" + "    delete Row6 indx D15_6 -l X\n" + "    delete Row7 indx D15_7 -l X\n"
                        + "    delete Row8 indx D31_NoVis_8\n" + "    delete Row9 indx D31_NoVis_9\n" + "\n");
    }
    
    private String getAssortedSimulatedLogOutput() {
        return ("\nIndexes older than 2   days:           2\n" + "Indexes older than 7   days:           2\n" + "Indexes older than 14  days:           2\n"
                        + "Indexes older than 30  days:           2\n" + "Indexes older than 60  days:           0\n"
                        + "Indexes older than 90  days:           0\n" + "Indexes older than 180 days:           0\n");
    }
    
    private String getOneHourSimulatedLogOutput() {
        return ("\nIndexes older than 2   days:           0\n" + "Indexes older than 7   days:           0\n" + "Indexes older than 14  days:           0\n"
                        + "Indexes older than 30  days:           0\n" + "Indexes older than 60  days:           0\n"
                        + "Indexes older than 90  days:           0\n" + "Indexes older than 180 days:           0\n");
    }
    
    private String getAssortedDataThreeDaySimulatedLogOutput() {
        return ("\nIndexes older than 3   days:           6\n");
    }
}
