package datawave.index.validation;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created on 8/10/16. This class will scan an accumulo table for all or a specified number of columns. The number of results for rows old than A,B,C,D... days
 * will be displayed. The user can enter a list of days to check
 */
public class AccumuloIndexAgeDisplay implements AutoCloseable {
    private static final Logger log = Logger.getLogger(AccumuloIndexAgeDisplay.class);
    private static final long MILLIS_IN_DAY = 86400000;
    
    private AccumuloClient accumuloClient = null;
    
    private String tableName = null;
    private String columns = null;
    
    private Integer buckets[] = {180, 90, 60, 30, 14, 7, 2};
    
    private ArrayList<String>[] dataBuckets;
    
    public AccumuloIndexAgeDisplay(AccumuloClient accumuloClient, String tableName, String columns, Integer[] buckets) {
        this.tableName = tableName;
        setColumns(columns);
        setBuckets(buckets);
        
        this.accumuloClient = accumuloClient;
    }
    
    public AccumuloIndexAgeDisplay(String instanceName, String zookeepers, String tableName, String columns, String userName, PasswordToken password,
                    Integer[] buckets) {
        this(Accumulo.newClient().to(instanceName, zookeepers).as(userName, password).build(), tableName, columns, buckets);
    }
    
    @Override
    public void close() {
        accumuloClient.close();
    }
    
    /**
     * Sets the accumulo columns to use. Columns may be column families and or column families and column qualifiers
     * 
     * @param columns
     *            The String containing the columns to pull from accumulo
     */
    private void setColumns(String columns) {
        if (null != columns) {
            this.columns = columns;
        }
    }
    
    /**
     * Set the buckets to use to sort the accumulo data. A row of data should be in the oldest bucket possible. The buckets will be reversed sorted. If an null
     * or empty list is passed in it will be ignored. Entries less than two will be ignored.
     * 
     * @param unsorted
     *            - The int array that holds the day buckets
     */
    public void setBuckets(Integer[] unsorted) {
        if ((unsorted != null) && (unsorted.length > 0)) {
            List<Integer> tmp = sortAndReverseUnsortedBuckets(unsorted);
            filterBuckets(tmp);
        }
    }
    
    /**
     * Sort and reverse the Integer array. Return as a List
     * 
     * @param unsorted
     *            the array to sort
     * @return the sorted List
     */
    private List<Integer> sortAndReverseUnsortedBuckets(Integer[] unsorted) {
        List<Integer> tmp = Arrays.asList(unsorted);
        Collections.sort(tmp);
        Collections.reverse(tmp); // reverse the sorted order
        return (tmp);
    }
    
    /**
     * Remove any elements smaller than 2 from the List.
     * 
     * @param tmp
     *            - The List to filter
     */
    private void filterBuckets(List<Integer> tmp) {
        int cnt = 0;
        Integer[] filtered = new Integer[tmp.size()];
        for (int ii = 0; ii < tmp.size(); ii++) {
            int value = tmp.get(ii);
            if (value >= 2) {
                filtered[ii] = value;
                cnt++;
            } else {
                break; // don't need to continue since it's already sorted
            }
        }
        
        buckets = new Integer[cnt];
        System.arraycopy(filtered, 0, buckets, 0, buckets.length);
    }
    
    /**
     * Returns the reverse sorted bucket array
     * 
     * @return the reverse sorted bucket int array
     */
    public Integer[] getBuckets() {
        return (buckets);
    }
    
    /**
     * Add one or more column families or column family and qualifier to a scanner.
     * 
     * @param scanner
     *            to add columns to.
     * @return The scanner with columns
     */
    private Scanner addColumnsToScanner(Scanner scanner) {
        if ((null != columns) && (!columns.equals(("")))) {
            String[] cols = columns.split(",");
            for (String colStr : cols) {
                String[] parts = colStr.split(":");
                if (parts.length == 1) {
                    scanner.fetchColumnFamily(new Text(parts[0]));
                } else if (parts.length == 2) {
                    scanner.fetchColumn(new Text(parts[0]), new Text(parts[1]));
                }
            }
        }
        
        return (scanner);
    }
    
    /**
     * Pull data from accumulo, create a collection of delete cmds for th accumulo script for {@code indexes > 1 day}
     */
    public void extractDataFromAccumulo() {
        dataBuckets = new ArrayList[buckets.length];
        for (int ii = 0; ii < dataBuckets.length; ii++) {
            dataBuckets[ii] = new ArrayList<>();
        }
        
        Scanner scanner = null;
        
        try {
            Authorizations userAuthorizations = accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami());
            scanner = accumuloClient.createScanner(tableName, userAuthorizations);
            scanner = addColumnsToScanner(scanner);
            Range range = new Range();
            scanner.setRange(range);
            
            long currentTime = System.currentTimeMillis();
            long mostRecent = currentTime - (MILLIS_IN_DAY * buckets[buckets.length - 1]);
            for (Map.Entry<Key,Value> entry : scanner) {
                long rowAge = entry.getKey().getTimestamp();
                if (rowAge < mostRecent) { // ** ignore data less than the newest bucket
                    Text deleteCommand = createDeleteCmd(entry);
                    int bucketIndex = 0;
                    // separate the data according to the bucket they will be dropped into
                    for (int age : buckets) {
                        if (rowAge <= (currentTime - (age * MILLIS_IN_DAY))) { // don't delete indexes < 1 day old
                            dataBuckets[bucketIndex].add((deleteCommand.toString()).replace("\0", "\\x00"));
                            break;
                        }
                        bucketIndex++;
                    }
                }
            }
            
        } catch (AccumuloException ae) {
            log.error("Authorization error.");
        } catch (AccumuloSecurityException ase) {
            log.error("Accumulo security error.");
        } catch (TableNotFoundException tnfe) {
            log.error("Unable to find " + tableName + " in our accumulo instance.");
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }
    
    /**
     * Creates the delete statements that go into the script.
     * 
     * @param entry
     *            The indexs from the accumulo table
     * @return the Text instance with the delete command
     */
    private Text createDeleteCmd(Map.Entry<Key,Value> entry) {
        Text cmd;
        if ((null != entry.getKey().getColumnVisibility()) && (!entry.getKey().getColumnVisibility().toString().equals(("")))) {
            cmd = new Text("    delete " + entry.getKey().getRow() + " " + entry.getKey().getColumnFamily() + " " + entry.getKey().getColumnQualifier()
                            + " -l " + entry.getKey().getColumnVisibility());
        } else {
            cmd = new Text("    delete " + entry.getKey().getRow() + " " + entry.getKey().getColumnFamily() + " " + entry.getKey().getColumnQualifier());
        }
        
        return (cmd);
    }
    
    /**
     * Logs a summary of how the indexes fit into the age buckets
     * 
     * @return a String containing the summary
     */
    public String logAgeSummary() {
        
        StringBuilder sb = new StringBuilder();
        for (int ii = buckets.length - 1; ii >= 0; --ii) {
            sb.append(String.format("\nIndexes older than %1$-3d %2$-6s %3$10d", buckets[ii], "days:", dataBuckets[ii].size()));
        }
        sb.append("\n");
        
        String summary = sb.toString();
        log.info(summary);
        
        return (summary);
    }
    
    /**
     * Creates a file containing an accumulo shell script that can be run to remove "old" indexes. There are comments in the script so only a subset could be
     * run.
     * 
     * @param fileName
     *            The name of the file to create
     */
    public void createAccumuloShellScript(String fileName) {
        if ((null != fileName) && (!fileName.equals(""))) {
            PrintWriter pw = null;
            try {
                pw = new PrintWriter(new File(fileName));
                pw.println("# Run the following commands in an accummulo shell.\n" + "# The lines starting with '#' " + "will be ignored.\n");
                pw.println("table " + tableName);
                
                for (int ii = 0; ii < buckets.length; ii++) {
                    pw.println("# Indexes older than " + buckets[ii] + " days:");
                    ArrayList<String> al = dataBuckets[ii];
                    for (String row : al) {
                        pw.println(row);
                    }
                }
                pw.println();
            } catch (FileNotFoundException fnfe) {
                System.err.println("Error:  Could not find " + fileName + ".\n\n");
            } finally {
                if (null != pw) {
                    pw.close();
                }
            }
        } else {
            log.info("Cannot create output file.  FileName is not defined.");
        }
    }
    
    /**
     * Options for running the progam. If no password is given it will be prompted for at the console. if fileName is not given the shell script will not be
     * created. If buckets is not passed in then the default will be used.
     * 
     * @return an Options object with all the possible command line options
     */
    private static Options buildOptions() {
        Options options = new Options();
        
        // Instance instance, String tableName, String columns, String userName, PasswordToken password
        options.addOption("i", "instanceName", true, "Name of the accumulo instance.");
        options.addOption("z", "zooKeepers", true, "ZooKeperServer(s)");
        options.addOption("t", "tableName", true, "Name of the table to scan.");
        options.addOption("c", "columns", true, "Comma separated list of column families.");
        options.addOption("u", "userName", true, "Accumulo user name.");
        options.addOption("p", "password", true, "Password for accumulo user.");
        options.addOption("f", "fileName", true, "fully qualified output filename.");
        options.addOption("b", "buckets", true, "comma separated int of days (descending order)");
        
        return (options);
    }
    
    /**
     * Displays an usage message.
     */
    private static void usage() {
        System.out.println("\n\nUsage: java AccumuloIndexAgeDisplay -i instanceName -z zooKeeperServer(s) -t tableName "
                        + "-u userName -c columns <-p password> <-f outputFileName> <-b dayBuckets>");
        System.out.println("columns are comma separated columns (eg. a,b:cqual,d).");
        System.out.println("dayBuckets are optional descending comma separated integers.  Defaults are 180,90,60,30,14,7,2\n\n");
        System.exit(0);
    }
    
    /**
     * Converts the comma separated String to an arrary of ints
     * 
     * @param cmd
     *            The command line object possibly containing the buckets
     * @return The array of ints which is are bucket array
     */
    private static Integer[] getBucketArray(CommandLine cmd) {
        String b = cmd.getOptionValue("b");
        String[] parts = b.split(",");
        int len = parts.length;
        Integer[] buf = new Integer[len];
        for (int ii = 0; ii < len; ii++) {
            buf[ii] = Integer.valueOf(parts[ii]);
        }
        
        return (buf);
    }
    
    /**
     * Prompts the user for the password at the console
     * 
     * @return The PasswordToken created from the password String
     */
    private static PasswordToken getPasswordFromConsole() {
        Console console = System.console();
        if (null == console) {
            log.error("Could not get Console instance.  Exiting...");
            System.exit(0);
        }
        char passwordArray[] = console.readPassword("\nEnter accumulo password: ");
        
        return (new PasswordToken(new String(passwordArray)));
    }
    
    /**
     * Our main method
     * 
     * @param args
     *            - command line arguments
     */
    public static void main(String[] args) {
        Options options = AccumuloIndexAgeDisplay.buildOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        
        String instanceName = null, zooKeepers = null, tableName = null, columns = null, userName = null, fileName = null;
        PasswordToken password;
        Integer[] bucket;
        try {
            cmd = parser.parse(options, args);
            
            if (cmd.hasOption("z")) {
                zooKeepers = cmd.getOptionValue("z");
            } else {
                usage();
            }
            if (cmd.hasOption("i")) {
                instanceName = cmd.getOptionValue("i");
            } else {
                usage();
            }
            if (cmd.hasOption("t")) {
                tableName = cmd.getOptionValue("t");
            } else {
                usage();
            }
            if (cmd.hasOption("c")) {
                columns = cmd.getOptionValue("c");
            } else {
                usage();
            }
            if (cmd.hasOption("u")) {
                userName = cmd.getOptionValue("u");
            } else {
                usage();
            }
            if (cmd.hasOption("p")) {
                password = new PasswordToken(cmd.getOptionValue("p"));
            } else {
                // usage();
                password = getPasswordFromConsole();
            }
            if (cmd.hasOption("f")) {
                fileName = cmd.getOptionValue("f");
            }
            if (cmd.hasOption("b")) {
                bucket = getBucketArray(cmd);
            } else {
                bucket = new Integer[0];
            }
            
            try (AccumuloIndexAgeDisplay aiad = new AccumuloIndexAgeDisplay(instanceName, zooKeepers, tableName, columns, userName, password, bucket)) {
                aiad.extractDataFromAccumulo();
                aiad.logAgeSummary();
                aiad.createAccumuloShellScript(fileName);
            }
            
            /*
             * Running with maven will display the following exception: java.lang.InterruptedException: sleep interrupted at java.lang.Thread.sleep(Native
             * Method) at org.apache.accumulo.core.clientImpl.ThriftTransportPool$Closer.closeConnections(ThriftTransportPool.java:138) at
             * org.apache.accumulo.core.clientImpl.ThriftTransportPool$Closer.run(ThriftTransportPool.java:148) at java.lang.Thread.run(Thread.java:745)
             * 
             * From mail-archives.apache.org This appears to be a known issue with ZooKeeper and Thrift resources that are not cleaned up. The main thread
             * finishes, but before the exec plugin stopped the application, it tried and failed to interrupt the other non-daemon threads that were still
             * running.
             */
            
        } catch (ParseException pe) {
            log.error("Failed to parse command line.");
        }
    }
}
