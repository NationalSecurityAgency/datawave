package datawave.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;

/**
 * Runs a configuration comparison between two DATA-config.xml files.
 */
public class RunCompare {

    private static final String SEP = "=======================";

    /**
     * Runs a comparison of the two given files and outputs results to stdout.
     *
     * @param filename1
     *            a file name
     * @param filename2
     *            a different file name
     * @throws IOException
     *             if either file cannot be found
     */
    public void run(String filename1, String filename2) throws IOException {
        Configuration c1 = fileBasedConf(filename1);
        Configuration c2 = fileBasedConf(filename2);

        System.out.println(String.format("Comparing (Left=%s) and (Right=%s)", filename1, filename2));
        CompareResult result = new DataTypeConfigCompare().run(c1, c2);

        printResultSection("Same", result.getSame());
        printResultSection("Different", result.getDiff());
        printResultSection(filename1 + " only", result.getLeftOnly());
        printResultSection(filename2 + " only", result.getRightOnly());
    }

    /**
     * Prints out a section of comparison results.
     *
     * @param title
     *            the section title
     * @param entries
     *            the content of the section
     */
    private void printResultSection(String title, Collection<String> entries) {
        System.out.println(title);
        System.out.println(SEP);
        for (String entry : entries) {
            System.out.println(entry);
        }
        System.out.println();
    }

    /**
     * Creates a new configuration from a local file.
     *
     * @param filename
     *            the file name
     * @return configuration a new configuration
     * @throws FileNotFoundException
     *             if the file is not found
     */
    private Configuration fileBasedConf(String filename) throws FileNotFoundException {
        Configuration c = new Configuration();
        c.addResource(new FileInputStream(filename));
        return c;
    }

    /**
     * Prints this tool's usage and exits.
     */
    private static void printUsageAndExit() {
        System.out.println("RunCompare expects two arguments: <config1> and <config2>");
        System.out.println("Where both <config1> and <config2> are DataWave config.xml files.");
        System.exit(0);
    }

    /**
     * Verifies that the given filename 1) exists and 2) can be read by this user.
     *
     * @param filename
     *            the file name
     * @return True if the file exists and can be read, false, otherwise.
     */
    private static boolean verifyFile(String filename) {
        File f = new File(filename);
        boolean isReadable = f.exists() && f.canRead();
        if (!isReadable)
            System.out.println("Could not read file: " + filename);
        return isReadable;
    }

    /**
     * Entry point
     *
     * @param args
     *            command line args
     * @throws IOException
     *             if something goes wrong
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 2 || !verifyFile(args[0]) || !verifyFile(args[1]))
            printUsageAndExit();

        new RunCompare().run(args[0], args[1]);
    }
}
