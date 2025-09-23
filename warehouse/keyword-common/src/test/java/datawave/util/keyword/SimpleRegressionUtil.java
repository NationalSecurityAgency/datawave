package datawave.util.keyword;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;

//@formatter:off
/** A set of simple utilities for creating parameterized tests based on a series of input files stored in a directory
 *  on disk. {@link #getTestsForClassData(Class)} uses the test class name to find input and expected output files
 *  in a directory under <pre>target/test-classes</pre> based on the class' package and class names. Additionally,
 *  the methods {@link #getInputFileForTest(String, Class)}, {@link #getExpectedFileForTest(String, Class)}, and
 *  {@link #getOutputFileForTest(String, Class)} can be used to get File objects used for individual test outputs.
 *  <p/>
 *  A skeleton for a test that would use this class could look like:
 *  <pre>
 *  public class YakeKeywordRegressionTest {
 *     @ParameterizedTest
 *     @MethodSource({"findTests"})
 *     public void regressionTest(String testName) throws IOException {
 *         File inputFile = getInputFileForTest(testName, YakeKeywordRegressionTest.class);
 *         File expectedFile = getExpectedFileForTest(testName, YakeKeywordRegressionTest.class);
 *         File outputFile = getOutputFileForTest(testName, YakeKeywordRegressionTest.class);
 *
 *         String input = IOUtils.toString(inputFile.toURI(), StandardCharsets.UTF_8);
 *         String expectedRaw = IOUtils.toString(inputFile.toURI(), StandardCharsets.UTF_8);
 *         // (do something with input and output here)
 *     }
 *
 *     public static List<String> findTests() {
 *         return getTestsForClassData(YakeKeywordRegressionTest.class);
 *     }
 *  }
 *  </pre>
 */
// @formatter:on
public class SimpleRegressionUtil {
    public static final String INPUT_BASE_DIR = "target/test-classes";
    public static final String OUTPUT_BASE_DIR = "target/test-output";
    public static final String INPUT_SUFFIX = "-input.txt";
    public static final String EXPECTED_SUFFIX = "-expected.txt";
    public static final String OUTPUT_SUFFIX = "-output.txt";

    /**
     * Given a test class, find the input and expected data for that class and generate a test name based on each instance of data. Input data is expected to be
     * found in src/test/resources in a directory based on the package and class name of the provided class. Each file should end with
     *
     * <pre>
     * -input.txt.
     * </pre>
     *
     * This method will attempt to find corresponding expected output for the given input using the {@link #getValidatedTestName(Class, File)} method, which
     * checks to see if a file with the test name and
     *
     * <pre>
     * -expected
     * </pre>
     *
     * suffix exists in the same directory as the input files found.
     *
     * @param clazz
     *            the name of the class executing the test.
     * @return a list of valid test names for the specified class.
     * @throws IllegalStateException
     *             if the corresponding expected output file for an input file is not found.
     */
    public static List<String> getTestsForClassData(Class<?> clazz) {
        String classBasedPath = getClassBasedPath(clazz);
        File inputBase = new File(INPUT_BASE_DIR, classBasedPath);
        final List<String> testNames = new ArrayList<>();
        if (inputBase.exists() && inputBase.isDirectory()) {
            String[] list = inputBase.list();
            if (list != null) {
                Arrays.stream(list).filter(s -> s.endsWith(INPUT_SUFFIX)).map(s -> new File(inputBase, s)).map(s -> getValidatedTestName(clazz, s))
                                .forEach(testNames::add);

            }
        }
        return testNames;
    }

    /**
     * Get an input file based on a test name and the class specified, input files will be found in 'target/test-classes' in a directory based on the specified
     * class' package and name.
     *
     * @param testName
     *            the test name, typically from {@link #getTestsForClassData(Class)}
     * @param clazz
     *            the name of the class executing the test.
     * @return a file, whose location can be used to write test data.
     */
    public static File getInputFileForTest(String testName, Class<?> clazz) {
        String classBasedPath = getClassBasedPath(clazz);
        File inputBase = new File(INPUT_BASE_DIR, classBasedPath);
        return new File(inputBase, testName + INPUT_SUFFIX);
    }

    /**
     * Get an expected file based on a test name and the class specified, expected files will be found in 'target/test-classes' in a directory based on the
     * specified class' package and name. They must exist at the beginning of a test and be used to validate the component's output.
     *
     * @param testName
     *            the test name, typically from {@link #getTestsForClassData(Class)}
     * @param clazz
     *            the name of the class executing the test.
     * @return a file, whose location can be used to write test data.
     */
    public static File getExpectedFileForTest(String testName, Class<?> clazz) {
        String classBasedPath = getClassBasedPath(clazz);
        File inputBase = new File(INPUT_BASE_DIR, classBasedPath);
        return new File(inputBase, testName + EXPECTED_SUFFIX);
    }

    /**
     * Get an output file based on a test name and the class specified, output files will be found in 'target/test-output' in a directory based on the specified
     * class' package and name. They will typically not exist at the beginning of a test, but this provides a standard location for a test to write its results.
     *
     * @param testName
     *            the test name, typically from {@link #getTestsForClassData(Class)}
     * @param clazz
     *            the name of the class executing the test.
     * @return a file, whose location can be used to write test data.
     */
    public static File getOutputFileForTest(String testName, Class<?> clazz) {
        String classBasedPath = getClassBasedPath(clazz);
        File outputBase = new File(OUTPUT_BASE_DIR, classBasedPath);
        return new File(outputBase, testName + OUTPUT_SUFFIX);
    }

    /**
     * Convenience method to write test output data to the file determined by {@link #getOutputFileForTest(String, Class)} will attempt to create the parent
     * output directory if it does not exist.
     *
     * @param output
     *            the output to write.
     * @param testName
     *            the current test name.
     * @param clazz
     *            the name of the class executing the test.
     * @throws IOException
     */
    public static void writeTestOutput(String output, String testName, Class<?> clazz) throws IOException {
        File outputFile = getOutputFileForTest(testName, clazz);
        File outputBase = outputFile.getParentFile();
        boolean result = outputBase.mkdirs();
        if (outputBase.exists() && outputBase.isDirectory()) {
            try (FileWriter writer = new FileWriter(outputFile)) {
                IOUtils.write(output, writer);
            }
        } else {
            throw new IOException("Output base directory does not exist or is not a directory: " + outputBase);
        }
    }

    /**
     * Find the test names to run after validating that the expected files exist.
     *
     * @param clazz
     *            the name of the class executing the test.
     * @param inputFile
     *            the file we'll derive the test name from.
     * @return a test name based on the input file
     * @throws IllegalStateException
     *             if the expected file does not exist.
     */
    protected static String getValidatedTestName(Class<?> clazz, File inputFile) {
        String testName = inputFile.getName().replace(INPUT_SUFFIX, "");
        File expectedFile = getExpectedFileForTest(testName, clazz);
        if (expectedFile.exists() && expectedFile.isFile()) {
            return testName;
        }
        throw new IllegalStateException("Unable to find expected test input: " + expectedFile);
    }

    /**
     * Get a path based on the specified classes package and class name
     *
     * @param clazz
     *            the name of the class executing the test.
     * @return the class-bathed path to use for input and output.
     */
    protected static String getClassBasedPath(Class<?> clazz) {
        return clazz.getName().replace('.', '/');
    }

}
