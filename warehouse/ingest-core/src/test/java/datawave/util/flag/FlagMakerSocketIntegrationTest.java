package datawave.util.flag;

import static datawave.util.flag.config.FlagMakerConfigUtilityTest.TEST_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.common.test.integration.IntegrationTest;

/**
 * Tests FlagMaker's behavior in response to socket messaging. It currently supports "shutdown" and "kill dataTypeName"
 */
@Category(IntegrationTest.class)
public class FlagMakerSocketIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(FlagMakerSocketIntegrationTest.class);

    // FlagMaker's interval between attempts to either process files or, if signaled, to shutdown
    private static final long FLAG_MAKER_INTERVAL = TimeUnit.MILLISECONDS.toMillis(200);

    // Two intervals should be plenty of time for the FlagMaker to receive and react to a socket message
    private static final long MINIMUM_WAIT_TIME = FLAG_MAKER_INTERVAL * 2;

    private static final long SOCKET_CONNECTION_RETRY_INTERVAL = TimeUnit.MILLISECONDS.toMillis(500);

    private static int socketPort = 22222;

    @Rule
    // Kill the unit test method if it's still going after four seconds
    public Timeout timeout = new Timeout(4, TimeUnit.SECONDS);

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Rule
    public TestName testName = new TestName();

    private FlagFileTestSetup flagFileTestSetup;
    private Thread thread;

    @Before
    public void startFlagMakerOneNewPort() {
        socketPort++;
        thread = startFlagMakerThread();
    }

    @After
    public void stopThreadIfRunning() {
        if (thread.isAlive()) {
            thread.interrupt();
        }
    }

    @Before
    public void createInputFiles() throws Exception {
        // Create only four input files (2 for foo, 2 for bar)
        // The FlagMaker configuration attempts to create flag files of size 10

        // @formatter:off
        flagFileTestSetup = new FlagFileTestSetup().withTestFlagMakerConfig()
                .withFilesPerDay(2)
                .withNumDays(1);
        // @formatter:on

        flagFileTestSetup.createTestFiles();
    }

    @After
    public void cleanupCreatedFiles() throws IOException {
        flagFileTestSetup.deleteTestDirectories();
    }

    @Test
    public void baselineRunsWithoutShutdownOrFilesOutput() throws Exception {
        // verifies that a simple flag maker will run indefinitely unless interrupted
        // and won't create flag files (due to insufficient input files)

        // verify it's still running 2 seconds later
        thread.join(TimeUnit.SECONDS.toMillis(2));
        assertTrue(thread.isAlive());

        thread.interrupt();

        assertFlagMakerStops(thread);

        assertZeroFlagFilesCreated();
    }

    @Test
    public void shutdownViaSocketStopsFlagMaker() throws InterruptedException, IOException {
        writeToSocket(retryConnectingToSocket(), "shutdown");
        assertFlagMakerStops(thread);

        assertZeroFlagFilesCreated();
    }

    @Test
    public void ignoresUnknownMessage() throws InterruptedException, IOException {
        writeToSocket(retryConnectingToSocket(), "ignore");
        assertFlagMakerStaysAlive(thread);

        assertZeroFlagFilesCreated();
    }

    @Test
    public void startAgainWithShutdownArgStopsFlagMakerWithSystemExit() throws InterruptedException, IOException {
        exit.expectSystemExitWithStatus(0);

        // ensure socket is ready
        retryConnectingToSocket().close();

        // start a duplicative FlagMaker but with the shutdown flag
        startFlagMakerThread("-shutdown");
        assertFlagMakerStops(thread);

        assertZeroFlagFilesCreated();
    }

    @Test
    public void testKickDataTypeCausesFlagFileWrite() throws InterruptedException, IOException {
        // send "kick" signal to socket
        writeToSocket(retryConnectingToSocket(), "kick foo");
        assertFlagMakerStaysAlive(thread);

        // should have forced some output
        assertNumberOfFlagFilesCreated(1);
    }

    @Test
    public void testKickWithoutDataTypeIgnored() throws InterruptedException, IOException {
        // send "kick" signal to socket, without specifying datatype
        writeToSocket(retryConnectingToSocket(), "kick");
        assertFlagMakerStaysAlive(thread);

        // no impact without datatype
        assertZeroFlagFilesCreated();
    }

    private Thread startFlagMakerThread(String... additionalArguments) {
        Thread thread = createFlagMakerThread(additionalArguments);
        thread.start();
        assertTrue(thread.isAlive());
        return thread;
    }

    private Thread createFlagMakerThread(String... additionalArguments) {
        return new Thread(() -> {
            try {
                String[] args = {"-flagConfig", TEST_CONFIG, "-socket", Integer.toString(socketPort), "-sleepMilliSecs", Long.toString(FLAG_MAKER_INTERVAL)};
                if (additionalArguments != null && additionalArguments.length > 0) {
                    args = Stream.concat(Arrays.stream(args), Arrays.stream(additionalArguments)).toArray(String[]::new);
                }
                FlagMaker.main(args);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void assertFlagMakerStaysAlive(Thread thread) throws InterruptedException {
        // verify it's still running after two full flag maker intervals
        Thread.sleep(MINIMUM_WAIT_TIME);
        assertTrue(thread.isAlive());
    }

    private void assertFlagMakerStops(Thread thread) throws InterruptedException {
        // verify it stopped. wait up to two full flag maker intervals, longer than worst case scenario
        Thread.sleep(MINIMUM_WAIT_TIME);
        assertFalse(thread.isAlive());
    }

    private Socket retryConnectingToSocket() {
        boolean connected = false;
        Socket socket = null;
        while (!connected) {
            try {
                Thread.sleep(SOCKET_CONNECTION_RETRY_INTERVAL);
                socket = new Socket("localhost", socketPort);
                connected = true;
            } catch (IOException | InterruptedException e) {
                // try again until test case timeout
                LOG.info("Failed, will retrying connecting to {}", socketPort);
            }
        }
        return socket;
    }

    private void writeToSocket(Socket socket, final String message) throws IOException {
        OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream());
        out.write(message + "\n");
        out.flush();
    }

    private void assertZeroFlagFilesCreated() {
        // not enough input files exist for flag file to be written
        assertNumberOfFlagFilesCreated(0);
    }

    private void assertNumberOfFlagFilesCreated(int numberExpected) {
        List<File> files = FlagFileTestInspector.listFlagFiles(flagFileTestSetup.getFlagMakerConfig());
        assertEquals(FlagFileTestInspector.logFiles(files), numberExpected, files.size());
    }
}
