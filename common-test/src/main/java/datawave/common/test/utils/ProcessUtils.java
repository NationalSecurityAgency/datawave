package datawave.common.test.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProcessUtils {

    public static final int SYSTEM_EXIT_MINUS_ONE = 255;
    public static final int SYSTEM_EXIT_MINUS_TWO = 254;
    public static final int SYSTEM_EXIT_ONE = 1;

    public static final String JAVA_PATH = getJavaPath();
    public static final String DEBUG_CONFIG = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:8000";

    private ProcessUtils() {
        throw new IllegalStateException("Do not instantiate utility class");
    }

    public static List<String> buildApplicationCommandLine(String clzName, List<String> systemProperties, boolean setupDebugger) {

        List<String> results = new ArrayList<>();

        results.add(JAVA_PATH);

        if (setupDebugger) {
            results.add(DEBUG_CONFIG);
        }

        results.add("-cp");
        results.add(System.getProperty("java.class.path"));

        for (String property : systemProperties) {
            results.add(property);
        }

        results.add(clzName);

        return results;
    }

    /**
     * Returns the path of the currently executing JVM, or simply "java" if the path can't be verified for whatever reason
     */
    public static String getJavaPath() {
        // Sanity check current 'java.home' path
        try {
            File javaBinary = Paths.get(System.getProperty("java.home"), "bin", "java").toFile();
            if (javaBinary.isFile()) {
                return javaBinary.getAbsolutePath();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Worst case, rely on system PATH to resolve java and hope for the best
        return "java";
    }

    public static String[] convertCommandLine(List<String> arguments) {

        return arguments.toArray(new String[arguments.size()]);
    }

    public static Process runInstance(String[] cmdArray, Map<String,String> newEnvironment, List<String> dropFromEnvironment, File workingDirectory)
                    throws IOException {

        ProcessBuilder pb = new ProcessBuilder(cmdArray);

        Map<String,String> environment = pb.environment();

        for (Map.Entry<String,String> item : newEnvironment.entrySet()) {

            environment.put(item.getKey(), item.getValue());
        }

        for (String drop : dropFromEnvironment) {

            environment.remove(drop);
        }

        pb.directory(workingDirectory);

        return pb.start();
    }

    public static List<String> getStandardOutDumps(Process proc) throws IOException {

        return getOutputDumps(new InputStreamReader(proc.getInputStream()));
    }

    public static List<String> getStandardErrorDumps(Process proc) throws IOException {

        return getOutputDumps(new InputStreamReader(proc.getErrorStream()));
    }

    protected static List<String> getOutputDumps(InputStreamReader isr) throws IOException {

        List<String> messages = new ArrayList<>();

        BufferedReader br = new BufferedReader(isr);

        String line = null;

        while (null != (line = br.readLine())) {

            if (!line.startsWith("Cobertura: ")) {

                messages.add(line);
            }
        }

        return messages;
    }

}
