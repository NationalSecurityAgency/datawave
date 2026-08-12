package datawave.ingest.scripts;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the ingest scripts build a complete classpath before launching a DataWave main class. These scripts are never compiled, so a classpath defect
 * in one is invisible to the build and surfaces only as a {@code ClassNotFoundException} on a deployed cluster.
 */
class IngestScriptClasspathTest {

    /** The script tree, relative to the module basedir that surefire runs in. */
    private static final Path BIN_DIR = Paths.get("src", "main", "resources", "bin");

    /** Matches a shell assignment, including {@code export FOO=} and {@code FOO=$(...)} forms. */
    private static final Pattern ASSIGNMENT = Pattern.compile("^\\s*(?:export\\s+)?([A-Za-z_][A-Za-z0-9_]*)=");

    /** Matches an assignment building a classpath-like value; group 2 is the raw right-hand side. */
    private static final Pattern CLASSPATH_ASSIGNMENT = Pattern.compile("^\\s*(?:export\\s+)?(CLASSPATH|ADDJARS|LIBJARS)=(.*)$");

    /** Matches a variable reference in either {@code $FOO} or <code>${FOO}</code> form. */
    private static final Pattern REFERENCE = Pattern.compile("\\$\\{?([A-Za-z_][A-Za-z0-9_]*)\\}?");

    /** Matches the accumulo launcher being handed a DataWave class, after line continuations are folded away. */
    private static final Pattern ACCUMULO_LAUNCH = Pattern.compile("\\baccumulo\\s+datawave\\.[a-z0-9_.]*\\.[A-Z]");

    /**
     * Variables legitimately supplied by the surrounding deployment rather than by any script in this tree: operator-set environment, or the inherited
     * classpath itself when a script appends to what it was given.
     */
    private static final Set<String> EXTERNALLY_PROVIDED = new HashSet<>(
                    Arrays.asList("ACCUMULO_HOME", "ADDITIONAL_INGEST_LIBS", "CLASSPATH", "HADOOP_HOME", "JAVA_HOME", "ZOOKEEPER_HOME"));

    private static List<Script> scripts;

    @BeforeAll
    static void loadScripts() throws IOException {
        assertTrue(Files.isDirectory(BIN_DIR), BIN_DIR.toAbsolutePath() + " is not a directory; the test is looking in the wrong place");
        try (Stream<Path> tree = Files.walk(BIN_DIR)) {
            scripts = tree.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().endsWith(".sh")).map(Script::read)
                            .sorted((a, b) -> a.name.compareTo(b.name)).collect(Collectors.toList());
        }
        assertTrue(scripts.size() > 20, "expected the full ingest script tree, found only " + scripts.size() + " scripts");
    }

    /**
     * Asserts every variable used to build a classpath is assigned somewhere in the script tree. An unset variable expands to the empty string rather than
     * failing, so a typo silently drops a jar.
     */
    @Test
    void everyClasspathVariableIsAssignedSomewhere() {
        Set<String> assigned = scripts.stream().flatMap(s -> s.assignments().stream()).collect(Collectors.toSet());

        List<String> undefined = new ArrayList<>();
        for (Script script : scripts) {
            for (int i = 0; i < script.lines.size(); i++) {
                Matcher classpathLine = CLASSPATH_ASSIGNMENT.matcher(script.lines.get(i));
                if (!classpathLine.matches()) {
                    continue;
                }
                Matcher reference = REFERENCE.matcher(assignedValue(classpathLine.group(2)));
                while (reference.find()) {
                    String name = reference.group(1);
                    if (!assigned.contains(name) && !EXTERNALLY_PROVIDED.contains(name)) {
                        undefined.add(script.name + ":" + (i + 1) + " uses $" + name + ", which no script assigns");
                    }
                }
            }
        }

        assertTrue(undefined.isEmpty(), "Classpath entries reference variables that are never assigned, so they expand to an empty "
                        + "string and silently drop from the classpath:\n  " + String.join("\n  ", new TreeSet<>(undefined)));
    }

    /**
     * Asserts no script hands the {@code accumulo} launcher a hand-picked list of jars. Such a subset holds only until the main class, or something it calls,
     * reaches one class further, and {@code ingest-libs.sh} already builds the complete classpath.
     */
    @Test
    void noScriptBuildsItsOwnJarListForTheAccumuloLauncher() {
        List<String> offenders = new ArrayList<>();
        for (Script script : scripts) {
            for (int i = 0; i < script.lines.size(); i++) {
                Matcher classpathLine = CLASSPATH_ASSIGNMENT.matcher(script.lines.get(i));
                if (classpathLine.matches() && "ADDJARS".equals(classpathLine.group(1))) {
                    offenders.add(script.name + ":" + (i + 1) + " builds ADDJARS from individual jars");
                }
            }
        }

        assertTrue(offenders.isEmpty(), "These scripts assemble their own jar list instead of exporting the full CLASSPATH from " + "ingest-libs.sh:\n  "
                        + String.join("\n  ", offenders) + "\nSource ingest-libs.sh and 'export CLASSPATH' before invoking " + "the launcher instead.");
    }

    /**
     * Asserts every script using the {@code accumulo} launcher sources {@code ingest-libs.sh}, directly or through another script. The launcher takes its
     * classpath from the environment; scripts submitting through {@code hadoop jar ... -libjars} are excluded because they pass dependencies explicitly.
     */
    @Test
    void scriptsUsingTheAccumuloLauncherSourceIngestLibs() {
        List<String> offenders = new ArrayList<>();
        for (Script script : scripts) {
            if (ACCUMULO_LAUNCH.matcher(script.folded).find() && !sourcesIngestLibsTransitively(script, new HashSet<>())) {
                offenders.add(script.name);
            }
        }

        assertTrue(offenders.isEmpty(), "These scripts hand a DataWave class to the accumulo launcher but never source "
                        + "ingest-libs.sh, so they run with whatever CLASSPATH they inherited:\n  " + String.join("\n  ", offenders));
    }

    /** Walks the {@code . some-script.sh} chain looking for {@code ingest-libs.sh}. */
    private boolean sourcesIngestLibsTransitively(Script script, Set<String> visited) {
        if (!visited.add(script.name)) {
            return false;
        }
        Matcher sourced = Pattern.compile("^\\s*(?:\\.|source)\\s+\\S*?([A-Za-z0-9_.-]+\\.sh)", Pattern.MULTILINE).matcher(script.body);
        while (sourced.find()) {
            String target = sourced.group(1);
            if ("ingest-libs.sh".equals(target)) {
                return true;
            }
            for (Script candidate : scripts) {
                if (candidate.fileName.equals(target) && sourcesIngestLibsTransitively(candidate, visited)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns the portion of an assignment's right-hand side that is actually assigned. {@code FOO=value command args} carries a one-shot environment prefix
     * rather than assigning the whole line, so only the first token counts; whitespace inside quotes or a command substitution stays part of the value.
     */
    private static String assignedValue(String rawValue) {
        int depth = 0;
        char quote = 0;
        for (int i = 0; i < rawValue.length(); i++) {
            char c = rawValue.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '"' || c == '\'') {
                quote = c;
            } else if (c == '(' || c == '{') {
                depth++;
            } else if (c == ')' || c == '}') {
                depth--;
            } else if (Character.isWhitespace(c) && depth <= 0) {
                return rawValue.substring(0, i);
            }
        }
        return rawValue;
    }

    /** A single shell script, held as raw text, folded text, and lines so checks can report a line number. */
    private static final class Script {
        private final String name;
        private final String fileName;
        private final String body;
        /** {@link #body} with backslash line continuations joined, so a wrapped command matches as one line. */
        private final String folded;
        private final List<String> lines;

        private Script(Path path, String body) {
            this.name = BIN_DIR.relativize(path).toString();
            this.fileName = path.getFileName().toString();
            this.body = body;
            this.folded = body.replaceAll("\\\\\\r?\\n\\s*", " ");
            this.lines = Arrays.asList(body.split("\n", -1));
        }

        private static Script read(Path path) {
            try {
                return new Script(path, new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new IllegalStateException("Could not read " + path, e);
            }
        }

        private Set<String> assignments() {
            Set<String> names = new HashSet<>();
            for (String line : lines) {
                Matcher assignment = ASSIGNMENT.matcher(line);
                if (assignment.find()) {
                    names.add(assignment.group(1));
                }
            }
            return names;
        }
    }
}
