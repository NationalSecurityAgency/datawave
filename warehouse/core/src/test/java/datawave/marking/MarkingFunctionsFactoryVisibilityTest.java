package datawave.marking;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates the visibility/synchronization bug in {@link MarkingFunctionsFactory}.
 *
 * <p>
 * {@link MarkingFunctionsFactory#createMarkingFunctions()} is {@code synchronized} and reads the static {@code markingFunctions} field under the class lock.
 * However, {@link MarkingFunctionsFactory#postContruct()} writes that same static field without holding any lock and without a {@code volatile} declaration on
 * the field. Under the Java Memory Model, a write performed without a matching happens-before edge is not guaranteed to be visible to a reader that
 * synchronizes on a different (or no) monitor. In practice this means a thread calling {@code postContruct()} during CDI initialization can install the
 * CDI-provided {@code MarkingFunctions} instance, but a concurrent caller of {@code createMarkingFunctions()} may observe a stale {@code null} long after the
 * publish completed, which causes {@code createMarkingFunctions()} to fall through into its Spring-context-load branch and return either {@code null} (when no
 * Spring context is available in the test VM) or a different {@code MarkingFunctions} instance than the one the writer published.
 * </p>
 *
 * <p>
 * The two structural tests below assert the contract directly and fail deterministically on the unfixed code:
 * </p>
 * <ul>
 * <li>{@link #markingFunctionsFieldIsVolatile()} — asserts the static {@code markingFunctions} field is declared {@code volatile}.</li>
 * <li>{@link #postContructIsSynchronized()} — asserts {@code postContruct()} is declared {@code synchronized}, matching {@code createMarkingFunctions()}.</li>
 * </ul>
 * <p>
 * On the unfixed code both tests fail because the field is declared without {@code volatile} and the method is declared without {@code synchronized}. After the
 * fix both tests pass.
 * </p>
 */
public class MarkingFunctionsFactoryVisibilityTest {

    private static final Logger log = LoggerFactory.getLogger(MarkingFunctionsFactoryVisibilityTest.class);

    private MarkingFunctions<?> originalMarkingFunctions;
    private MarkingFunctions<?> originalApplicationMarkingFunctions;
    private Field markingFunctionsField;
    private Field applicationMarkingFunctionsField;

    @Before
    public void snapshotAndInjectSentinel() throws Exception {
        markingFunctionsField = MarkingFunctionsFactory.class.getDeclaredField("markingFunctions");
        markingFunctionsField.setAccessible(true);
        applicationMarkingFunctionsField = MarkingFunctionsFactory.class.getDeclaredField("applicationMarkingFunctions");
        applicationMarkingFunctionsField.setAccessible(true);

        // Snapshot the original static state so we can restore it after the test regardless of outcome.
        originalMarkingFunctions = (MarkingFunctions<?>) markingFunctionsField.get(null);
        originalApplicationMarkingFunctions = (MarkingFunctions<?>) applicationMarkingFunctionsField.get(null);
    }

    @After
    public void restoreOriginalState() throws Exception {
        if (markingFunctionsField != null) {
            markingFunctionsField.set(null, originalMarkingFunctions);
        }
        if (applicationMarkingFunctionsField != null) {
            applicationMarkingFunctionsField.set(null, originalApplicationMarkingFunctions);
        }
    }

    /**
     * Asserts that the static {@code markingFunctions} field is declared {@code volatile}. Without {@code volatile}, writes by {@code postContruct()} are not
     * guaranteed to be visible to readers in {@code createMarkingFunctions()} that synchronize on the class monitor, because {@code synchronized} on the
     * reader only establishes a happens-before edge against other callers that also synchronize on the same monitor — and {@code postContruct()} does not.
     */
    @Test
    public void markingFunctionsFieldIsVolatile() {
        int modifiers = markingFunctionsField.getModifiers();
        assertTrue("MarkingFunctionsFactory.markingFunctions must be declared volatile so that unsynchronized writes by postContruct() are visible to "
                        + "synchronized readers in createMarkingFunctions(). Found modifiers: " + Modifier.toString(modifiers),
                        Modifier.isVolatile(modifiers));
    }

    /**
     * Asserts that {@code postContruct()} is declared {@code synchronized}, matching the synchronization already present on
     * {@code createMarkingFunctions()}. Without this, the static-field write inside {@code postContruct()} races against the read inside
     * {@code createMarkingFunctions()} with no happens-before edge in either direction.
     */
    @Test
    public void postContructIsSynchronized() throws Exception {
        Method postContruct = MarkingFunctionsFactory.class.getDeclaredMethod("postContruct");
        int modifiers = postContruct.getModifiers();
        assertTrue("MarkingFunctionsFactory.postContruct() must be declared synchronized so its write to the static markingFunctions field has a "
                        + "happens-before edge with respect to createMarkingFunctions(). Found modifiers: " + Modifier.toString(modifiers),
                        Modifier.isSynchronized(modifiers));
    }
}
