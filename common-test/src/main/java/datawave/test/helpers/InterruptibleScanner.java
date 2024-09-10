package datawave.test.helpers;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * <p>
 * This mock scanner can be interrupted at any time, refreshing/cloning the iterator and resetting the range. This is to help to test tear down scenarios, the
 * deep copy usage and sequential key output.
 * </p>
 *
 * <p>
 * Create this scanner with an existing <code>MockScanner</code> from the mock package. At any point in testing a new iterator can be generated using the the
 * interrupt method. The old iterator will still be usable but will not actually test the interruption scenario.
 * </p>
 *
 * <p>
 * Example usage
 * </p>
 *
 * <pre>
 * Scanner s = conn.createScanner(TABLE_NAME, authorizations);
 * InterruptibleScanner scanner = new InterruptibleScanner(s);
 * // Configure scanner, set range
 * Iterator&lt;Entry&lt;Key,Value&gt;&gt; iter = scanner.iterator();
 * // Iterate over key values, test some values
 * iter = scanner.interrupt(); // Simulate a tear down
 * // Test that this value is the expected next value, and that it has not returned a duplicate
 * </pre>
 */
public interface InterruptibleScanner extends Scanner {

    Iterator<Entry<Key,Value>> interrupt();
}
