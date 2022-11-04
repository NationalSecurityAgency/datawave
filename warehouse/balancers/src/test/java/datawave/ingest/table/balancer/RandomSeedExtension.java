package datawave.ingest.table.balancer;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.lang.reflect.Field;

public class RandomSeedExtension implements TestWatcher {
    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        try {
            Field f = context.getRequiredTestInstance().getClass().getDeclaredField("randomSeed");
            f.setAccessible(true);
            System.err.println("Random seed for this test was: " + f.get(context.getRequiredTestInstance()));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
