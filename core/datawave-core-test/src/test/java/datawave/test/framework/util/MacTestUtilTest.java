package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.jupiter.api.Test;

class MacTestUtilTest {

    private static final String TABLE = "shard";

    /**
     * Setting a property that already exists leaves the key visible under its old value until the change propagates, so a wait that only checks for key
     * presence returns immediately and hands the test the previous value.
     */
    @Test
    void testWaitsForTheNewValueWhenTheKeyAlreadyHoldsTheOldValue() {
        LaggingTableOperations lagging = new LaggingTableOperations(Map.of("table.custom.foo", "old"), 3);
        TableOperations tops = lagging.proxy();

        MacTestUtil.addPropertiesAndWait(tops, TABLE, Map.of("table.custom.foo", "new"));

        assertEquals("new", lagging.visible.get("table.custom.foo"));
    }

    /**
     * The wait is not satisfied by the first property to land - every requested value has to be visible before it returns.
     */
    @Test
    void testWaitsForEveryRequestedValue() {
        LaggingTableOperations lagging = new LaggingTableOperations(Map.of("table.custom.foo", "old", "table.custom.bar", "old"), 2);
        TableOperations tops = lagging.proxy();

        Map<String,String> properties = Map.of("table.custom.foo", "new", "table.custom.bar", "newer");
        MacTestUtil.addPropertiesAndWait(tops, TABLE, properties);

        assertEquals("new", lagging.visible.get("table.custom.foo"));
        assertEquals("newer", lagging.visible.get("table.custom.bar"));
    }

    /**
     * A property whose value is unchanged is already propagated, so the wait returns without polling for it a second time.
     */
    @Test
    void testAnUnchangedValueIsAlreadySatisfied() {
        LaggingTableOperations lagging = new LaggingTableOperations(Map.of("table.custom.foo", "same"), 0);
        TableOperations tops = lagging.proxy();

        MacTestUtil.addPropertiesAndWait(tops, TABLE, Map.of("table.custom.foo", "same"));

        assertEquals("same", lagging.visible.get("table.custom.foo"));
        assertEquals(1, lagging.polls);
    }

    /**
     * A {@link TableOperations} stand-in that holds a set property back for a fixed number of polls, mimicking the delay between the call returning and the
     * change becoming visible. Only the two methods the wait relies on are implemented.
     */
    private static final class LaggingTableOperations implements InvocationHandler {

        private final Map<String,String> visible = new LinkedHashMap<>();
        private final Map<String,String> pending = new LinkedHashMap<>();
        private final int lagPolls;

        private int polls = 0;

        private LaggingTableOperations(Map<String,String> initial, int lagPolls) {
            this.visible.putAll(initial);
            this.lagPolls = lagPolls;
        }

        private TableOperations proxy() {
            return (TableOperations) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {TableOperations.class}, this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "setProperty":
                    pending.put((String) args[1], (String) args[2]);
                    return null;
                case "getProperties":
                    if (++polls > lagPolls) {
                        visible.putAll(pending);
                        pending.clear();
                    }
                    return snapshot();
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        }

        private List<Map.Entry<String,String>> snapshot() {
            List<Map.Entry<String,String>> entries = new ArrayList<>(visible.size());
            for (Map.Entry<String,String> entry : visible.entrySet()) {
                entries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
            }
            return entries;
        }
    }
}
