package datawave.core.common.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.shared.SharedValue;
import org.apache.curator.framework.recipes.shared.SharedValueListener;
import org.apache.curator.framework.recipes.shared.SharedValueReader;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;

/**
 *
 *
 */
public class SharedTriState implements Closeable, SharedTriStateReader, Listenable<SharedTriStateListener> {

    private static Logger log = LoggerFactory.getLogger(SharedTriState.class);

    public enum STATE {
        NEEDS_UPDATE(0), UPDATING(1), UPDATED(2);

        private final int value;
        private static final Map<Integer,STATE> typesByValue = new HashMap<>();
        static {
            for (STATE state : STATE.values()) {
                typesByValue.put(state.value, state);
            }
        }

        STATE(final int newValue) {
            this.value = newValue;
        }

        public int getValue() {
            return this.value;
        }

        public static STATE forValue(int value) {
            return typesByValue.get(value);
        }
    }

    private final Map<SharedTriStateListener,SharedValueListener> listeners = Maps.newConcurrentMap();
    private final SharedValue sharedValue;

    public SharedTriState(CuratorFramework client, String path, STATE seedValue) {
        this.sharedValue = new SharedValue(client, path, toBytes(seedValue));
    }

    public STATE getState() {
        if (log.isDebugEnabled()) {
            log.debug("in getState, sharedValue has {}", Arrays.toString(sharedValue.getValue()));
        }
        return fromBytes(this.sharedValue.getValue());
    }

    public void setState(STATE newState) throws Exception {
        this.sharedValue.setValue(toBytes(newState));
        log.debug("setState( {} )", newState);
    }

    public boolean trySetState(STATE newState) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("trySetState( {} )", newState);
        }
        return this.sharedValue.trySetValue(toBytes(newState));
    }

    public void addListener(SharedTriStateListener listener) {
        this.addListener(listener, MoreExecutors.directExecutor());
    }

    public void addListener(final SharedTriStateListener listener, Executor executor) {
        SharedValueListener valueListener = new SharedValueListener() {
            public void valueHasChanged(SharedValueReader sharedValue, byte[] newValue) throws Exception {
                if (log.isDebugEnabled()) {
                    log.debug("valueHasChanged in {} to {}", Arrays.toString(sharedValue.getValue()), Arrays.toString(newValue));
                }

                listener.stateHasChanged(SharedTriState.this, SharedTriState.fromBytes(newValue));
            }

            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                listener.stateChanged(client, newState);
            }
        };
        this.sharedValue.getListenable().addListener(valueListener, executor);
        this.listeners.put(listener, valueListener);
    }

    public void removeListener(SharedTriStateListener listener) {
        this.listeners.remove(listener);
    }

    public void start() throws Exception {
        this.sharedValue.start();
    }

    public void close() throws IOException {
        this.sharedValue.close();
    }

    private static byte[] toBytes(STATE value) {
        return new byte[] {(byte) value.getValue()};
    }

    private static STATE fromBytes(byte[] bytes) {
        if (log.isDebugEnabled()) {
            log.debug("fromBytes( {} ) and STATE: {}", Arrays.toString(bytes), STATE.forValue((int) bytes[0]));
        }
        return STATE.forValue((int) bytes[0]);
    }

    @Override
    public String toString() {
        return "SharedTriState{" + "listeners=" + listeners + ", sharedValue=" + Arrays.toString(sharedValue.getValue()) + " which is "
                        + fromBytes(sharedValue.getValue()) + '}';
    }
}
