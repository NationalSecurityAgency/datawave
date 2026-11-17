package datawave.microservice.annotationCache.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PersistenceModeTest {
    @Test
    void modesRoundTripThroughWireValues() {
        for (PersistenceMode mode : PersistenceMode.values()) {
            assertEquals(mode, PersistenceMode.fromValue(mode.value()));
        }
    }

    @Test
    void rejectsNullEmptyAndUnknownWireValues() {
        assertThrows(IllegalArgumentException.class, () -> PersistenceMode.fromValue(null));
        assertThrows(IllegalArgumentException.class, () -> PersistenceMode.fromValue(""));
        assertThrows(IllegalArgumentException.class, () -> PersistenceMode.fromValue("WRITE_THROUGH"));
    }
}
