package datawave.test.framework.util;

import java.nio.charset.StandardCharsets;
import java.util.Date;

import com.google.common.base.Preconditions;

import datawave.table.hash.UID;

public class UidGenerator {

    private UidGenerator() {
        // enforce static access
    }

    /**
     * Build the uid for the given event id. The id is encoded as UTF-8 so a uid does not depend on the platform default charset.
     *
     * @param id
     *            the event id, which must not be null or empty
     * @return the uid
     */
    public static String uid(String id) {
        Preconditions.checkArgument(id != null && !id.isEmpty(), "id must not be null or empty");
        return UID.builder().newId(id.getBytes(StandardCharsets.UTF_8), (Date) null).toString();
    }
}
