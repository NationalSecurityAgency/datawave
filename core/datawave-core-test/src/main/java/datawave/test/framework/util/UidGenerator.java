package datawave.test.framework.util;

import java.util.Date;

import datawave.table.hash.UID;

public class UidGenerator {

    private UidGenerator() {
        // enforce static access
    }

    public static String uid(String id) {
        return UID.builder().newId(id.getBytes(), (Date) null).toString();
    }
}
