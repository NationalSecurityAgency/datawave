package datawave.core.common.audit;

import java.util.List;
import java.util.Map;

/**
 * Constants marking private parameters that are computed internally and then added at runtime to the incoming query parameters for the purposes of passing them
 * along to the audit service.
 */
public class PrivateAuditConstants {
    public static final String PREFIX = "audit.";

    public static final String AUDIT_TYPE = PREFIX + "auditType";
    public static final String COLUMN_VISIBILITY = PREFIX + "columnVisibility";
    public static final String LOGIC_CLASS = PREFIX + "logicClass";
    public static final String USER_DN = PREFIX + "userDn";
    public static final String SELECTORS = PREFIX + "selectors";

    public static void stripPrivateParameters(Map<String,List<String>> queryParameters) {
        queryParameters.entrySet().removeIf(entry -> entry.getKey().startsWith(PREFIX));
    }
}
