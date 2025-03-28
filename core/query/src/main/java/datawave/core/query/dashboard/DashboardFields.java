package datawave.core.query.dashboard;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;

public final class DashboardFields {

    private static final Logger log = LoggerFactory.getLogger(DashboardFields.class);
    private static final String NUM_RESULTS = "NUM_RESULTS";
    private static final String ERROR_MESSAGE = "ERROR_MESSAGE";
    private static final String CREATE_CALL_TIME = "CREATE_CALL_TIME";
    private static final String PAGE_METRICS = "PAGE_METRICS";
    private static final String PAGE_METRICS_0 = PAGE_METRICS + ".0";
    private static final String POSITIVE_SELECTORS = "POSITIVE_SELECTORS";
    private static final String NEGATIVE_SELECTORS = "NEGATIVE_SELECTORS";
    private static final String SETUP_TIME = "SETUP_TIME";
    private static final String RETURN_FIELDS = toCSV(NUM_RESULTS, ERROR_MESSAGE, CREATE_CALL_TIME, PAGE_METRICS, POSITIVE_SELECTORS, NEGATIVE_SELECTORS,
                    SETUP_TIME);

    private DashboardFields() {}

    private static String toCSV(String... strings) {
        return StringUtils.join(strings, ',');
    }

    public static String getReturnFields() {
        return RETURN_FIELDS;
    }

    public static void addEvent(DashboardSummary summary, EventBase event) {
        long createCallTime = 0;
        long pageReturnTime = 0;
        long setupTime = 0;
        boolean error = false;
        int results = 0;
        int selectors = 0;

        for (FieldBase f : (List<FieldBase>) (event.getFields())) {
            switch (f.getName()) {
                case PAGE_METRICS_0:
                    // we only care about the first page
                    String strValue = f.getValueString();
                    List<String> strArray = Arrays.asList(strValue.split("/"));
                    if (strArray.size() > 1) {
                        try {
                            pageReturnTime = Long.parseLong(strArray.get(1));
                        } catch (NumberFormatException ex) {
                            log.warn("", ex);
                        }
                    }
                    break;
                case CREATE_CALL_TIME:
                    try {
                        createCallTime = Long.parseLong(f.getValueString());
                    } catch (NumberFormatException ex) {
                        log.warn("NumberFormatException:", ex);
                        return;
                    }
                    break;
                case SETUP_TIME:
                    try {
                        setupTime = Long.parseLong(f.getValueString());
                    } catch (NumberFormatException ex) {
                        log.warn("NumberFormatException:", ex);
                        return;
                    }
                    break;
                case ERROR_MESSAGE:
                    error = true;
                    break;
                case NUM_RESULTS:
                    try {
                        results = Integer.parseInt(f.getValueString());
                    } catch (NumberFormatException ex) {
                        log.warn("NumberFormatException:", ex);
                        return;
                    }
                    break;
                case NEGATIVE_SELECTORS:
                    selectors++;
                    break;
                case POSITIVE_SELECTORS:
                    selectors++;
                    break;
            }
        }

        summary.addQuery(createCallTime + pageReturnTime + setupTime, error, results, selectors);
    }
}
