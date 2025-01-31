package datawave.modification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.util.MetadataHelper;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.modification.DefaultModificationRequest;
import datawave.webservice.modification.DefaultUUIDModificationRequest;
import datawave.webservice.modification.EventIdentifier;
import datawave.webservice.modification.ModificationEvent;
import datawave.webservice.modification.ModificationOperation;
import datawave.webservice.modification.ModificationOperation.OPERATIONMODE;
import datawave.webservice.modification.ModificationOperationImpl;
import datawave.webservice.modification.ModificationRequestBase;
import datawave.webservice.modification.ModificationRequestBase.MODE;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;

/**
 * Class that handles requests for modification requests (INSERT, UPDATE, DELETE, REPLACE) for metadata. From a DefaultUUIDModificationRequest it performs <br>
 * a UUIDLookupQuery in order to get the event to modify. The lookup based on the UUID <b>must return a single event</b>. In order to UPDATE or DELETE,<br>
 * there must either be exactly one value for the specified field or oldFieldValue must be specified indicating which current value to modify.<br>
 * INSERT adds the specified value to the specified field <br>
 * DELETE deletes the specified value from the specified field <br>
 * UPDATE changes the specified value (current) to a new value of the specified field <br>
 * REPLACE deletes all current values of the specified field and then inserts the specified value <br>
 * <br>
 * <br>
 * INSERT Example:
 *
 * <pre>
 * &lt;DefaultUUIDModificationRequest xmlns=&quot;http://webservice.datawave.nsa/v1&quot;&gt;
 *   &lt;Events&gt;
 *     &lt;Event&gt;
 *       &lt;id&gt;A12BCD34-56E7-8FG9-0HIJ123KLM4N&lt;/id&gt;
 *       &lt;idType&gt;UUID&lt;/idType&gt;
 *         &lt;operations&gt;
 *           &lt;operation&gt;
 *             &lt;operationMode&gt;INSERT&lt;/operationMode&gt;
 *             &lt;fieldName&gt;TEST&lt;/fieldName&gt;
 *             &lt;fieldValue&gt;ABC&lt;/fieldValue&gt;
 *             &lt;columnVisibility&gt;viz1&amp;amp;viz2&lt;/columnVisibility&gt;
 *           &lt;/operation&gt;
 *         &lt;/operations&gt;
 *         &lt;user&gt;joeuser&lt;/user&gt;
 *     &lt;/Event&gt;
 *   &lt;/Events&gt;
 * &lt;/DefaultUUIDModificationRequest&gt;
 * </pre>
 *
 * <br>
 * DELETE Example:
 *
 * <pre>
 * &lt;DefaultUUIDModificationRequest xmlns=&quot;http://webservice.datawave.nsa/v1&quot;&gt;
 *   &lt;Events&gt;
 *     &lt;Event&gt;
 *       &lt;id&gt;A12BCD34-56E7-8FG9-0HIJ123KLM4N&lt;/id&gt;
 *       &lt;idType&gt;UUID&lt;/idType&gt;
 *         &lt;operations&gt;
 *           &lt;operation&gt;
 *             &lt;operationMode&gt;DELETE&lt;/operationMode&gt;
 *             &lt;fieldName&gt;TEST&lt;/fieldName&gt;
 *             &lt;oldFieldValue&gt;ABC&lt;/oldFieldValue&gt;
 *             &lt;columnVisibility&gt;viz1&amp;amp;viz2&lt;/columnVisibility&gt;
 *           &lt;/operation&gt;
 *         &lt;/operations&gt;
 *         &lt;user&gt;joeuser&lt;/user&gt;
 *     &lt;/Event&gt;
 *   &lt;/Events&gt;
 * &lt;/DefaultUUIDModificationRequest&gt;
 * </pre>
 *
 * <br>
 * UPDATE Example:
 *
 * <pre>
 * &lt;DefaultUUIDModificationRequest xmlns=&quot;http://webservice.datawave.nsa/v1&quot;&gt;
 *   &lt;Events&gt;
 *     &lt;Event&gt;
 *       &lt;id&gt;A12BCD34-56E7-8FG9-0HIJ123KLM4N&lt;/id&gt;
 *       &lt;idType&gt;UUID&lt;/idType&gt;
 *         &lt;operations&gt;
 *           &lt;operation&gt;
 *             &lt;operationMode&gt;UPDATE&lt;/operationMode&gt;
 *             &lt;fieldName&gt;TEST&lt;/fieldName&gt;
 *             &lt;fieldValue&gt;DEF&lt;/fieldValue&gt;
 *             &lt;oldFieldValue&gt;ABC&lt;/oldFieldValue&gt;
 *             &lt;columnVisibility&gt;viz1&amp;amp;viz2&lt;/columnVisibility&gt;
 *           &lt;/operation&gt;
 *         &lt;/operations&gt;
 *         &lt;user&gt;joeuser&lt;/user&gt;
 *     &lt;/Event&gt;
 *   &lt;/Events&gt;
 * &lt;/DefaultUUIDModificationRequest&gt;
 * </pre>
 *
 * <br>
 * REPLACE Example:
 *
 * <pre>
 * &lt;DefaultUUIDModificationRequest xmlns=&quot;http://webservice.datawave.nsa/v1&quot;&gt;
 *   &lt;Events&gt;
 *     &lt;Event&gt;
 *       &lt;id&gt;A12BCD34-56E7-8FG9-0HIJ123KLM4N&lt;/id&gt;
 *       &lt;idType&gt;UUID&lt;/idType&gt;
 *         &lt;operations&gt;
 *           &lt;operation&gt;
 *             &lt;operationMode&gt;REPLACE&lt;/operationMode&gt;
 *             &lt;fieldName&gt;TEST&lt;/fieldName&gt;
 *             &lt;fieldValue&gt;DEF&lt;/fieldValue&gt;
 *             &lt;columnVisibility&gt;viz1&amp;amp;viz2&lt;/columnVisibility&gt;
 *           &lt;/operation&gt;
 *         &lt;/operations&gt;
 *         &lt;user&gt;joeuser&lt;/user&gt;
 *     &lt;/Event&gt;
 *   &lt;/Events&gt;
 * &lt;/DefaultUUIDModificationRequest&gt;
 * </pre>
 *
 * <br>
 * Example of bulk request:
 *
 * <pre>
 * &lt;DefaultUUIDModificationRequest xmlns=&quot;http://webservice.datawave.nsa/v1&quot;&gt;
 *   &lt;Events&gt;
 *     &lt;Event&gt;
 *       &lt;id&gt;A12BCD34-56E7-8FG9-0HIJ123KLM4N&lt;/id&gt;
 *       &lt;idType&gt;UUID&lt;/idType&gt;
 *         &lt;operations&gt;
 *           &lt;operation&gt;
 *             &lt;operationMode&gt;UPDATE&lt;/operationMode&gt;
 *             &lt;fieldName&gt;TEST&lt;/fieldName&gt;
 *             &lt;fieldValue&gt;DEF&lt;/fieldValue&gt;
 *             &lt;oldFieldValue&gt;ABC&lt;/oldFieldValue&gt;
 *             &lt;columnVisibility&gt;viz1&amp;amp;viz2&lt;/columnVisibility&gt;
 *           &lt;/operation&gt;
 *           &lt;operation&gt;
 *             &lt;operationMode&gt;INSERT&lt;/operationMode&gt;
 *             &lt;fieldName&gt;TEST&lt;/fieldName&gt;
 *             &lt;fieldValue&gt;XYZ&lt;/fieldValue&gt;
 *             &lt;columnVisibility&gt;viz1&amp;amp;viz2&lt;/columnVisibility&gt;
 *           &lt;/operation&gt;
 *         &lt;/operations&gt;
 *         &lt;user&gt;joeuser&lt;/user&gt;
 *     &lt;/Event&gt;
 *     &lt;Event&gt;
 *       &lt;id&gt;W12XYZ34-56E7-8FG9-0HIJ123KLM4N&lt;/id&gt;
 *       &lt;idType&gt;UUID&lt;/idType&gt;
 *         &lt;operations&gt;
 *           &lt;operation&gt;
 *             &lt;operationMode&gt;REPLACE&lt;/operationMode&gt;
 *             &lt;fieldName&gt;TEST&lt;/fieldName&gt;
 *             &lt;fieldValue&gt;DEF&lt;/fieldValue&gt;
 *             &lt;columnVisibility&gt;viz1&amp;amp;viz2&lt;/columnVisibility&gt;
 *           &lt;/operation&gt;
 *         &lt;/operations&gt;
 *         &lt;user&gt;bobuser&lt;/user&gt;
 *     &lt;/Event&gt;
 *   &lt;/Events&gt;
 * &lt;/DefaultUUIDModificationRequest&gt;
 * </pre>
 */

public class MutableMetadataUUIDHandler extends MutableMetadataHandler {

    private static final String DESCRIPTION = "Service that processes modification requests of event " + "fields for event(s) identified by an ID.";
    private Logger log = LoggerFactory.getLogger(this.getClass());

    private String fieldName = "";
    private String fieldValue = "";
    private String oldFieldValue = null;
    private Map<String,OPERATIONMODE> replaceMap = new HashMap<>();
    private String fieldColumnVisibility = "";
    private int fieldCount = 0;

    @Override
    public Class<? extends ModificationRequestBase> getRequestClass() {
        return DefaultUUIDModificationRequest.class;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    public void ResetValues() {
        fieldValue = "";
        fieldCount = 0;
        fieldColumnVisibility = "";
        oldFieldValue = null;
        if (replaceMap == null) {
            replaceMap = new HashMap<>();
        } else {
            replaceMap.clear();
        }
    }

    @Override
    public void process(AccumuloClient client, ModificationRequestBase request, Map<String,Set<String>> mutableFieldList, Set<Authorizations> userAuths,
                    ProxiedUserDetails userDetails)
                    throws DatawaveModificationException, AccumuloException, AccumuloSecurityException, TableNotFoundException, ExecutionException {
        String user = userDetails.getShortName();

        ArrayList<Exception> exceptions = new ArrayList<>();
        MetadataHelper mHelper = getMetadataHelper(client);

        // Receive DefaultUUIDModificationRequest
        DefaultUUIDModificationRequest uuidModReq = DefaultUUIDModificationRequest.class.cast(request);
        List<ModificationEvent> events = uuidModReq.getEvents();

        for (ModificationEvent event : events) {
            List<ModificationOperationImpl> operations = event.getOperations();
            for (ModificationOperationImpl operation : operations) {
                ResetValues();
                OPERATIONMODE mode = operation.getOperationMode();
                String columnVisibility = operation.getColumnVisibility();
                String oldColumnVisibility = operation.getOldColumnVisibility();
                String eventUser = event.getUser();

                // check whether this is a security-marking exempt field. Meaning we can pull the marking if not specified
                boolean securityMarkingExempt = false;
                fieldName = operation.getFieldName();
                for (String s : this.getSecurityMarkingExemptFields()) {
                    if (fieldName.toUpperCase().equals(s)) {
                        securityMarkingExempt = true;
                    }
                }

                // if they are updating, assume the old values should be the same as current
                if (OPERATIONMODE.UPDATE.equals(mode) && oldColumnVisibility == null) {
                    oldColumnVisibility = columnVisibility;
                }

                try {
                    if (mHelper.getIndexOnlyFields(mutableFieldList.keySet()).contains(event.getIdType().toUpperCase())) {
                        throw new IllegalStateException("Cannot perform modification because " + event.getIdType() + " is index only. Please search "
                                        + "with a different uuidType to identify the event you wish to modify.");
                    }

                    // perform the lookupUUID
                    EventBase<?,? extends FieldBase<?>> idEvent = findMatchingEventUuid(event.getId(), event.getIdType(), userAuths, operation, userDetails);
                    // extract contents from lookupUUID necessary for modification
                    List<? extends FieldBase<?>> fields = idEvent.getFields();
                    if (operation.getOldFieldValue() != null)
                        oldFieldValue = operation.getOldFieldValue();
                    if (fields != null) {
                        // there may be multiple values for a single field
                        for (FieldBase<?> f : fields) {
                            if (f.getName().equals(fieldName)) {
                                fieldCount++;

                                // if they are doing a replace, we need all the current values, store them
                                if (operation.getOperationMode().equals(OPERATIONMODE.REPLACE)) {
                                    if (log != null)
                                        log.trace("Adding {},delete to replaceMap", f.getValueString());
                                    replaceMap.put(f.getValueString(), OPERATIONMODE.DELETE);
                                }

                                // user sent an oldValue and we found that value or no oldValue
                                if ((oldFieldValue != null && f.getValueString().equals(oldFieldValue)) || oldFieldValue == null) {
                                    fieldValue = f.getValueString();

                                    if (columnVisibility == null && securityMarkingExempt) {
                                        fieldColumnVisibility = f.getColumnVisibility();
                                    }
                                }
                            }
                            // if the field doesn't exist, default columnVisibility to those of the UUID used to identify the event
                            // only if the input didn't supply a security marking AND it is an exempt field
                            else if (f.getName().equalsIgnoreCase(event.getIdType()) && fieldCount < 1 && columnVisibility == null && securityMarkingExempt) {
                                if (log != null)
                                    log.trace("Using visibility of {} and setting to {}", f.getName(), f.getColumnVisibility());
                                fieldColumnVisibility = f.getColumnVisibility();
                            }
                        }

                        List<DefaultModificationRequest> modificationRequests = new ArrayList<>();

                        if (OPERATIONMODE.INSERT.equals(mode) || OPERATIONMODE.UPDATE.equals(mode) || OPERATIONMODE.DELETE.equals(mode)) {
                            modificationRequests
                                            .add(createModificationRequest(idEvent, operation, columnVisibility, oldColumnVisibility, securityMarkingExempt));
                        } else if (OPERATIONMODE.REPLACE.equals(mode)) {
                            if (log != null)
                                log.trace("Adding {},insert to replaceMap", operation.getFieldValue());
                            replaceMap.put(operation.getFieldValue(), OPERATIONMODE.INSERT);

                            // create a modification request of delete for each current value and an insert for the new value
                            for (String s : replaceMap.keySet()) {
                                ModificationOperation replaceOperation = operation.clone();
                                replaceOperation.setOperationMode(replaceMap.get(s));
                                replaceOperation.setFieldValue(s);
                                oldFieldValue = s;
                                modificationRequests.add(createModificationRequest(idEvent, replaceOperation, columnVisibility, oldColumnVisibility,
                                                securityMarkingExempt));
                            }
                        }

                        if (log != null)
                            log.trace("modificationRequests= {}", modificationRequests);
                        for (DefaultModificationRequest modReq : modificationRequests) {
                            try {
                                if (fieldCount > 1 && (oldFieldValue == null && modReq.getMode() != MODE.INSERT) && !mode.equals(OPERATIONMODE.REPLACE)) {
                                    throw new IllegalStateException("Unable to perform modification. More than one value exists for " + modReq.getFieldName()
                                                    + ". Please specify the current value you wish to modify in the oldFieldValue field.");
                                } else if (fieldCount < 1 && modReq.getMode() != MODE.INSERT) {
                                    throw new IllegalStateException("Unable to perform modification. No values exist for " + modReq.getFieldName() + ".");
                                } else if (columnVisibility == null && !securityMarkingExempt) {
                                    throw new IllegalStateException("Must provide columnVisibility");
                                } else // submit DefaultModificationRequest
                                {
                                    log.info("eventUser = {}, event.getUser() = {}", eventUser, event.getUser());
                                    if (log != null)
                                        log.trace("Submitting request to MutableMetadataHandler from MutableMetadataUUIDHandler: {}", modReq);

                                    super.process(client, modReq, mutableFieldList, userAuths, userDetails);
                                }
                            }
                            // log exceptions that occur for each modification request. Let as many requests work as possible before returning
                            catch (Exception e) {
                                if (log != null)
                                    log.error("Modification error", e);
                                exceptions.add(new Exception(event.getId() + ": " + e.getMessage() + "\n" + modReq));
                            }
                        }
                        modificationRequests.clear();
                    } else {
                        throw new IllegalStateException("No event matched " + event.getId());
                    }
                } catch (Exception e) {
                    if (log != null)
                        log.error("Modification error", e);
                    exceptions.add(new Exception(event.getId() + ": " + e.getMessage() + "\n"));
                }

            }
        }

        // If any errors occurred, return them in the response to the user
        if (!exceptions.isEmpty()) {
            if (exceptions.size() == 1) {
                throw new DatawaveModificationException(new QueryException(DatawaveErrorCode.MODIFICATION_ERROR, exceptions.get(0)));
            } else {
                DatawaveModificationException exception = new DatawaveModificationException(new QueryException(DatawaveErrorCode.MODIFICATION_ERROR));
                for (Exception e : exceptions) {
                    QueryException qe = new QueryException(DatawaveErrorCode.MODIFICATION_ERROR, e);
                    exception.addException(qe);
                }
                throw exception;
            }
        }
    }

    // transforms a ModificationOperation and event information into a DefaultModificationRequest that can be used by the MutableMetadataHandler
    public DefaultModificationRequest createModificationRequest(EventBase<?,? extends FieldBase<?>> idEvent, ModificationOperation operation,
                    String columnVisibility, String oldColumnVisibility, boolean securityMarkingExempt) throws Exception {
        // make event identifier for modification request
        EventIdentifier ei = new EventIdentifier();
        ei.setDatatype(idEvent.getMetadata().getDataType());
        ei.setShardId(idEvent.getMetadata().getRow());
        ei.setEventUid(idEvent.getMetadata().getInternalId());
        List<EventIdentifier> thisEvent = Collections.singletonList(ei);
        if (log != null)
            log.trace("operation={}", operation);

        // set values for modification request
        DefaultModificationRequest modReq = new DefaultModificationRequest();
        modReq.setEvents(thisEvent);

        if (OPERATIONMODE.INSERT.equals(operation.getOperationMode())) {
            modReq.setMode(MODE.INSERT);
        } else if (OPERATIONMODE.DELETE.equals(operation.getOperationMode())) {
            modReq.setMode(MODE.DELETE);
        } else if (OPERATIONMODE.UPDATE.equals(operation.getOperationMode())) {
            modReq.setMode(MODE.UPDATE);
        } else {
            throw new Exception("Invalid operationMode: " + operation.getOperationMode());
        }

        modReq.setFieldName(fieldName);
        modReq.setFieldMarkings(null);
        modReq.setColumnVisibility(columnVisibility);
        modReq.setOldFieldMarkings(null);
        modReq.setOldColumnVisibility(oldColumnVisibility);
        if (columnVisibility == null && securityMarkingExempt) {
            // replace potential brackets with nothing so the visibility can be parsed correctly
            fieldColumnVisibility = fieldColumnVisibility.replaceAll("\\[", "");
            fieldColumnVisibility = fieldColumnVisibility.replaceAll("\\]", "");

            modReq.setColumnVisibility(fieldColumnVisibility);
        }

        if (modReq.getMode() == MODE.UPDATE) {
            modReq.setFieldValue(operation.getFieldValue());
            modReq.setOldFieldValue(fieldValue);
        } else if (modReq.getMode() == MODE.INSERT) {
            modReq.setFieldValue(operation.getFieldValue());
        } else if (modReq.getMode() == MODE.DELETE) {
            if (oldFieldValue == null)
                modReq.setFieldValue(fieldValue);
            else
                modReq.setFieldValue(oldFieldValue);
        }

        if (log != null)
            log.trace("Returning modReq={}", modReq);
        return modReq;
    }

}
