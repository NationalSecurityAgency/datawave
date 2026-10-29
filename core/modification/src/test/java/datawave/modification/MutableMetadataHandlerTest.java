package datawave.modification;

import static datawave.modification.MutableMetadataHandler.NULL_BYTE;
import static datawave.modification.MutableMetadataHandler.NULL_VALUE;
import static datawave.modification.MutableMetadataHandlerTestSupport.CAPONE_EVENT_TIMESTAMP;
import static datawave.modification.MutableMetadataHandlerTestSupport.CAPONE_UID;
import static datawave.modification.MutableMetadataHandlerTestSupport.DATATYPE;
import static datawave.modification.MutableMetadataHandlerTestSupport.LOCATION_FIELD;
import static datawave.modification.MutableMetadataHandlerTestSupport.NAME_FIELD;
import static datawave.modification.MutableMetadataHandlerTestSupport.QUOTE_FIELD;
import static datawave.modification.MutableMetadataHandlerTestSupport.QUOTE_TOKEN_FIELD;
import static datawave.modification.MutableMetadataHandlerTestSupport.SENTENCE_FIELD;
import static datawave.modification.MutableMetadataHandlerTestSupport.SHARD_ID;
import static datawave.modification.MutableMetadataHandlerTestSupport.USER;
import static datawave.modification.MutableMetadataHandlerTestSupport.UUID_FIELD;
import static datawave.modification.MutableMetadataHandlerTestSupport.VIS_ALL;
import static datawave.modification.MutableMetadataHandlerTestSupport.VIS_A_AND_B;
import static datawave.modification.MutableMetadataHandlerTestSupport.contentContextField;
import static datawave.modification.MutableMetadataHandlerTestSupport.globalIndexQualifier;
import static datawave.modification.MutableMetadataHandlerTestSupport.normalizeUuid;

import java.util.Set;

import org.junit.jupiter.api.Test;

import datawave.table.constants.TableName;
import datawave.webservice.modification.DefaultModificationRequest;
import datawave.webservice.modification.ModificationRequestBase.MODE;

/**
 * Accumulo-backed integration tests for {@link MutableMetadataHandler}.
 * <p>
 * The suite uses the query-core in-memory shard fixture and validates the handler by scanning the underlying tables directly after each modification request.
 * The tests are organized by behavior slice instead of by individual handler method:
 * </p>
 * <ul>
 * <li>basic indexed field lifecycle</li>
 * <li>visibility and markings handling</li>
 * <li>history behavior</li>
 * <li>content/token cleanup</li>
 * <li>index-only field behavior</li>
 * <li>grouped-field behavior</li>
 * </ul>
 * <p>
 * The assertions intentionally stay close to shard-schema details so that regressions in event-table, field-index, global-index, reverse-index, metadata, or
 * history side effects are easy to diagnose from a single failing test.
 * </p>
 */
public class MutableMetadataHandlerTest {

    @Test
    public void insertShouldAddEventFieldFieldIndexGlobalIndexReverseIndexMetadataAndHistory() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_insert");
        String newUuid = "CAPONE-UPDATED";
        String normalized = normalizeUuid(newUuid);

        DefaultModificationRequest request = support.newRequest(MODE.INSERT, UUID_FIELD, newUuid, null);
        support.process(request);

        support.assertEventFieldPresent(UUID_FIELD, newUuid, VIS_ALL);
        support.assertFieldIndexPresent(UUID_FIELD, normalized, VIS_ALL);
        support.assertShardIndexMutation(UUID_FIELD, normalized, VIS_ALL, 1);
        support.assertReverseIndexMutation(UUID_FIELD, normalized, VIS_ALL, 1);
        support.assertLatestMetadataFrequencyValue(UUID_FIELD, 1L);
        support.assertHistoryInsert(UUID_FIELD, newUuid);
    }

    @Test
    public void deleteShouldRemoveExistingFieldAndDecrementIndexesAndMetadata() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_delete");
        String value = "DELETE-ME";
        String normalized = normalizeUuid(value);

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, value, null));
        support.process(support.newRequest(MODE.DELETE, UUID_FIELD, value, null));

        support.assertEventFieldAbsent(UUID_FIELD, value);
        support.assertFieldIndexAbsent(UUID_FIELD, normalized);
        support.assertShardIndexMutation(UUID_FIELD, normalized, VIS_ALL, -1);
        support.assertReverseIndexMutation(UUID_FIELD, normalized, VIS_ALL, -1);
        support.assertLatestMetadataFrequencyValue(UUID_FIELD, -1L);
        support.assertHistoryDelete(UUID_FIELD, value);
    }

    @Test
    public void updateShouldRemoveOldValueInsertNewValueAndWriteHistory() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_update");
        String oldValue = "OLD-UUID";
        String newValue = "NEW-UUID";
        String oldNormalized = normalizeUuid(oldValue);
        String newNormalized = normalizeUuid(newValue);

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, oldValue, null));
        support.process(support.newRequest(MODE.UPDATE, UUID_FIELD, newValue, oldValue));

        support.assertEventFieldAbsent(UUID_FIELD, oldValue);
        support.assertEventFieldPresent(UUID_FIELD, newValue, VIS_ALL);
        support.assertFieldIndexAbsent(UUID_FIELD, oldNormalized);
        support.assertFieldIndexPresent(UUID_FIELD, newNormalized, VIS_ALL);
        support.assertShardIndexMutation(UUID_FIELD, oldNormalized, VIS_ALL, -1);
        support.assertShardIndexMutation(UUID_FIELD, newNormalized, VIS_ALL, 1);
        support.assertReverseIndexMutation(UUID_FIELD, oldNormalized, VIS_ALL, -1);
        support.assertReverseIndexMutation(UUID_FIELD, newNormalized, VIS_ALL, 1);
        support.assertLatestMetadataFrequencyValue(UUID_FIELD, 1L);
        support.assertHistoryUpdate(UUID_FIELD, oldValue);
        support.assertHistoryUpdate(UUID_FIELD, newValue);
    }

    @Test
    public void deleteWithWrongOldValueShouldRejectWithoutChangingTables() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_deleteReject");
        String actualValue = "KEEP-ME";
        String wrongValue = "NOT-THERE";
        String actualNormalized = normalizeUuid(actualValue);

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, actualValue, null));

        org.apache.accumulo.core.data.Value beforeMetadata = support.captureMetadataFrequencyValue(UUID_FIELD);

        support.assertProcessThrows(IllegalArgumentException.class, support.newRequest(MODE.DELETE, UUID_FIELD, wrongValue, null));

        support.assertEventFieldPresent(UUID_FIELD, actualValue, VIS_ALL);
        support.assertFieldIndexPresent(UUID_FIELD, actualNormalized, VIS_ALL);
        support.assertShardIndexMutation(UUID_FIELD, actualNormalized, VIS_ALL, 1);
        support.assertMetadataUnchanged(UUID_FIELD, beforeMetadata);
    }

    @Test
    public void insertWithColumnVisibilityShouldUseProvidedVisibility() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_insertExplicitVisibility");
        String newUuid = "VISIBLE-UUID";
        String normalized = normalizeUuid(newUuid);

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, newUuid, null, VIS_A_AND_B));

        support.assertEventFieldPresent(UUID_FIELD, newUuid, VIS_A_AND_B);
        support.assertFieldIndexPresent(UUID_FIELD, normalized, VIS_A_AND_B);
        support.assertShardIndexMutation(UUID_FIELD, normalized, VIS_A_AND_B, 1);
        support.assertHistoryInsert(UUID_FIELD, newUuid);
    }

    @Test
    public void insertWithMarkingsAndNoVisibilityShouldTranslateMarkingsToVisibility() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_insertMarkings");
        String newUuid = "MARKINGS-UUID";
        String normalized = normalizeUuid(newUuid);

        support.process(support.newMarkingsInsertRequest(UUID_FIELD, newUuid, VIS_A_AND_B));

        support.assertEventFieldPresent(UUID_FIELD, newUuid, VIS_A_AND_B);
        support.assertFieldIndexPresent(UUID_FIELD, normalized, VIS_A_AND_B);
        support.assertShardIndexMutation(UUID_FIELD, normalized, VIS_A_AND_B, 1);
    }

    @Test
    public void deleteWithOldColumnVisibilityShouldMatchEquivalentFlattenedVisibility() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_deleteEquivalentVisibility");
        String actualValue = "VISIBLE-DELETE";
        String normalized = normalizeUuid(actualValue);

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, actualValue, null, VIS_A_AND_B));
        support.process(support.newRequest(MODE.DELETE, UUID_FIELD, actualValue, null, "B&A"));

        support.assertEventFieldAbsent(UUID_FIELD, actualValue);
        support.assertFieldIndexAbsent(UUID_FIELD, normalized);
        support.assertShardIndexMutation(UUID_FIELD, normalized, VIS_A_AND_B, -1);
    }

    @Test
    public void deleteWithWrongVisibilityShouldNotDeleteAnything() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_deleteWrongVisibility");
        String actualValue = "KEEP-VISIBILITY";
        String normalized = normalizeUuid(actualValue);

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, actualValue, null, VIS_A_AND_B));

        org.apache.accumulo.core.data.Value beforeMetadata = support.captureMetadataFrequencyValue(UUID_FIELD);

        support.assertProcessThrows(IllegalArgumentException.class, support.newRequest(MODE.DELETE, UUID_FIELD, actualValue, null, "A&C"));

        support.assertEventFieldPresent(UUID_FIELD, actualValue, VIS_A_AND_B);
        support.assertFieldIndexPresent(UUID_FIELD, normalized, VIS_A_AND_B);
        support.assertShardIndexMutation(UUID_FIELD, normalized, VIS_A_AND_B, 1);
        support.assertMetadataUnchanged(UUID_FIELD, beforeMetadata);
    }

    @Test
    public void insertShouldAddHistoryFieldWhenInsertHistoryEnabled() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_historyInsertEnabled");
        String value = "HISTORY-INSERT";

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, value, null));

        support.assertHistoryEntryCount(UUID_FIELD, 1);
        support.assertHistoryInsertAtTimestamp(UUID_FIELD, value, CAPONE_EVENT_TIMESTAMP);
    }

    @Test
    public void deleteShouldAddHistoryFieldWhenInsertHistoryEnabled() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_historyDeleteEnabled");
        String value = "HISTORY-DELETE";

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, value, null), false, false);
        support.process(support.newRequest(MODE.DELETE, UUID_FIELD, value, null));

        support.assertHistoryEntryCount(UUID_FIELD, 1);
        support.assertHistoryDeleteAtTimestamp(UUID_FIELD, value, CAPONE_EVENT_TIMESTAMP);
    }

    @Test
    public void updateShouldAddHistoryEntriesForUpdatePath() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_historyUpdateEnabled");
        String oldValue = "HISTORY-OLD";
        String newValue = "HISTORY-NEW";

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, oldValue, null), false, false);
        support.process(support.newRequest(MODE.UPDATE, UUID_FIELD, newValue, oldValue));

        support.assertHistoryEntryCount(UUID_FIELD, 2);
        support.assertHistoryUpdateAtTimestamp(UUID_FIELD, oldValue, CAPONE_EVENT_TIMESTAMP);
        support.assertHistoryUpdateAtTimestamp(UUID_FIELD, newValue, CAPONE_EVENT_TIMESTAMP);
    }

    @Test
    public void insertShouldSkipHistoryWhenDisabled() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_historyInsertDisabled");

        support.process(support.newRequest(MODE.INSERT, UUID_FIELD, "NO-HISTORY", null), false, false);

        support.assertHistoryEntryCount(UUID_FIELD, 0);
    }

    @Test
    public void deleteContentFieldShouldRemoveAssociatedDColumnEntries() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_contentDelete");
        String quote = "Content cleanup target";

        support.setContentFields(Set.of(QUOTE_FIELD));
        support.process(support.newRequest(MODE.INSERT, QUOTE_FIELD, quote, null), false, false);
        support.put(TableName.SHARD, SHARD_ID, "d", DATATYPE + NULL_BYTE + CAPONE_UID + NULL_BYTE + QUOTE_FIELD + NULL_BYTE + "0", "ALL",
                        CAPONE_EVENT_TIMESTAMP, NULL_VALUE);

        support.process(support.newRequest(MODE.DELETE, QUOTE_FIELD, quote, null), false, false);

        support.assertContentRowAbsent(QUOTE_FIELD, "0");
    }

    @Test
    public void deleteWithPurgeTokensShouldRemoveTokenFieldIndexEntriesForMappedFields() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_purgeTokens");
        String quote = "Kind word";
        String tokenValue = "word";

        support.setIndexOnlySuffixes(Set.of("_TOKEN"));
        support.process(support.newRequest(MODE.INSERT, QUOTE_FIELD, quote, null), false, false);
        support.setupTokenField(QUOTE_TOKEN_FIELD, tokenValue);

        support.process(support.newRequest(MODE.DELETE, QUOTE_FIELD, quote, null), true, false);

        support.assertFieldIndexAbsent(QUOTE_TOKEN_FIELD, tokenValue);
        support.assertTermFrequencyRowAbsent(QUOTE_TOKEN_FIELD, tokenValue);
        support.assertShardIndexMutation(QUOTE_TOKEN_FIELD, tokenValue, VIS_ALL, -1);
    }

    @Test
    public void deleteWithPurgeTokensShouldRemoveContentContextTermFrequencyRows() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_purgeTokensContentContext");
        String quote = "Kind word";
        String tokenValue = "word";
        String hashedTokenField = contentContextField(QUOTE_TOKEN_FIELD, quote);

        support.setIndexOnlySuffixes(Set.of("_TOKEN"));
        support.process(support.newRequest(MODE.INSERT, QUOTE_FIELD, quote, null), false, false);
        support.setupTokenField(QUOTE_TOKEN_FIELD, tokenValue, hashedTokenField);
        support.assertShardIndexMutation(QUOTE_TOKEN_FIELD, tokenValue, VIS_ALL, 1);
        support.assertFieldIndexPresent(QUOTE_TOKEN_FIELD, tokenValue, VIS_ALL);

        support.assertTermFrequencyRowPresent(hashedTokenField, tokenValue, VIS_ALL);

        support.process(support.newRequest(MODE.DELETE, QUOTE_FIELD, quote, null), true, false);

        support.assertFieldIndexAbsent(QUOTE_TOKEN_FIELD, tokenValue);
        support.assertShardIndexMutation(QUOTE_TOKEN_FIELD, tokenValue, VIS_ALL, -1);
        support.assertTermFrequencyRowAbsent(hashedTokenField, tokenValue);
    }

    @Test
    public void deleteWithoutPurgeTokensShouldLeaveDerivedTokenEntriesInPlace() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_keepTokens");
        String quote = "Kind word";
        String tokenValue = "word";

        support.setIndexOnlySuffixes(Set.of("_TOKEN"));
        support.process(support.newRequest(MODE.INSERT, QUOTE_FIELD, quote, null), false, false);
        support.setupTokenField(QUOTE_TOKEN_FIELD, tokenValue);

        support.process(support.newRequest(MODE.DELETE, QUOTE_FIELD, quote, null), false, false);

        support.assertFieldIndexPresent(QUOTE_TOKEN_FIELD, tokenValue, VIS_ALL);
        support.assertTermFrequencyRowPresent(QUOTE_TOKEN_FIELD, tokenValue, VIS_ALL);
    }

    @Test
    public void insertIndexOnlyFieldShouldWriteIndexesWithoutEventValue() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_indexOnlyInsert");
        String value = "CHICAGO";
        String normalized = normalizeUuid(value);

        support.addMutableField(LOCATION_FIELD);
        support.process(support.newRequest(MODE.INSERT, LOCATION_FIELD, value, null), false, true);

        support.assertEventFieldAbsent(LOCATION_FIELD, value);
        support.assertFieldIndexPresent(LOCATION_FIELD, normalized, VIS_ALL);
        support.assertShardIndexMutation(LOCATION_FIELD, normalized, VIS_ALL, 1);
        support.assertReverseIndexMutation(LOCATION_FIELD, normalized, VIS_ALL, 1);
        support.assertHistoryEntryCount(LOCATION_FIELD, 0);
    }

    @Test
    public void indexOnlyFieldWithoutDatatypesShouldFailFast() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_indexOnlyNoDatatypes");

        support.addMutableField(SENTENCE_FIELD);

        support.assertProcessThrows(IllegalStateException.class, support.newRequest(MODE.INSERT, SENTENCE_FIELD, "broken", null), false, false);

        support.assertEventFieldAbsent(SENTENCE_FIELD, "broken");
        support.assertFieldIndexAbsent(SENTENCE_FIELD, "broken");
    }

    @Test
    public void deleteIndexOnlyFieldShouldRejectWithoutChangingIndexes() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_indexOnlyDeleteReject");
        String value = "CHICAGO";
        String normalized = normalizeUuid(value);

        support.addMutableField(LOCATION_FIELD);
        support.process(support.newRequest(MODE.INSERT, LOCATION_FIELD, value, null), false, true);

        support.assertProcessThrows(IllegalArgumentException.class, support.newRequest(MODE.DELETE, LOCATION_FIELD, value, null), false, true);

        support.assertEventFieldAbsent(LOCATION_FIELD, value);
        support.assertFieldIndexPresent(LOCATION_FIELD, normalized, VIS_ALL);
        support.assertShardIndexMutation(LOCATION_FIELD, normalized, VIS_ALL, 1);
        support.assertReverseIndexMutation(LOCATION_FIELD, normalized, VIS_ALL, 1);
        support.assertHistoryEntryCount(LOCATION_FIELD, 0);
    }

    @Test
    public void deleteGroupedFieldShouldRemoveMatchingGroupedEntryAndLeaveExistingIndexRowsUntouched() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_groupedDelete");
        String groupedField = "NAME.0";
        String value = "ALPHONSE";
        String normalized = normalizeUuid(value);

        support.addMutableField(groupedField);
        support.process(support.newRequest(MODE.DELETE, groupedField, value, null));

        support.assertEventFieldAbsent(groupedField, value);
        support.assertEventFieldPresent("NAME.1", "FRANK", VIS_ALL);
        support.assertFieldIndexPresent(NAME_FIELD, normalized, VIS_ALL);
        support.assertShardIndexMutation(NAME_FIELD, normalized, VIS_ALL, 1);
        support.assertHistoryEntryContains("NAME.0", ":" + USER + ":" + value + ":delete");
    }

    @Test
    public void updateGroupedFieldShouldOnlyTouchSpecifiedGroupedValueAndNotCreateNewIndexRows() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_groupedUpdate");
        String groupedField = "NAME.0";
        String oldValue = "ALPHONSE";
        String newValue = "SCARFACE";
        String oldNormalized = normalizeUuid(oldValue);
        String newNormalized = normalizeUuid(newValue);

        support.addMutableField(groupedField);
        support.process(support.newRequest(MODE.UPDATE, groupedField, newValue, oldValue));

        support.assertEventFieldAbsent(groupedField, oldValue);
        support.assertEventFieldPresent(groupedField, newValue, VIS_ALL);
        support.assertEventFieldPresent("NAME.1", "FRANK", VIS_ALL);
        support.assertFieldIndexPresent(NAME_FIELD, oldNormalized, VIS_ALL);
        support.assertFieldIndexAbsent(NAME_FIELD, newNormalized);
        support.assertShardIndexMutation(NAME_FIELD, oldNormalized, VIS_ALL, 1);
        support.assertEntryAbsent(TableName.SHARD_INDEX, newNormalized, NAME_FIELD, globalIndexQualifier());
        support.assertHistoryEntryContains("NAME.0", ":" + USER + ":" + oldValue + ":update");
        support.assertHistoryEntryContains("NAME.0", ":" + USER + ":" + newValue + ":update");
    }

    @Test
    public void deleteUngroupedFieldNameAgainstGroupedDataShouldRejectWithoutChangingTables() throws Exception {
        MutableMetadataHandlerTestSupport support = new MutableMetadataHandlerTestSupport(getClass().getSimpleName() + "_groupedDeleteReject");
        String actualValue = "ALPHONSE";
        String normalized = normalizeUuid(actualValue);

        support.assertProcessThrows(IllegalArgumentException.class, support.newRequest(MODE.DELETE, NAME_FIELD, actualValue, null));

        support.assertEventFieldPresent("NAME.0", actualValue, VIS_ALL);
        support.assertFieldIndexPresent(NAME_FIELD, normalized, VIS_ALL);
        support.assertShardIndexMutation(NAME_FIELD, normalized, VIS_ALL, 1);
    }
}
