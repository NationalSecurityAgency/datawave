package datawave.query.util;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Locale;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.protobuf.Uid;
import datawave.table.constants.MetadataColumnFamilyConstants;
import datawave.table.constants.TableName;
import datawave.util.time.DateHelper;

/**
 * Deterministic event, field-index, and global-index data for proxied authorization queries. The visibilities match the Docker Compose proxied-auth.csv
 * fixture.
 */
public class ProxiedEntitiesIngest {
    public static final String DAY = "20160426";
    private static final String SHARD = DAY + "_0";
    private static final String DATATYPE = "proxied";
    private static final String QUERY_VALUE = "proxy";
    private static final long TIMESTAMP = DateHelper.parse(DAY).getTime();
    private static final Value EMPTY_VALUE = new Value();

    public enum Event {
        COMMON("A&B"),
        USER_ONE_SERVER_ONE("A&B&C"),
        USER_TWO_SERVER_ONE("A&B&E"),
        USER_ONE_ONLY("D"),
        USER_TWO_ONLY("F"),
        SERVER_ONE_ONLY("G"),
        SERVER_TWO_ONLY("H"),
        USER_ONE_PARTIAL("A&D"),
        SERVER_ONE_PARTIAL("A&G"),
        SPLIT_USERS_PARTIAL("C&E");

        private final ColumnVisibility visibility;

        Event(String visibility) {
            this.visibility = new ColumnVisibility(visibility);
        }

        public String uuid() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static void write(AccumuloClient client) throws Exception {
        TableOperations tables = client.tableOperations();
        tables.create(TableName.SHARD);
        tables.create(TableName.SHARD_INDEX);
        tables.create(TableName.METADATA);

        client.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C", "D", "E", "F", "G", "H"));
        writeMetadata(client);

        try (BatchWriter shardWriter = client.createBatchWriter(TableName.SHARD); BatchWriter indexWriter = client.createBatchWriter(TableName.SHARD_INDEX)) {
            for (Event event : Event.values()) {
                String uid = UID.builder().newId(event.uuid().getBytes(StandardCharsets.UTF_8), (Date) null).toString();
                writeShardEvent(shardWriter, event, uid);
                writeGlobalIndex(indexWriter, event, uid);
            }
        }
    }

    private static void writeMetadata(AccumuloClient client) throws Exception {
        try (BatchWriter writer = client.createBatchWriter(TableName.METADATA)) {
            Mutation shards = new Mutation("num_shards");
            shards.put("ns", DAY + "_1", EMPTY_VALUE);
            writer.addMutation(shards);

            Mutation caseField = new Mutation("CASE");
            caseField.put(MetadataColumnFamilyConstants.COLF_E, new Text(DATATYPE), EMPTY_VALUE);
            caseField.put(MetadataColumnFamilyConstants.COLF_I, new Text(DATATYPE), EMPTY_VALUE);
            caseField.put(MetadataColumnFamilyConstants.COLF_T, new Text(DATATYPE + "\0" + LcNoDiacriticsType.class.getName()), EMPTY_VALUE);
            writer.addMutation(caseField);

            Mutation uuidField = new Mutation("UUID");
            uuidField.put(MetadataColumnFamilyConstants.COLF_E, new Text(DATATYPE), EMPTY_VALUE);
            uuidField.put(MetadataColumnFamilyConstants.COLF_T, new Text(DATATYPE + "\0" + LcNoDiacriticsType.class.getName()), EMPTY_VALUE);
            writer.addMutation(uuidField);
        }
    }

    private static void writeShardEvent(BatchWriter writer, Event event, String uid) throws Exception {
        Mutation shard = new Mutation(SHARD);
        String eventFamily = DATATYPE + "\0" + uid;
        shard.put(eventFamily, "CASE\0" + QUERY_VALUE, event.visibility, TIMESTAMP, EMPTY_VALUE);
        shard.put(eventFamily, "UUID\0" + event.uuid(), event.visibility, TIMESTAMP, EMPTY_VALUE);
        shard.put("fi\0CASE", QUERY_VALUE + "\0" + DATATYPE + "\0" + uid, event.visibility, TIMESTAMP, EMPTY_VALUE);
        writer.addMutation(shard);
    }

    private static void writeGlobalIndex(BatchWriter writer, Event event, String uid) throws Exception {
        Uid.List.Builder ids = Uid.List.newBuilder();
        ids.setIGNORE(false);
        ids.setCOUNT(1L);
        ids.addUID(uid);

        Mutation index = new Mutation(QUERY_VALUE);
        index.put("CASE", SHARD + "\0" + DATATYPE, event.visibility, TIMESTAMP, new Value(ids.build().toByteArray()));
        writer.addMutation(index);
    }
}
