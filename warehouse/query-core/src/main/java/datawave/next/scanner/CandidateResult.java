package datawave.next.scanner;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.next.stats.DocIdQueryIterStats;
import datawave.next.stats.DocIterStats;

/**
 * Serializes candidate document keys, {@link DocIdQueryIterStats} and {@link DocIterStats}
 */
public class CandidateResult implements KryoSerializable {

    private Set<Key> candidates = null;
    private DocIdQueryIterStats queryStats = null;
    private DocIterStats iterStats = null;

    @Override
    public void write(Kryo kryo, Output output) {
        if (candidates == null) {
            output.writeInt(0, true);
        } else {
            output.writeInt(candidates.size(), true);

            if (!candidates.isEmpty()) {
                String row = candidates.iterator().next().getRow().toString();
                output.writeString(row);
            }

            for (Key result : candidates) {
                output.writeString(result.getColumnFamily().toString());
            }
        }

        if (queryStats == null) {
            output.writeBoolean(false);
        } else {
            output.writeBoolean(true);
            queryStats.write(kryo, output);
        }

        if (iterStats == null) {
            output.writeBoolean(false);
        } else {
            output.writeBoolean(true);
            iterStats.write(kryo, output);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        int candidateCount = input.readInt(true);
        candidates = new HashSet<>(candidateCount);
        if (candidateCount > 0) {
            String row = input.readString();
            for (int i = 0; i < candidateCount; i++) {
                String cf = input.readString();
                candidates.add(new Key(row, cf));
            }
        }

        boolean queryStatsExist = input.readBoolean();
        if (queryStatsExist) {
            queryStats = new DocIdQueryIterStats();
            queryStats.read(kryo, input);
        }

        boolean iterStatsExist = input.readBoolean();
        if (iterStatsExist) {
            iterStats = new DocIterStats();
            iterStats.read(kryo, input);
        }
    }

    public Set<Key> getCandidates() {
        return candidates;
    }

    public void setCandidates(Set<Key> candidates) {
        this.candidates = candidates;
    }

    public DocIdQueryIterStats getQueryStats() {
        return queryStats;
    }

    public void setQueryStats(DocIdQueryIterStats queryStats) {
        this.queryStats = queryStats;
    }

    public DocIterStats getIterStats() {
        return iterStats;
    }

    public void setIterStats(DocIterStats iterStats) {
        this.iterStats = iterStats;
    }
}
