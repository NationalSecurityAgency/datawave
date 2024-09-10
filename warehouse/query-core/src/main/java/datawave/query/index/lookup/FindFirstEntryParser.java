package datawave.query.index.lookup;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl3.parser.ASTOrNode;

import datawave.core.query.configuration.Result;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;

/**
 * Special case EntryParse for the 'find-first' use case.
 */
public class FindFirstEntryParser extends EntryParser {

    private final ASTOrNode node;

    public FindFirstEntryParser(ASTOrNode node) {
        this.node = node;
    }

    @Override
    public Tuple2<String,IndexInfo> apply(Result entry) {
        IndexInfo info = parseInfo(entry.getValue());

        String date = entry.getKey().getColumnQualifier().toString();

        return Tuples.tuple(date, info);
    }

    private IndexInfo parseInfo(Value value) {
        try {
            IndexInfo info = new IndexInfo();
            info.readFields(new DataInputStream(new ByteArrayInputStream(value.get())));

            // scanner hit on first field in the index, apply whole union to index info
            info.applyNode(node);

            return info;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse value");
        }
    }
}
