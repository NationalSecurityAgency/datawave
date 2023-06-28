package datawave.edge.util;

import static datawave.edge.util.EdgeKey.COL_SEPARATOR_BYTE;
import static datawave.edge.util.EdgeKey.COL_SUB_SEPARATOR;
import static datawave.edge.util.EdgeKey.DATE_TYPE;
import static datawave.edge.util.EdgeKey.EDGE_FORMAT;
import static datawave.edge.util.EdgeKey.EDGE_VERSION;
import static datawave.edge.util.EdgeKey.EdgeColumnParts;
import static datawave.edge.util.EdgeKey.STATS_BYTES;
import static datawave.edge.util.EdgeKey.STATS_TYPE;

import java.nio.charset.CharacterCodingException;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

/**
 * Extracted from datawave.edge.util.EdgeKey
 *
 * Its implementation was slightly modified so that the decoder could be reused and thus reuse Text objects to avoid excessive allocation and garbage
 * collection.
 */
public class EdgeKeyDecoder {
    private static final Logger log = Logger.getLogger(EdgeKeyDecoder.class);
    private static final int DATE_LEN = 8;

    private final Text textCf;
    private final Text textCq;
    private final Text textRow;
    private final Text textCv;

    public EdgeKeyDecoder() {
        textCf = new Text();
        textCq = new Text();
        textRow = new Text();
        textCv = new Text();
    }

    public EdgeKey decode(Key key, EdgeKey.EdgeKeyBuilder builder) {
        key.getColumnFamily(textCf);

        builder.setFormat(determineEdgeFormat(textCf));

        // Parse row for type and source/sink information

        final String row = key.getRow(textRow).toString();

        // the previous code threw an exception when there was no row key, so
        // we should maintain that same
        if (row.isEmpty())
            throw new IllegalStateException("Invalid row identifier");

        int nullPos = row.indexOf('\0');

        if (nullPos < 0 && log.isTraceEnabled()) {
            log.trace("No null character found");
        }

        /**
         * determine if we are dealing with a stats edge
         */
        boolean statsEdge = builder.getFormat() == EDGE_FORMAT.STATS;

        /**
         * If we have a stats edge or an invalid row key, we will assign the row to the source and null to the sink.
         */
        String source = (statsEdge || nullPos <= 0) ? row : row.substring(0, nullPos);
        String sink = (statsEdge || nullPos <= 0) ? null : row.substring(nullPos + 1);

        if (log.isTraceEnabled()) {
            log.trace("Source is " + source + " sink is " + sink);
        }
        switch (builder.getFormat()) {
            case STANDARD:
                builder.setSourceData(source);
                builder.setSinkData(sink);
                break;
            case STATS:
                builder.setSourceData(source);
                break;
        }

        byte[] colQual = key.getColumnQualifier(textCq).getBytes();

        // Parse colf and colq as one big list, decoding with EDGE_VERSION logic
        EdgeColumnParts parts = new EdgeColumnParts(textCf, textCq);

        EDGE_VERSION version = EDGE_VERSION.getEdgeVersion(parts);

        builder.setType(parts.get(version.getTypeIndex()));
        builder.setYyyymmdd(parts.get(version.getYMDIndex()));
        if (version.getFormat() == EDGE_FORMAT.STATS) {
            builder.setStatsType(version.getStatsType(parts));
            builder.setSourceRelationship(parts.get(version.getRelationshipIndex()));
            builder.setSourceAttribute1(parts.get(version.getCategoryIndex()));
        } else {
            String[] subPieces = StringUtils.splitPreserveAllTokens(parts.get(version.getRelationshipIndex()), COL_SUB_SEPARATOR);
            builder.setSourceRelationship(subPieces[0]);
            builder.setSinkRelationship(subPieces[1]);
            subPieces = StringUtils.splitPreserveAllTokens(parts.get(version.getCategoryIndex()), COL_SUB_SEPARATOR);
            builder.setSourceAttribute1(subPieces[0]);
            builder.setSinkAttribute1(subPieces[1]);
        }

        if (version.hasAttribute2()) {
            builder.setAttribute2(parts.get(version.getAttribute2Index()));
        }
        if (version.hasAttribute3()) {
            builder.setAttribute3(parts.get(version.getAttribute3Index()));
        }
        if (version.getDateTypeIndex() >= 0) {
            builder.setDateType(DATE_TYPE.parse(parts.get(version.getDateTypeIndex())));
        }

        // set the rest
        builder.setColvis(key.getColumnVisibility(textCv));
        builder.setDeleted(key.isDeleted());
        builder.setTimestamp(key.getTimestamp());

        return builder.build();
    }

    public static EDGE_FORMAT determineEdgeFormat(Text colFam) {
        if (WritableComparator.compareBytes(colFam.getBytes(), 0, STATS_BYTES.length, STATS_BYTES, 0, STATS_BYTES.length) == 0) {
            return EDGE_FORMAT.STATS;
        } else {
            return EDGE_FORMAT.STANDARD;
        }
    }

    public static String getYYYYMMDD(Text colQual) {
        int numCharsToCheck = Math.min(DATE_LEN + 1, colQual.getLength());

        int firstSlashIndex = DATE_LEN; // there may not be a slash
        byte[] bytes = colQual.getBytes();

        // find the first slash if it exists
        for (int i = 0; i < numCharsToCheck; i++) {
            if (bytes[i] == COL_SEPARATOR_BYTE) {
                firstSlashIndex = i;
                break;
            }
        }

        try {
            return Text.decode(colQual.getBytes(), 0, Math.min(colQual.getLength(), firstSlashIndex));
        } catch (CharacterCodingException e) {
            // same behavior as EdgeKey.getParts
            throw new RuntimeException("Edge key column encoding exception", e);
        }
    }

    public static STATS_TYPE determineStatsType(Text colFam) {
        int offset = STATS_BYTES.length + 1;
        int secondSlashIndex = 0;
        int numCharsToCheck = Math.min(offset + 1 + STATS_TYPE.getMaxLength(), colFam.getLength());

        // faster to just compare bytes with STATS/statsType/ for each statsType

        for (int i = offset; i < numCharsToCheck; i++) {
            secondSlashIndex = i;
            if (colFam.getBytes()[i] == COL_SEPARATOR_BYTE) {
                break;
            }
        }
        int count = (secondSlashIndex > offset ? secondSlashIndex - offset : offset);
        try {
            return STATS_TYPE.getStatsType(Text.decode(colFam.getBytes(), offset, count));
        } catch (CharacterCodingException e) {
            // same behavior as EdgeKey.getParts
            throw new RuntimeException("Edge key column encoding exception", e);
        }
    }
}
