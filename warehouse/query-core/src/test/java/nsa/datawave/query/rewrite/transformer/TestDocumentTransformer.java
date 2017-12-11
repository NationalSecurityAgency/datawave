package nsa.datawave.query.rewrite.transformer;

import nsa.datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestDocumentTransformer {

    static protected Key correctKey(Key key) {
        String colFam = key.getColumnFamily().toString();
        String[] colFamParts = StringUtils.split(colFam, '\0');
        if (colFamParts.length == 3) {
            // skip part 0 and return parts 1 & 2 for the colFam
            return new Key(key.getRow(), new Text(colFamParts[1] + '\0' + colFamParts[2]), key.getColumnQualifier(), key.getColumnVisibility(),
                    key.getTimestamp());
        } else {
            return key;
        }
    }

    public static void main(String[] args) {
        Key key = new Key("20171201_0", "+aE1\0datatype\0uid");
        System.out.println(key.toString());
        System.out.println(correctKey(key).toString());
    }


    
}
