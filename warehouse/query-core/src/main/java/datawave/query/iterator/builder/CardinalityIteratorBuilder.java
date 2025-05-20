package datawave.query.iterator.builder;

import org.apache.hadoop.io.Text;

import datawave.query.iterator.logic.IndexIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;

public class CardinalityIteratorBuilder extends IndexIteratorBuilder {

    public IndexIteratorBridge build() {
        if (notNull(field, value, source, datatypeFilter, keyTform, timeFilter)) {

            //  @formatter:off
            IndexIterator iter = IndexIterator.builder(new Text(field), new Text(value), source)
                    .withTimeFilter(timeFilter)
                    .withTypeMetadata(typeMetadata)
                    .shouldBuildDocument(buildDocument)
                    .withDatatypeFilter(datatypeFilter)
                    .withAggregation(this.keyTform).build();
            //  @formatter:on

            IndexIteratorBridge itr = new IndexIteratorBridge(iter, getNode(), getField());
            field = null;
            value = null;
            source = null;
            timeFilter = null;
            datatypeFilter = null;
            keyTform = null;
            node = null;
            return itr;
        } else {
            StringBuilder msg = new StringBuilder(256);
            msg.append("Cannot build iterator-- a field was null!\n");
            if (field == null) {
                msg.append("\tField was null!\n");
            }
            if (value == null) {
                msg.append("\tValue was null!\n");
            }
            if (source == null) {
                msg.append("\tSource was null!\n");
            }
            msg.setLength(msg.length() - 1);
            throw new IllegalStateException(msg.toString());
        }
    }

}
