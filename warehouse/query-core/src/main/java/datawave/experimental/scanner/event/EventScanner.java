package datawave.experimental.scanner.event;

import org.apache.accumulo.core.data.Range;

import datawave.query.attributes.Document;

/**
 * Interface for event aggregation
 */
public interface EventScanner {

    Document fetchDocument(Range range, String datatypeUid);
}
