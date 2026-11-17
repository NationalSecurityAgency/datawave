package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;

import org.w3c.dom.Node;

public interface AgeOffFileLoaderDependencyProvider {
    InputStream getParentStream(Node parent) throws IOException;
}
