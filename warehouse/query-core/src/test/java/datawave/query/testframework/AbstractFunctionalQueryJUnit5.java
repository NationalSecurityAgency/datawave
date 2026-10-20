package datawave.query.testframework;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Jupiter lifecycle for functional query tests that still share the query assertions and setup in {@link AbstractFunctionalQuery}.
 */
public abstract class AbstractFunctionalQueryJUnit5 extends AbstractFunctionalQuery {

    @TempDir
    Path temporaryDirectory;

    protected AbstractFunctionalQueryJUnit5(RawDataManager mgr) {
        super(mgr);
    }

    @BeforeEach
    public void setUpFunctionalQuery() throws IOException {
        querySetUp();
    }

    @Override
    protected File createTemporaryFolder() throws IOException {
        return Files.createTempDirectory(temporaryDirectory, "query-").toFile();
    }
}
