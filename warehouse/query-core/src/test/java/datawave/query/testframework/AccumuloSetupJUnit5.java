package datawave.query.testframework;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Jupiter class-level fixture for loading Accumulo test data.
 */
public class AccumuloSetupJUnit5 extends AccumuloSetup implements BeforeAllCallback, AfterAllCallback {

    public AccumuloSetupJUnit5() {
        super();
    }

    public AccumuloSetupJUnit5(boolean assureTempFolderDeletion) {
        super(assureTempFolderDeletion);
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        createTempFolder();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        deleteTempFolder();
    }
}
