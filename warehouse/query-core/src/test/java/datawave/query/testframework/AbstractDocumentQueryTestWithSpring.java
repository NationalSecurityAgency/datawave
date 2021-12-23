package datawave.query.testframework;

import datawave.query.planner.QueryPlanner;
import datawave.query.planner.document.batch.DocumentQueryPlanner;
import datawave.query.tables.document.batch.DocumentLogic;

/**
 * Provides the basic initialization required to initialize and execute queries. This class will initialize the following runtime settings:
 * <ul>
 * <li>timezone => GMT</li>
 * <li>file.encoding => UTF-8</li>
 * <li>DATAWAVE_INGEST_HOME => target directory</li>
 * <li>hadoop.home.dir => target directory</li>
 * </ul>
 */
public abstract class AbstractDocumentQueryTestWithSpring extends AbstractBaseQueryFramework<DocumentLogic> {

    protected AbstractDocumentQueryTestWithSpring(RawDataManager mgr) {
        super(mgr);
    }

    @Override
    protected DocumentLogic createQueryLogic(){
        // TODO: use spring and xml to create this instead
        return new DocumentLogic();
    }

    @Override
    protected QueryPlanner createQueryPlanner(){
        return new DocumentQueryPlanner();
    }
}