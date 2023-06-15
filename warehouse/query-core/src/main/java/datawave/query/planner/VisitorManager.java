package datawave.query.planner;

import datawave.query.exceptions.DatawaveQueryException;
import org.apache.commons.jexl3.parser.ASTJexlScript;

/**
 * An interface that lets us pass a lambda operation to the {@link TimedVisitorManager}
 */
public interface VisitorManager {
    ASTJexlScript apply() throws DatawaveQueryException;
}
