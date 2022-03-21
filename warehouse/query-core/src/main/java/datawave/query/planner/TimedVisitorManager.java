package datawave.query.planner;

import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import static datawave.query.planner.DefaultQueryPlanner.logQuery;

/**
 * Handles boilerplate code execution for operations like timing a visit call, logging the query tree, and eventualy validating the resulting query tree.
 */
public class TimedVisitorManager {
    
    private boolean isDebugEnabled; // log query tree after visit?
    private boolean validateAst; // validate the query tree after visit?
    
    // default, do not log, do not validate
    public TimedVisitorManager() {
        this(false, false);
    }
    
    /**
     * Sets flags to log the query or validate the query
     *
     * @param isDebugEnabled
     *            set this flag to log the query
     * @param validateAst
     *            set this flag to validate the query
     */
    public TimedVisitorManager(boolean isDebugEnabled, boolean validateAst) {
        this.isDebugEnabled = isDebugEnabled;
        this.validateAst = validateAst;
    }
    
    /**
     * Wrap visitor execution with a timer and debug msg
     *
     * @param timers
     *            a {@link QueryStopwatch}
     * @param stageName
     *            the name for this operation, for example "Fix Not Null Intent" or "Expand Regex Nodes"
     * @param visitorManager
     *            an interface that allows us to pass a lambda operation {@link VisitorManager}
     * @return the query tree, a {@link ASTJexlScript}
     * @throws DatawaveQueryException
     *             if something goes wrong
     */
    public ASTJexlScript timedVisit(QueryStopwatch timers, String stageName, VisitorManager visitorManager) throws DatawaveQueryException {
        
        ASTJexlScript script;
        TraceStopwatch stopwatch = timers.newStartedStopwatch("DefaultQueryPlanner - " + stageName);
        
        try {
            script = visitorManager.apply();
            
            if (isDebugEnabled) {
                logQuery(script, "Query after visit: " + stageName);
            }
            
            if (validateAst) {
                try {
                    ASTValidator.isValid(script, stageName);
                } catch (InvalidQueryTreeException e) {
                    throw new DatawaveQueryException(e);
                }
            }
        } finally {
            stopwatch.stop();
        }
        return script;
    }
    
    // no timers, only validate
    public ASTJexlScript validateAndVisit(VisitorManager visitorManager) throws DatawaveQueryException {
        ASTJexlScript script = visitorManager.apply();
        
        if (validateAst) {
            try {
                ASTValidator.isValid(script);
            } catch (InvalidQueryTreeException e) {
                throw new DatawaveQueryException(e);
            }
        }
        
        return script;
    }
}
