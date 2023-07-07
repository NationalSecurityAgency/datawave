package datawave.configuration;

import org.apache.deltaspike.core.api.projectstage.ProjectStage;
import org.apache.deltaspike.core.api.projectstage.ProjectStageHolder;

/**
 * Custom project stage to allow injected beans to be avoided when CDI is used in an Embedded manner (e.g., MapReduce jobs).
 */
public class DatawaveEmbeddedProjectStageHolder implements ProjectStageHolder {
    public static final class DatawaveEmbedded extends ProjectStage {
        private static final long serialVersionUID = 1029094387976167179L;
    }

    @SuppressWarnings("unused")
    public static final DatawaveEmbedded DatawaveEmbedded = new DatawaveEmbedded();
}
