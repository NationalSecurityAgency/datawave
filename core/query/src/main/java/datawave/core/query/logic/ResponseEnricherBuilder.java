package datawave.core.query.logic;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.marking.MarkingFunctions;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.query.result.event.ResponseObjectFactory;

public interface ResponseEnricherBuilder {
    public ResponseEnricherBuilder withConfig(GenericQueryConfiguration config);

    public ResponseEnricherBuilder withMarkingFunctions(MarkingFunctions functions);

    public ResponseEnricherBuilder withResponseObjectFactory(ResponseObjectFactory factory);

    public ResponseEnricherBuilder withCurrentUser(ProxiedUserDetails user);

    public ResponseEnricherBuilder withServerUser(ProxiedUserDetails user);

    public ResponseEnricher build();
}
