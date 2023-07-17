package datawave.webservice.query.logic;

import java.security.Principal;

import datawave.marking.MarkingFunctions;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.result.event.ResponseObjectFactory;

public interface ResponseEnricherBuilder {
    public ResponseEnricherBuilder withConfig(GenericQueryConfiguration config);

    public ResponseEnricherBuilder withMarkingFunctions(MarkingFunctions functions);

    public ResponseEnricherBuilder withResponseObjectFactory(ResponseObjectFactory factory);

    public ResponseEnricherBuilder withPrincipal(Principal principal);

    public ResponseEnricher build();
}
