package datawave.configuration;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple CDI extension that manages lifetime of the {@link RefreshableScope}. In particular, this extension tells the {@link RefreshableScopeContext} to
 * invalidate all {@link RefreshableScope} bean references upon receipt of a {@link RefreshEvent} event, thus ending the lifetime of any such beans and causing
 * any references to be updated to newly created instances.
 */
public class RefreshableScopeExtension implements Extension {
    private Logger log = LoggerFactory.getLogger(getClass());
    private RefreshableScopeContext refreshableScopeContext;

    @SuppressWarnings("unused")
    void beforeBeanDiscovery(@Observes BeforeBeanDiscovery bbd, BeanManager bm) {
        bbd.addScope(RefreshableScope.class, true, false);
    }

    @SuppressWarnings("unused")
    void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
        refreshableScopeContext = new RefreshableScopeContext();
        abd.addContext(refreshableScopeContext);
    }

    @SuppressWarnings("unused")
    void onRefresh(@Observes RefreshEvent event, BeanManager bm) {
        if (refreshableScopeContext != null) {
            log.debug("Invalidating beans created using the RefreshableScope scope.");
            refreshableScopeContext.invalidate();
        }
    }
}
