package datawave.visibility.standard;

import java.util.Collection;

import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.visibility.AccessExpressionUtil;
import datawave.visibility.FlattenedVisibilityCache;
import datawave.visibility.VisibilityFunctions;

/** DataWave's stock policy: normalized conjunction and cached Accumulo flattening. */
public final class StandardVisibilityFunctions implements VisibilityFunctions {
    @Override
    public ColumnVisibility combine(Collection<ColumnVisibility> visibilities) {
        return AccessExpressionUtil.conjunction(visibilities);
    }

    @Override
    public byte[] flatten(ColumnVisibility visibility) {
        return FlattenedVisibilityCache.flatten(visibility);
    }
}
