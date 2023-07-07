package datawave.query.function.ws;

import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;

import datawave.query.attributes.Document;
import datawave.query.function.MaskedValueFilterFactory;
import datawave.query.function.MaskedValueFilterInterface;

/**
 * Replicates the {@link datawave.query.function.MaskedValueFilterInterface} inside the webservice.
 * <p>
 * This is only used in conjunction with a {@link datawave.query.jexl.DatawavePartialInterpreter}.
 */
public class MaskedValueFilterFunction implements Function<Entry<Key,Document>,Entry<Key,Document>> {

    private final MaskedValueFilterInterface mvfi;

    public MaskedValueFilterFunction(boolean includeGroupingContext, boolean reducedResponse) {
        this.mvfi = MaskedValueFilterFactory.get(includeGroupingContext, reducedResponse);
    }

    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> input) {
        return mvfi.apply(input);
    }
}
