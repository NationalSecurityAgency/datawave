package datawave.experimental.threads;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

public class GenericFutureCallback implements FutureCallback {

    private static final Logger log = Logger.getLogger(GenericFutureCallback.class);

    @Override
    public void onSuccess(Object result) {

    }

    @Override
    public void onFailure(Throwable t) {
        log.error(t.getMessage());
        Throwables.propagateIfPossible(t, RuntimeException.class);
    }
}
