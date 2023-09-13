package datawave.webservice.common.cache;

import org.apache.curator.framework.state.ConnectionStateListener;

public interface SharedBooleanListener extends ConnectionStateListener {

    void booleanHasChanged(SharedBooleanReader var1, boolean var2) throws Exception;
}
