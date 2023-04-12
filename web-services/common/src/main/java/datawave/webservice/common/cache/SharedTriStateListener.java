package datawave.webservice.common.cache;

import org.apache.curator.framework.state.ConnectionStateListener;

public interface SharedTriStateListener extends ConnectionStateListener {
    
    void stateHasChanged(SharedTriStateReader var1, SharedTriState.STATE var2) throws Exception;
}
