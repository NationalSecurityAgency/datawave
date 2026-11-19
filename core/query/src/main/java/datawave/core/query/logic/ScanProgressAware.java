package datawave.core.query.logic;

/** Opts a result transformer into delivering scan progress as intermediate results. */
public interface ScanProgressAware {
    void setReturnScanProgress(boolean returnScanProgress);

    default boolean tracksWebserverProgress() {
        return false;
    }

    default long getWebserverProgress() {
        return 0;
    }
}
