package datawave.query.tables.async;

public interface SessionArbiter {

    boolean canRun(ScannerChunk chunk);
}
