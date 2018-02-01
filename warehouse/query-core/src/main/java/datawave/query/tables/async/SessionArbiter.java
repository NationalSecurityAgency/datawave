package datawave.query.tables.async;

public interface SessionArbiter {
    
    public boolean canRun(ScannerChunk chunk);
}
