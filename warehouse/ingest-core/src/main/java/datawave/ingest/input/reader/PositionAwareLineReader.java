package datawave.ingest.input.reader;

public interface PositionAwareLineReader extends LineReader {

    void setPos(long newPos);

    long getPos();

    long getEnd();

    LfLineReader getLfLineReader();

    int getMaxLineLength();

}
