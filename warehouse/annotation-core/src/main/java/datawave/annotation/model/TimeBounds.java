package datawave.annotation.model;

public class TimeBounds {
    private float start;
    private float end;

    public TimeBounds(float start, float end) {
        this.start = start;
        this.end = end;
    }

    public float getStart() {
        return start;
    }

    public void setStart(float start) {
        this.start = start;
    }

    public float getEnd() {
        return end;
    }

    public void setEnd(float end) {
        this.end = end;
    }
}
