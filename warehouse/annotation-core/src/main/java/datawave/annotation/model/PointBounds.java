package datawave.annotation.model;

public class PointBounds {
    private Point topLeft;
    private Point bottomRight;
    private int rotation;

    public PointBounds(Point topLeft, Point bottomRight, int rotation) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
        this.rotation = rotation;
    }

    public Point getTopLeft() {
        return topLeft;
    }

    public void setTopLeft(Point topLeft) {
        this.topLeft = topLeft;
    }

    public Point getBottomRight() {
        return bottomRight;
    }

    public void setBottomRight(Point bottomRight) {
        this.bottomRight = bottomRight;
    }

    public int getRotation() {
        return rotation;
    }

    public void setRotation(int rotation) {
        this.rotation = rotation;
    }
}
