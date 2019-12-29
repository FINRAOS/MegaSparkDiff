package org.finra.msd.enums;

public enum VisualResultType {
    LEFT("LEFT"),
    RIGHT("RIGHT"),
    BOTH("BOTH");

    /**
     * Represents the visual result type
     */
    private final String text;

    /**
     * Initializes the constructor with the visual result type provided
     * @param text
     */
    private VisualResultType(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
