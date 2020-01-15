package de.hpi.msc.jschneider.utility;

import lombok.Builder;
import lombok.Getter;

@Builder @Getter
public class Int32Range
{
    // Start is included
    private int from;
    // End is excluded
    private int to;

    public int length()
    {
        return to - from;
    }

    public boolean contains(int value)
    {
        return value >= from && value < to;
    }
}
