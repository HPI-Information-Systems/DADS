package de.hpi.msc.jschneider.utility;

import lombok.Builder;
import lombok.Getter;

@Builder @Getter
public class Int32Range
{
    // Start is included
    private int start;
    // End is excluded
    private int end;

    public int length()
    {
        return end - start;
    }

    public boolean contains(int value)
    {
        return value >= start && value < end;
    }
}
