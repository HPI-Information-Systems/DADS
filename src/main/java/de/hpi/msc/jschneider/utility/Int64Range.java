package de.hpi.msc.jschneider.utility;

import lombok.Builder;
import lombok.Getter;

@Builder @Getter
public class Int64Range
{
    // Start is included.
    private long from;
    // End is excluded.
    private long to;

    public long length()
    {
        return to - from;
    }

    public boolean contains(long value)
    {
        return value >= from && value < to;
    }
}
