package de.hpi.msc.jschneider.utility;

import lombok.Builder;
import lombok.Getter;

@Builder @Getter
public class Int64Range
{
    // Start is included.
    private long start;
    // End is excluded.
    private long end;

    public long length()
    {
        return end - start;
    }

    public boolean contains(long value)
    {
        return value >= start && value < end;
    }
}
