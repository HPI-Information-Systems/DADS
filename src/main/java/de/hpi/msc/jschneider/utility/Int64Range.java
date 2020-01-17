package de.hpi.msc.jschneider.utility;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder @Getter @EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Int64Range
{
    @EqualsAndHashCode.Include
    private long from;
    @EqualsAndHashCode.Include
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
