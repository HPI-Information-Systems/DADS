package de.hpi.msc.jschneider.utility;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder @Getter @EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Int32Range
{
    @EqualsAndHashCode.Include
    private int from;
    @EqualsAndHashCode.Include
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
