package de.hpi.msc.jschneider.data.graph;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder @Getter @EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class GraphNode
{
    @EqualsAndHashCode.Include
    private int intersectionSegment;
    @EqualsAndHashCode.Include
    private int index;

    @Override
    public String toString()
    {
        return String.format("{%1$d_%2$d}", intersectionSegment, index);
    }
}
