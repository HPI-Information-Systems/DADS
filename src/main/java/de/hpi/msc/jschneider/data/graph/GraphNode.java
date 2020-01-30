package de.hpi.msc.jschneider.data.graph;

import lombok.Builder;
import lombok.Getter;

@Builder @Getter
public class GraphNode
{
    private int intersectionSegment;
    private int index;

    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }

        if (!(obj instanceof GraphNode))
        {
            return false;
        }

        return hashCode() == obj.hashCode();
    }

    @Override
    public String toString()
    {
        return String.format("{%1$d_%2$d}", intersectionSegment, index);
    }
}
