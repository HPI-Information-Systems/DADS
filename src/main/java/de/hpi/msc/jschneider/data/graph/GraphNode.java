package de.hpi.msc.jschneider.data.graph;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor @AllArgsConstructor @Builder @Getter
public class GraphNode
{
    private int intersectionSegment;
    private int index;
    private String thisAsString;

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
        if (thisAsString == null)
        {
            thisAsString = "{" + intersectionSegment + "_" + index + "}";
        }
        return thisAsString;
    }
}
