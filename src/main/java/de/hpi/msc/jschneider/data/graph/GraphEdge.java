package de.hpi.msc.jschneider.data.graph;

import de.hpi.msc.jschneider.utility.Counter;
import lombok.Builder;
import lombok.Getter;

@Builder
public class GraphEdge
{
    @Getter
    private GraphNode from;
    @Getter
    private GraphNode to;
    @Builder.Default
    private Counter weight = new Counter(1L);
    private String key;
    private String thisAsString;

    public long getWeight()
    {
        return weight.get();
    }

    public void incrementWeight()
    {
        weight.increment();
    }

    public void setWeight(long weight)
    {
        this.weight = new Counter(Math.max(1L, weight));
    }

    public String getKey()
    {
        if (key == null)
        {
            key = from + " -> " + to;
        }
        return key;
    }

    @Override
    public int hashCode()
    {
        return getKey().hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }

        if (!(obj instanceof GraphEdge))
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
            thisAsString = from + " -[" + weight.get() + "]-> " + to;
        }
        return thisAsString;
    }
}
