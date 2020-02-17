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
        return String.format("%1$s -> %2$s", from, to);
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
        return String.format("%1$s -[%2$d]-> %3$s", from, weight.get(), to);
    }
}
