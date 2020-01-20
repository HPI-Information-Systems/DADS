package de.hpi.msc.jschneider.data.graph;

import de.hpi.msc.jschneider.utility.Counter;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder @EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class GraphEdge
{
    @Getter @EqualsAndHashCode.Include
    private GraphNode from;
    @Getter @EqualsAndHashCode.Include
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

    @Override
    public String toString()
    {
        return String.format("%1$s -[%2$d]-> %3$s", from, weight.get(), to);
    }
}
