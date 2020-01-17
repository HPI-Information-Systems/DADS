package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import de.hpi.msc.jschneider.utility.Counter;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder @Getter @EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class LocalEdge
{
    @EqualsAndHashCode.Include
    private LocalNode from;
    @EqualsAndHashCode.Include
    private LocalNode to;
    private final Counter weight = new Counter(1L);

    @Override
    public String toString()
    {
        return String.format("%1$s -[%2$d]-> %3$s", from, weight.get(), to);
    }
}
