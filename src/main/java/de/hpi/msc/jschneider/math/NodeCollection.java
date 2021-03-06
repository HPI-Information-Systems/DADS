package de.hpi.msc.jschneider.math;

import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import lombok.Getter;

@Getter
public class NodeCollection
{
    private int intersectionSegment;
    private final DoubleBigList nodes;

    public NodeCollection(int intersectionSegment, long numberOfNodes)
    {
        this.intersectionSegment = intersectionSegment;
        nodes = new DoubleBigArrayBigList(Math.max(numberOfNodes, DoubleBigArrayBigList.DEFAULT_INITIAL_CAPACITY));
    }
}
