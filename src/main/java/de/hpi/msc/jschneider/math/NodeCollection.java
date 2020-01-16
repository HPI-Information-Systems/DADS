package de.hpi.msc.jschneider.math;

import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Builder @Getter
public class NodeCollection
{
    private int intersectionSegment;
    private final List<Node> nodes = new ArrayList<>();
}
