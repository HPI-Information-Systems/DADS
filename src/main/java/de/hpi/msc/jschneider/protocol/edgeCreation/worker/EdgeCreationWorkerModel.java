package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

@SuperBuilder
public class EdgeCreationWorkerModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private Int32Range localSegments;
    @Setter @Getter
    private int numberOfIntersectionSegments;
    @Setter @Getter
    private Int64Range localSubSequences;
    @Setter @Getter
    private Counter nextSubSequenceIndex;
    @Getter
    private final Map<Integer, List<LocalIntersection>> intersectionsInSegment = new HashMap<>();
    @Getter
    private final Map<Integer, float[]> nodesInSegment = new HashMap<>();
    @Setter @Getter
    private Queue<LocalIntersection> intersectionsToMatch;
    @Setter @Getter
    private GraphNode lastNode;
    @Getter
    private final Map<Integer, GraphEdge> edges = new HashMap<>();
    @Getter
    private final List<Integer> edgeCreationOrder = new ArrayList<>();
    @Getter
    private final Set<GraphNode> nodes = new HashSet<>();
}
