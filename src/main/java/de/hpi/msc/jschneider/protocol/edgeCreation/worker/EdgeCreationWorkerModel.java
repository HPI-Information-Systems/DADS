package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
public class EdgeCreationWorkerModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private Int32Range localSegments;
    @Setter @Getter
    private ActorRef nextResponsibleProcessor;
    @Setter @Getter
    private int numberOfIntersectionSegments;
    @Setter @Getter
    private Int64Range localSubSequences;
    @Setter @Getter
    private Counter nextSubSequenceIndex;
    @Getter
    private final Map<Integer, List<LocalIntersection>> intersectionsInSegment = new HashMap<>();
    @Getter
    private final Map<Integer, double[]> nodesInSegment = new HashMap<>();
    @Setter @Getter
    private int initialNumberOfIntersectionsToMatch;
    @Setter @Getter
    private int progressLogInterval;
    @Setter @Getter
    private int nextProgressLog;
    @Setter @Getter
    private List<LocalIntersection> intersectionsToMatch;
    @Setter @Getter
    private GraphNode lastNode;
    @NonNull @Getter @Builder.Default
    private final Graph graph = new Graph();
    @Setter @Getter
    private LocalDateTime startTime;
    @Setter @Getter
    private LocalDateTime endTime;
}
