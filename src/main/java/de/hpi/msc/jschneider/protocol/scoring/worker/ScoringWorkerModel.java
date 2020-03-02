package de.hpi.msc.jschneider.protocol.scoring.worker;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuperBuilder
public class ScoringWorkerModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final Set<ProcessorId> participants = new HashSet<>();
    @NonNull @Getter
    private final Set<ProcessorId> missingMinimumAndMaximumValueSenders = new HashSet<>();
    @Setter @Getter @Builder.Default
    private boolean waitForRemoteEdgeCreationOrder = true;
    @Setter @Getter @Builder.Default
    private boolean waitForRemotePathScores = true;
    @Setter @Getter @Builder.Default
    private double[] overlappingPathScores = new double[0];
    @Setter @Getter
    private List<List<Integer>> edgeCreationOrder;
    @Setter @Getter
    private int[][] remoteEdgeCreationOrder;
    @Setter @Getter
    private ActorRef processorResponsibleForNextSubSequences;
    @Setter @Getter
    private int queryPathLength;
    @Setter @Getter
    private int subSequenceLength;
    @Setter @Getter @Builder.Default
    private double globalMinimumScore = Double.MAX_VALUE;
    @Setter @Getter @Builder.Default
    private double globalMaximumScore = Double.MIN_VALUE;
    @Setter @Getter
    private Map<Integer, GraphEdge> edges;
    @Setter @Getter
    private Map<Integer, Long> nodeDegrees;
    @NonNull @Getter
    private final List<Double> pathScores = new ArrayList<>();
}
