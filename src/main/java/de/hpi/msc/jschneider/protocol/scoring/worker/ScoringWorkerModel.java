package de.hpi.msc.jschneider.protocol.scoring.worker;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

@SuperBuilder
public class ScoringWorkerModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter @Builder.Default
    private boolean responsibilitiesReceived = false;
    @Setter @Getter @Builder.Default
    private boolean waitForRemoteEdgeCreationOrder = true;
    @Setter @Getter
    private List<List<Integer>> edgeCreationOrder;
    @Setter @Getter
    private long numberOfMissingEdges;
    @Setter @Getter
    private int[][] remoteEdgeCreationOrder;
    @Setter @Getter
    private ActorRef processorResponsibleForNextSubSequences;
    @Setter @Getter
    private int queryPathLength;
    @Setter @Getter
    private Map<Integer, GraphEdge> edges;
    @Setter @Getter
    private Map<Integer, Long> nodeDegrees;
}
