package de.hpi.msc.jschneider.protocol.scoring.worker;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
public class ScoringWorkerModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter @Builder.Default
    private boolean responsibilitiesReceived = false;
    @Getter
    private final List<Integer> edgeCreationOrder = new ArrayList<>();
    @Getter
    private final List<Integer> remoteEdgeCreationOrder = new ArrayList<>();
    @Setter @Getter
    private ActorRef processorResponsibleForPreviousSubSequences;
    @Setter @Getter
    private int queryPathLength;
    @Setter @Getter
    private Map<Integer, GraphEdge> edges;
}
