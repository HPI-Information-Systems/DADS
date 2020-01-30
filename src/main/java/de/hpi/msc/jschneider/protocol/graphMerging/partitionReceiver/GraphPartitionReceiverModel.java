package de.hpi.msc.jschneider.protocol.graphMerging.partitionReceiver;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuperBuilder
public class GraphPartitionReceiverModel extends AbstractProtocolParticipantModel
{
    @Getter
    private ActorRef graphMerger;
    @Setter @Getter
    private Set<ProcessorId> runningDataTransfers;
    @Setter @Getter
    private ProcessorId[] workerSystems;
    @Getter @Builder.Default
    private final Map<ProcessorId, GraphMergingMessages.InitializeEdgePartitionTransferMessage> initializationMessages = new HashMap<>();
}
