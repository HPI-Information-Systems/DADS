package de.hpi.msc.jschneider.protocol.graphMerging.partitionReceiver;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.Set;

@SuperBuilder
public class GraphPartitionReceiverModel extends AbstractProtocolParticipantModel
{
    @Getter
    private ActorRef graphMerger;
    @Setter @Getter
    private Set<RootActorPath> runningDataTransfers;
    @Setter @Getter
    private RootActorPath[] workerSystems;
}
