package de.hpi.msc.jschneider.protocol.graphMerging.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class GraphMergingRootActorModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private ActorRef partitionSender;
    @Setter @Getter
    private ActorRef graphReceiver;
    @Setter @Getter
    private ActorRef graphMerger;
    @Setter @Getter
    private ActorRef partitionReceiver;
}
