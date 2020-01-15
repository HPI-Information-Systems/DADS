package de.hpi.msc.jschneider.protocol.nodeCreation.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class NodeCreationRootActorModel extends AbstractProtocolParticipantModel
{
    @Getter @Setter
    private ActorRef coordinator;
    @Getter @Setter
    private ActorRef worker;
}
