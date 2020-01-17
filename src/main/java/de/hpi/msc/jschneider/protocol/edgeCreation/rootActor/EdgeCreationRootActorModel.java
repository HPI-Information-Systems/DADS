package de.hpi.msc.jschneider.protocol.edgeCreation.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class EdgeCreationRootActorModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private ActorRef worker;
}
