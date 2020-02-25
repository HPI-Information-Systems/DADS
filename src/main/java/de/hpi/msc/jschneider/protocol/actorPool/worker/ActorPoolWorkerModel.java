package de.hpi.msc.jschneider.protocol.actorPool.worker;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class ActorPoolWorkerModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private ActorRef supervisor;
}
