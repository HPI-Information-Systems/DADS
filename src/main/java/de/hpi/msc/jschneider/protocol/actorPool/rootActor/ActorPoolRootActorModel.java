package de.hpi.msc.jschneider.protocol.actorPool.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.ArrayDeque;
import java.util.Queue;

@SuperBuilder
public class ActorPoolRootActorModel extends AbstractProtocolParticipantModel
{
    @Getter
    private int maximumNumberOfWorkers;
    @NonNull @Getter
    private final Queue<ActorRef> workers = new ArrayDeque<>();
    @NonNull @Getter
    private final Queue<ActorPoolMessages.WorkMessage> workQueue = new ArrayDeque<>();
}
