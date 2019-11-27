package de.hpi.msc.jschneider.protocol.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class MessageDispatcherModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final Map<RootActorPath, ActorRef> messageDispatchers = new HashMap<>();
    @NonNull @Getter
    private final Map<RootActorPath, ActorRef> messageProxies = new HashMap<>();
}
