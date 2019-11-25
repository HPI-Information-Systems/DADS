package de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.actor.common.AbstractActorModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class MessageDispatcherModel extends AbstractActorModel
{
    @NonNull @Getter
    private final Map<RootActorPath, ActorRef> messageDispatchers = new HashMap<>();
    @NonNull @Getter
    private final Map<RootActorPath, ActorRef> messageProxies = new HashMap<>();
}
