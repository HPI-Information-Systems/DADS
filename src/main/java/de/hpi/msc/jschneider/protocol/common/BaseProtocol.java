package de.hpi.msc.jschneider.protocol.common;

import akka.actor.ActorRef;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder @Getter
public class BaseProtocol implements Protocol
{
    @NonNull
    private ProtocolType type;
    @NonNull
    private ActorRef rootActor;
    @NonNull
    private ActorRef eventDispatcher;
}
