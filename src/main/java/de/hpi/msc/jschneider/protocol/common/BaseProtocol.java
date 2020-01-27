package de.hpi.msc.jschneider.protocol.common;

import akka.actor.ActorRef;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor @AllArgsConstructor @SuperBuilder @Getter
public class BaseProtocol implements Protocol
{
    private ProtocolType type;
    private ActorRef rootActor;
    private ActorRef eventDispatcher;
}
