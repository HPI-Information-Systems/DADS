package de.hpi.msc.jschneider.protocol.common;

import akka.actor.ActorRef;

public interface Protocol
{
    ProtocolType getType();

    ActorRef getRootActor();

    ActorRef getEventDispatcher();
}
