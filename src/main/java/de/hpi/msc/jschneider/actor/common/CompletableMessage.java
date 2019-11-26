package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.UUID;

public interface CompletableMessage extends Serializable
{
    UUID getId();

    ActorRef getSender();

    ActorRef getReceiver();
}
