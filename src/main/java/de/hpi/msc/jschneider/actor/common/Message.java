package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;

import java.io.Serializable;

public interface Message extends Serializable
{
    ActorRef getSender();

    ActorRef getReceiver();
}
