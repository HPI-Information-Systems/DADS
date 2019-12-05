package de.hpi.msc.jschneider.protocol.common.model;

import akka.actor.ActorRef;
import akka.actor.Props;

@FunctionalInterface
public interface ActorFactory
{
    ActorRef create(Props props, String name);
}
