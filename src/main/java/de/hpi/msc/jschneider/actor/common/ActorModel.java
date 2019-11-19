package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ActorModel
{
    ActorRef getSelf();

    ActorRef getSender();

    ActorRef getMessageSenderProxy();

    Set<ActorRef> getChildActors();

    Set<ActorRef> getWatchedActors();

    Function<Props, ActorRef> getChildFactory();

    Consumer<ActorRef> getWatchActorCallback();
}
