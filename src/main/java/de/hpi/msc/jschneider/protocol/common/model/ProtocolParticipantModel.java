package de.hpi.msc.jschneider.protocol.common.model;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ProtocolParticipantModel
{
    void setSelfProvider(Callable<ActorRef> provider);

    void setSenderProvider(Callable<ActorRef> provider);

    ActorRef getSelf();

    ActorRef getSender();

    Consumer<ActorRef> getWatchActorCallback();

    void setWatchActorCallback(Consumer<ActorRef> callback);

    Consumer<ActorRef> getUnwatchActorCallback();

    void setUnwatchActorCallback(Consumer<ActorRef> callback);

    Set<ActorRef> getWatchedActors();

    Function<Props, ActorRef> getChildFactory();

    void setChildFactory(Function<Props, ActorRef> childFactory);

    Set<ActorRef> getChildActors();
}
