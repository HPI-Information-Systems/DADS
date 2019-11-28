package de.hpi.msc.jschneider.protocol.common.model;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ProtocolParticipantModel
{
    void setSelfProvider(Callable<ActorRef> provider);

    void setSenderProvider(Callable<ActorRef> provider);

    void setProcessorProvider(Function<RootActorPath, Processor> provider);

    ActorRef getSelf();

    ActorRef getSender();

    Processor getLocalProcessor();

    Processor getProcessor(RootActorPath actorSystem);

    Consumer<ActorRef> getWatchActorCallback();

    void setWatchActorCallback(Consumer<ActorRef> callback);

    Consumer<ActorRef> getUnwatchActorCallback();

    void setUnwatchActorCallback(Consumer<ActorRef> callback);

    Set<ActorRef> getWatchedActors();

    Function<Props, ActorRef> getChildFactory();

    void setChildFactory(Function<Props, ActorRef> childFactory);

    Set<ActorRef> getChildActors();
}
