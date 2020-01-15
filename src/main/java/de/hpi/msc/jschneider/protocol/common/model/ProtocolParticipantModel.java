package de.hpi.msc.jschneider.protocol.common.model;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferManager;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public interface ProtocolParticipantModel
{
    void setSelfProvider(Callable<ActorRef> provider);

    void setSenderProvider(Callable<ActorRef> provider);

    void setProcessorProvider(Callable<Processor[]> provider);

    void setMaximumMessageSizeProvider(Callable<Long> provider);

    ActorRef getSelf();

    ActorRef getSender();

    Processor getLocalProcessor();

    Optional<Processor> getMasterProcessor();

    Optional<Processor> getProcessor(RootActorPath actorSystem);

    int getNumberOfProcessors();

    Processor[] getProcessors();

    long getMaximumMessageSize();

    Consumer<ActorRef> getWatchActorCallback();

    void setWatchActorCallback(Consumer<ActorRef> callback);

    Consumer<ActorRef> getUnwatchActorCallback();

    void setUnwatchActorCallback(Consumer<ActorRef> callback);

    Set<ActorRef> getWatchedActors();

    ActorFactory getChildFactory();

    void setChildFactory(ActorFactory childFactory);

    Set<ActorRef> getChildActors();

    DataTransferManager getDataTransferManager();

    void setDataTransferManager(DataTransferManager dataTransferManager);
}