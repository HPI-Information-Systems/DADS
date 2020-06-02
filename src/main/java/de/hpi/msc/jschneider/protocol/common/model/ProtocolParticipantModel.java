package de.hpi.msc.jschneider.protocol.common.model;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferManager;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public interface ProtocolParticipantModel
{
    void setSelfProvider(Callable<ActorRef> provider);

    void setProcessorProvider(Callable<Processor[]> provider);

    void setMaximumMessageSizeProvider(Callable<Integer> provider);

    ActorRef getSelf();

    Processor getLocalProcessor();

    Optional<Processor> getMasterProcessor();

    Optional<Processor> getProcessor(ActorRef actorRef);

    Optional<Processor> getProcessor(RootActorPath actorSystem);

    Optional<Processor> getProcessor(ProcessorId processorId);

    int getNumberOfProcessors();

    Processor[] getProcessors();

    int getMaximumMessageSize();

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
