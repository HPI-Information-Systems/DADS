package de.hpi.msc.jschneider.protocol.common.control;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

import java.util.Optional;
import java.util.function.Function;

public interface ProtocolParticipantControl<TModel extends ProtocolParticipantModel>
{
    TModel getModel();

    void setModel(TModel model) throws NullPointerException;

    void preStart();

    void postStop();

    ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder);

    Optional<Protocol> getLocalProtocol(ProtocolType protocolType);

    Optional<Protocol> getMasterProtocol(ProtocolType protocolType);

    Optional<Protocol> getProtocol(RootActorPath actorSystem, ProtocolType protocolType);

    Optional<Protocol> getProtocol(ActorRef actorRef, ProtocolType protocolType);

    Optional<Protocol> getProtocol(ProcessorId processorId, ProtocolType protocolType);

    boolean tryWatch(ActorRef subject);

    boolean tryUnwatch(ActorRef subject);

    <TChildModel extends ProtocolParticipantModel, TChildControl extends ProtocolParticipantControl<TChildModel>> Optional<ActorRef> trySpawnChild(TChildControl control, String name);

    void onAny(Object message);

    void forward(MessageExchangeMessages.RedirectableMessage message, ActorRef receiver);

    void send(MessageExchangeMessages.MessageExchangeMessage message);

    void send(Object message, ActorRef receiver);

    boolean trySendEvent(ProtocolType protocolType, Function<ActorRef, MessageExchangeMessages.RedirectableMessage> eventFactory);

    void subscribeToLocalEvent(ProtocolType protocolType, Class<? extends MessageExchangeMessages.RedirectableMessage> eventType);

    void subscribeToMasterEvent(ProtocolType protocolType, Class<? extends MessageExchangeMessages.RedirectableMessage> eventType);

    void subscribeToEvent(ProcessorId processorId, ProtocolType protocolType, Class<? extends MessageExchangeMessages.RedirectableMessage> eventType);

    void complete(MessageExchangeMessages.MessageExchangeMessage message);
}
