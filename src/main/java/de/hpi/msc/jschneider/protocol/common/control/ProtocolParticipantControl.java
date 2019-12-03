package de.hpi.msc.jschneider.protocol.common.control;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

import java.util.Optional;

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

    boolean tryWatch(ActorRef subject);

    boolean tryUnwatch(ActorRef subject);

    Optional<ActorRef> trySpawnChild(Props props);

    void onAny(Object message);

    void forward(MessageExchangeMessages.MessageExchangeMessage message, ActorRef receiver);

    void send(MessageExchangeMessages.MessageExchangeMessage message);

    void send(Object message, ActorRef receiver);

    void sendEvent(ProtocolType protocolType, MessageExchangeMessages.MessageExchangeMessage event);

    void subscribeToLocalEvent(ProtocolType protocolType, Class<?> eventType);

    void subscribeToMasterEvent(ProtocolType protocolType, Class<?> eventType);

    void subscribeToEvent(RootActorPath actorSystem, ProtocolType protocolType, Class<?> eventType);

    void complete(MessageExchangeMessages.MessageExchangeMessage message);
}
