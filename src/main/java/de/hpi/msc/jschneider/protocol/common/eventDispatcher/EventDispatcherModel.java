package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;

import java.util.Map;
import java.util.Set;

public interface EventDispatcherModel extends ProtocolParticipantModel
{
    Map<Class<? extends MessageExchangeMessages.RedirectableMessage>, Set<ActorRef>> getEventSubscribers();

    void setEventSubscribers(Map<Class<? extends MessageExchangeMessages.RedirectableMessage>, Set<ActorRef>> subscribers);
}
