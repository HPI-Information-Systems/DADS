package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface EventDispatcherModel
{
    Map<Class<?>, Set<ActorRef>> getEventSubscribers();

    void setEventSubscribers(Map<Class<?>, Set<ActorRef>> subscribers);

    void setMessageDispatcherProvider(Function<MessageExchangeMessages.MessageExchangeMessage, ActorRef> provider);

    ActorRef getMessageDispatcher(MessageExchangeMessages.MessageExchangeMessage message);
}
