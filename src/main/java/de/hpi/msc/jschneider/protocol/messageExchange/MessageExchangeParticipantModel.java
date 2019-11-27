package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;

import java.util.function.Function;

public interface MessageExchangeParticipantModel extends ProtocolParticipantModel
{
    void setMessageDispatcherProvider(Function<MessageExchangeMessages.MessageExchangeMessage, ActorRef> provider);

    ActorRef getMessageDispatcher(MessageExchangeMessages.MessageExchangeMessage message);
}
