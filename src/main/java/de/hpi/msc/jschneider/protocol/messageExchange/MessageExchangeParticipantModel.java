package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;

import java.util.concurrent.Callable;

public interface MessageExchangeParticipantModel extends ProtocolParticipantModel
{
    void setMessageDispatcherProvider(Callable<ActorRef> provider);

    ActorRef getMessageDispatcher();
}
