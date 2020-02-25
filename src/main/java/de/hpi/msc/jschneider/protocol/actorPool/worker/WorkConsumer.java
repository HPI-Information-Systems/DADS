package de.hpi.msc.jschneider.protocol.actorPool.worker;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;

@FunctionalInterface
public interface WorkConsumer
{
    void process(ActorPoolWorkerControl control, MessageExchangeMessages.RedirectableMessage workLoad);
}
