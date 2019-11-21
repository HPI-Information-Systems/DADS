package de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.actor.common.AbstractMessage;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

public class MessageDispatcherMessages
{
    @SuperBuilder
    public static class AddMessageDispatchersMessage extends AbstractMessage
    {
        private static final long serialVersionUID = 2016011952964740818L;
        @Getter
        private ActorRef[] messageDispatchers;
    }
}
