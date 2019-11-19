package de.hpi.msc.jschneider.actor.common.messageSenderProxy;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.actor.common.AbstractMessage;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class MessageSenderProxyMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class AddMessageReceiverMessage extends AbstractMessage
    {
        private static final long serialVersionUID = 2764089941012723670L;
        private ActorRef messageReceiver;
    }
}
