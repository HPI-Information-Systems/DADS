package de.hpi.msc.jschneider.protocol;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder @Getter @Setter
public class TestMessage extends MessageExchangeMessages.MessageExchangeMessage
{
    private static final long serialVersionUID = -3615953200640061693L;

    public static TestMessage empty()
    {
        return builder().sender(ActorRef.noSender())
                        .receiver(ActorRef.noSender())
                        .build();
    }
}
