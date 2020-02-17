package de.hpi.msc.jschneider.protocol;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder @Getter @Setter
public class TestMessage extends MessageExchangeMessages.RedirectableMessage
{
    private static final long serialVersionUID = -3615953200640061693L;

    @Override
    public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
    {
        return builder().sender(getSender())
                        .receiver(newReceiver)
                        .forwarder(getReceiver())
                        .build();
    }
}
