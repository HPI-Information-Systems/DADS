package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.function.Function;

@SuperBuilder
public abstract class AbstractMessageExchangeParticipantModel extends AbstractProtocolParticipantModel implements MessageExchangeParticipantModel
{
    @Setter
    private Function<MessageExchangeMessages.MessageExchangeMessage, ActorRef> messageDispatcherProvider;

    public final ActorRef getMessageDispatcher(MessageExchangeMessages.MessageExchangeMessage message)
    {
        try
        {
            return messageDispatcherProvider.apply(message);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve MessageDispatcher!", exception);
            return ActorRef.noSender();
        }
    }
}
