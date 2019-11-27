package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.concurrent.Callable;

@SuperBuilder
public abstract class AbstractMessageExchangeParticipantModel extends AbstractProtocolParticipantModel implements MessageExchangeParticipantModel
{
    @Setter
    private Callable<ActorRef> messageDispatcherProvider;

    public final ActorRef getMessageDispatcher()
    {
        try
        {
            return messageDispatcherProvider.call();
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve MessageDispatcher!", exception);
            return ActorRef.noSender();
        }
    }
}
