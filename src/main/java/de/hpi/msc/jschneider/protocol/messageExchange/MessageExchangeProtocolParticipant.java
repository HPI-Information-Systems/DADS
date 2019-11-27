package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;

public class MessageExchangeProtocolParticipant<TModel extends MessageExchangeParticipantModel, TControl extends MessageExchangeParticipantControl<TModel>> extends ProtocolParticipant<TModel, TControl>
{
    protected MessageExchangeProtocolParticipant(TControl control)
    {
        super(control);
    }

    public static <TModel extends MessageExchangeParticipantModel, TControl extends MessageExchangeParticipantControl<TModel>> Props props(TControl control)
    {
        return Props.create(MessageExchangeProtocolParticipant.class, () -> new MessageExchangeProtocolParticipant(control));
    }

    @Override
    protected void initializeModel(TModel model)
    {
        super.initializeModel(model);

        model.setMessageDispatcherProvider(MessageExchangeProtocol::getLocalRootActor);
    }
}
