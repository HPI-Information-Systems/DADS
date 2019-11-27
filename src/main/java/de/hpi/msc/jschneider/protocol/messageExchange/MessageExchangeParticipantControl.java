package de.hpi.msc.jschneider.protocol.messageExchange;

import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;

public interface MessageExchangeParticipantControl<TModel extends MessageExchangeParticipantModel> extends ProtocolParticipantControl<TModel>
{
    void send(MessageExchangeMessages.MessageExchangeMessage message);

    void complete(MessageExchangeMessages.MessageExchangeMessage message);
}
