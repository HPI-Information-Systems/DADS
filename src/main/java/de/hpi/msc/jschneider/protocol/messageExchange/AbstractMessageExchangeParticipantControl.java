package de.hpi.msc.jschneider.protocol.messageExchange;

import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import lombok.val;

public abstract class AbstractMessageExchangeParticipantControl<TModel extends MessageExchangeParticipantModel> extends AbstractProtocolParticipantControl<TModel> implements MessageExchangeParticipantControl<TModel>
{
    protected AbstractMessageExchangeParticipantControl(TModel model)
    {
        super(model);
    }

    @Override
    public void complete(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val completedMessage = MessageExchangeMessages.MessageCompletedMessage.builder()
                                                                              .sender(message.getReceiver())
                                                                              .receiver(message.getSender())
                                                                              .completedMessageId(message.getId())
                                                                              .build();

        send(completedMessage);
    }

    @Override
    public void send(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val messageDispatcher = getModel().getMessageDispatcher();
        messageDispatcher.tell(message, message.getSender());
    }
}
