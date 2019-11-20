package de.hpi.msc.jschneider.actor.common.messageSenderProxy;

import de.hpi.msc.jschneider.actor.common.AbstractActorControl;
import de.hpi.msc.jschneider.actor.common.Message;
import lombok.val;
import lombok.var;
import org.agrona.collections.MutableLong;

public class MessageSenderProxyControl extends AbstractActorControl<MessageSenderProxyModel>
{
    public MessageSenderProxyControl(MessageSenderProxyModel model)
    {
        super(model);
    }

    @Override
    public void send(Message message)
    {
        val receiverRootPath = message.getReceiver().path().root();
        val receiver = getModel().getRemoteMessageReceivers().get(receiverRootPath);
        if (receiver == null)
        {
            getLog().error(String.format("Unable to send message to %1$s!", receiverRootPath));
            return;
        }

        receiver.tell(message, message.getSender());

        var counter = getModel().getSentMessageCounter().get(receiverRootPath);
        if (counter == null)
        {
            getModel().getSentMessageCounter().put(receiverRootPath, new MutableLong(1L));
        }
        else
        {
            counter.set(counter.get() + 1);
        }
    }

    public void onAddMessageReceiver(MessageSenderProxyMessages.AddMessageReceiverPoolMessage message)
    {
        getModel().getRemoteMessageReceivers().put(message.getMessageReceiverPool().path().root(), message.getMessageReceiverPool());
    }

    public void onMessage(Message message)
    {
        send(message);
    }
}
