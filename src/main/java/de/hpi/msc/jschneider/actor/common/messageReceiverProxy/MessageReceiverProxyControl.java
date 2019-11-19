package de.hpi.msc.jschneider.actor.common.messageReceiverProxy;

import de.hpi.msc.jschneider.actor.common.AbstractActorControl;
import de.hpi.msc.jschneider.actor.common.Message;
import lombok.val;
import org.agrona.collections.MutableLong;

public class MessageReceiverProxyControl extends AbstractActorControl<MessageReceiverProxyModel>
{
    public MessageReceiverProxyControl(MessageReceiverProxyModel model)
    {
        super(model);
    }

    public void onMessage(Message message)
    {
        val senderRootPath = message.getSender().path().root();

        message.getReceiver().tell(message, message.getSender());

        val counter = getModel().getReceivedMessageCounter().get(senderRootPath);
        if (counter == null)
        {
            getModel().getReceivedMessageCounter().put(senderRootPath, new MutableLong(1L));
        }
        else
        {
            counter.set(counter.get() + 1);
        }
    }
}
