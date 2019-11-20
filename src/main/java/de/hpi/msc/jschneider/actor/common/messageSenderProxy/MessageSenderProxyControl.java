package de.hpi.msc.jschneider.actor.common.messageSenderProxy;

import de.hpi.msc.jschneider.actor.common.AbstractActorControl;
import de.hpi.msc.jschneider.actor.common.Message;
import de.hpi.msc.jschneider.actor.utility.actorPool.RoundRobinActorPool;
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
        val receiverPool = getModel().getRemoteMessageReceivers().get(receiverRootPath);
        if (receiverPool == null)
        {
            getLog().error(String.format("Unable to send message to %1$s!", receiverRootPath));
            return;
        }

        receiverPool.getActor().tell(message, message.getSender());

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
        if (message.getMessageReceivers().length < 1)
        {
            getLog().warn(String.format("%1$s tried to add 0 remote message receivers to %2$s.", getSender().path(), getClass().getName()));
            return;
        }

        val remoteRootPath = message.getSender().path().root();
        getModel().getRemoteMessageReceivers().put(remoteRootPath, new RoundRobinActorPool(message.getMessageReceivers()));
    }

    public void onMessage(Message message)
    {
        send(message);
    }
}
