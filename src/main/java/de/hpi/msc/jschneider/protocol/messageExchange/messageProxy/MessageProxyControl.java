package de.hpi.msc.jschneider.protocol.messageExchange.messageProxy;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import lombok.var;

public class MessageProxyControl extends AbstractProtocolParticipantControl<MessageProxyModel>
{
    public MessageProxyControl(MessageProxyModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(MessageExchangeMessages.MessageCompletedMessage.class, this::onMessageCompleted)
                      .match(MessageExchangeMessages.MessageExchangeMessage.class, this::onMessage);
    }

    private void onMessageCompleted(MessageExchangeMessages.MessageCompletedMessage message)
    {
        val senderQueue = getModel().getMessageQueues().get(message.getSender().path());
        if (senderQueue == null)
        {
            getLog().debug(String.format("Unable to get %1$s in order to complete earlier message! (Sender = %2$s)",
                                         ActorMessageQueue.class.getName(),
                                         message.getSender().path()));
            return;
        }
        else if (!senderQueue.tryAcknowledge(message.getCompletedMessageId()))
        {
            getLog().error(String.format("Unexpected message completion for a message we never seen before! (sender = %1$s, receiver = %2$s)",
                                         message.getSender().path(),
                                         message.getReceiver().path()));
        }

        getModel().getTotalNumberOfEnqueuedMessages().decrement();
        dequeueAndSend(senderQueue);

        if (message.getReceiver().path().root() != getModel().getSelf().path().root())
        {
            forward(message);
        }
    }

    private void onMessage(MessageExchangeMessages.MessageExchangeMessage message)
    {
        val receiverQueue = getOrCreateMessageQueue(message.getReceiver().path());
        val receiverQueueSize = receiverQueue.enqueueBack(message);
        val totalQueueSize = getModel().getTotalNumberOfEnqueuedMessages().incrementAndGet();

        if (receiverQueueSize == 1)
        {
            dequeueAndSend(receiverQueue);
        }

        if (receiverQueueSize < getModel().getSingleQueueBackPressureThreshold() &&
            totalQueueSize < getModel().getTotalQueueBackPressureThreshold())
        {
            return;
        }

        applyBackPressure(message.getSender());
    }

    private ActorMessageQueue getOrCreateMessageQueue(ActorPath receiverPath)
    {
        var queue = getModel().getMessageQueues().get(receiverPath);
        if (queue == null)
        {
            queue = new ActorMessageQueue();
            getModel().getMessageQueues().put(receiverPath, queue);
        }

        return queue;
    }

    private void dequeueAndSend(ActorMessageQueue queue)
    {
        if (queue == null)
        {
            return;
        }

        val message = queue.dequeue();
        if (message == null)
        {
            return;
        }

        getModel().getTotalNumberOfEnqueuedMessages().decrement();
        forward(message);
    }

    private void forward(MessageExchangeMessages.MessageExchangeMessage message)
    {
        if (message.getReceiver().path().root() == getModel().getSelf().path().root())
        {
            message.getReceiver().tell(message, message.getSender());
        }
        else
        {
            getModel().getMessageDispatcher().tell(message, message.getSender());
        }
    }

    private void applyBackPressure(ActorRef receiver)
    {
        if (receiver.path().root() != getModel().getSelf().path().root())
        {
            // apply back pressure only to local actors
            return;
        }

        val queue = getOrCreateMessageQueue(receiver.path());
        val message = MessageExchangeMessages.BackPressureMessage.builder()
                                                                 .sender(getModel().getSelf())
                                                                 .receiver(receiver)
                                                                 .build();
        val queueSize = queue.enqueueFront(message);
        getModel().getTotalNumberOfEnqueuedMessages().increment();

        if (queueSize == 1)
        {
            dequeueAndSend(queue);
        }
    }
}
