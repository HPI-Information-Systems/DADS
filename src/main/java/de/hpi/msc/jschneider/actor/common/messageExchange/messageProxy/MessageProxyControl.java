package de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import de.hpi.msc.jschneider.actor.common.AbstractActorControl;
import de.hpi.msc.jschneider.actor.common.Message;
import lombok.val;
import lombok.var;

public class MessageProxyControl extends AbstractActorControl<MessageProxyModel>
{
    public MessageProxyControl(MessageProxyModel model)
    {
        super(model);
    }

    public void onMessage(Message message)
    {
        val receiverMessageQueue = getOrCreateMessageQueue(message.getReceiver());
        val receiverMessageQueueSize = receiverMessageQueue.enqueueBack(message);
        val totalQueueSize = incrementTotalNumberOfEnqueuedMessages();

        if (receiverMessageQueueSize == 1)
        {
            sendMessage(receiverMessageQueue);
        }

        if (message.getSender().path().root() != getSelf().path().root())
        {
            // sender is not a local actor, so we can not apply back pressure
            return;
        }

        if (receiverMessageQueueSize < getModel().getSingleQueueBackPressureThreshold() &&
            totalQueueSize < getModel().getTotalQueueBackPressureThreshold())
        {
            return;
        }

        val senderMessageQueue = getOrCreateMessageQueue(message.getSender());
        val senderMessageQueueSize = senderMessageQueue.enqueueFront(MessageProxyMessages.BackPressureMessage.builder()
                                                                                                             .sender(getSelf())
                                                                                                             .receiver(message.getSender())
                                                                                                             .build());

        if (senderMessageQueueSize == 1)
        {
            sendMessage(senderMessageQueue);
        }
    }

    public void onMessageCompleted(MessageProxyMessages.MessageCompletedMessage message)
    {
        if (message.getReceiver().path().root() != getSelf().path().root())
        {
            send(message);
            return;
        }

        val messageQueue = getModel().getMessageQueues().get(message.getSender().path());
        if (messageQueue == null) // did we send a message (that could be acknowledged) to that actor earlier?
        {
            getLog().warn("Unexpected message completion from an actor we have never seen before!");
            return;
        }

        if (!messageQueue.tryAcknowledge(message.getAcknowledgedMessageId())) // does the sender acknowledge a message we sent earlier?
        {
            getLog().warn("Unexpected message completion for a message we have never sent!");
            return;
        }

        // the sender acknowledged on of the messages we have sent, that means we can send the next message now
        decrementTotalNumberOfEnqueuedMessages();
        sendMessage(messageQueue);
    }

    private ActorMessageQueue getOrCreateMessageQueue(ActorRef actor)
    {
        return getOrCreateMessageQueue(actor.path());
    }

    private ActorMessageQueue getOrCreateMessageQueue(ActorPath actorPath)
    {
        var queue = getModel().getMessageQueues().get(actorPath);
        if (queue == null)
        {
            queue = new ActorMessageQueue();
            getModel().getMessageQueues().put(actorPath, queue);
        }

        return queue;
    }

    private long incrementTotalNumberOfEnqueuedMessages()
    {
        val numberOfTotalEnqueuedMessages = getModel().getTotalNumberOfEnqueuedMessages().get();
        getModel().getTotalNumberOfEnqueuedMessages().set(numberOfTotalEnqueuedMessages + 1);

        return numberOfTotalEnqueuedMessages + 1;
    }

    private long decrementTotalNumberOfEnqueuedMessages()
    {
        val numberOfTotalEnqueuedMessages = getModel().getTotalNumberOfEnqueuedMessages().get();
        getModel().getTotalNumberOfEnqueuedMessages().set(numberOfTotalEnqueuedMessages - 1);

        return numberOfTotalEnqueuedMessages - 1;
    }

    private void sendMessage(ActorMessageQueue messageQueue)
    {
        val message = messageQueue.dequeue();
        if (message == null)
        {
            return;
        }

        send(message);
    }

    @Override
    public void send(Message message)
    {
        if (message.getReceiver().path().root() == getSelf().path().root())
        {
            message.getReceiver().tell(message, message.getSender());
        }
        else
        {
            getModel().getRemoteMessageDispatcher().tell(message, message.getSender());
        }
    }
}
