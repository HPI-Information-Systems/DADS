package de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy;

import de.hpi.msc.jschneider.actor.common.Message;
import lombok.val;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ActorMessageQueue
{
    private final List<Message> queuedMessages = new ArrayList<>();
    private final Map<UUID, Message> uncompletedMessages = new HashMap<>();

    public int size()
    {
        return queuedMessages.size() + numberOfUncompletedMessages();
    }

    public int numberOfUncompletedMessages()
    {
        return uncompletedMessages.size();
    }

    public int enqueueFront(Message message)
    {
        queuedMessages.add(0, message);
        return size();
    }

    public int enqueueBack(Message message)
    {
        queuedMessages.add(message);
        return size();
    }

    public Message dequeue()
    {
        if (queuedMessages.size() < 1)
        {
            return null;
        }

        val message = queuedMessages.remove(0);
        uncompletedMessages.put(message.getId(), message);

        return message;
    }

    public boolean tryAcknowledge(UUID messageId)
    {
        val message = uncompletedMessages.remove(messageId);
        return message != null;
    }
}
