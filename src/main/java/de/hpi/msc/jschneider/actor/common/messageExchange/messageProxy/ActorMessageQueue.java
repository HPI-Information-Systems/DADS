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
    private final Map<UUID, Message> unacknowledgedMessages = new HashMap<>();

    public int size()
    {
        return queuedMessages.size() + numberOfUnacknowledgedMessages();
    }

    public int numberOfUnacknowledgedMessages()
    {
        return unacknowledgedMessages.size();
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
        unacknowledgedMessages.put(message.getId(), message);

        return message;
    }

    public boolean tryAcknowledge(UUID messageId)
    {
        val message = unacknowledgedMessages.remove(messageId);

        return message != null;
    }
}
