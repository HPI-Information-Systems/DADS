package de.hpi.msc.jschneider.protocol.messageExchange.messageProxy;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.val;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ActorMessageQueue
{
    private final List<MessageExchangeMessages.MessageExchangeMessage> queuedMessages = new ArrayList<>();
    private final Map<UUID, MessageExchangeMessages.MessageExchangeMessage> uncompletedMessages = new HashMap<>();

    public int size()
    {
        return queuedMessages.size() + numberOfUncompletedMessages();
    }

    public int numberOfUncompletedMessages()
    {
        return uncompletedMessages.size();
    }

    public int enqueueFront(MessageExchangeMessages.MessageExchangeMessage message)
    {
        queuedMessages.add(0, message);
        return size();
    }

    public int enqueueBack(MessageExchangeMessages.MessageExchangeMessage message)
    {
        queuedMessages.add(message);
        return size();
    }

    public MessageExchangeMessages.MessageExchangeMessage dequeue()
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
