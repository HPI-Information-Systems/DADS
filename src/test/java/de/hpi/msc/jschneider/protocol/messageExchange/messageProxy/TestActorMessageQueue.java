package de.hpi.msc.jschneider.protocol.messageExchange.messageProxy;

import de.hpi.msc.jschneider.protocol.TestMessage;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import junit.framework.TestCase;
import lombok.val;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class TestActorMessageQueue extends TestCase
{
    private MessageExchangeMessages.MessageExchangeMessage enqueueMessage(ActorMessageQueue queue, Function<MessageExchangeMessages.MessageExchangeMessage, Integer> enqueueFunction)
    {
        val expectedSize = queue.size() + 1;
        val expectedUnacknowledgedMessages = queue.numberOfUncompletedMessages();
        val message = TestMessage.empty();

        assertThat(enqueueFunction.apply(message)).isEqualTo(expectedSize);
        assertThat(queue.size()).isEqualTo(expectedSize);
        assertThat(queue.numberOfUncompletedMessages()).isEqualTo(expectedUnacknowledgedMessages);

        return message;
    }

    private MessageExchangeMessages.MessageExchangeMessage dequeueMessage(ActorMessageQueue queue, MessageExchangeMessages.MessageExchangeMessage expectedMessage)
    {
        val expectedSize = queue.size();
        val expectedUnacknowledgedMessages = queue.numberOfUncompletedMessages() < queue.size()
                                             ? queue.numberOfUncompletedMessages() + 1
                                             : queue.numberOfUncompletedMessages();

        val message = queue.dequeue();

        assertThat(queue.size()).isEqualTo(expectedSize);
        assertThat(queue.numberOfUncompletedMessages()).isEqualTo(expectedUnacknowledgedMessages);
        assertThat(message).isEqualTo(expectedMessage);

        return message;
    }

    public void testEnqueueBack()
    {
        val queue = new ActorMessageQueue();
        val firstMessage = enqueueMessage(queue, queue::enqueueBack);
        val secondMessage = enqueueMessage(queue, queue::enqueueBack);

        dequeueMessage(queue, firstMessage);
    }

    public void testEnqueueFront()
    {
        val queue = new ActorMessageQueue();
        val firstMessage = enqueueMessage(queue, queue::enqueueFront);
        val secondMessage = enqueueMessage(queue, queue::enqueueFront);

        dequeueMessage(queue, secondMessage);
    }

    public void testAcknowledgeMessage()
    {
        val queue = new ActorMessageQueue();
        val message = enqueueMessage(queue, queue::enqueueBack);
        dequeueMessage(queue, message);

        assertThat(queue.tryAcknowledge(message.getId())).isTrue();
        assertThat(queue.size()).isEqualTo(0);
        assertThat(queue.numberOfUncompletedMessages()).isEqualTo(0);
    }
}
