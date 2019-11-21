package de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy;

import de.hpi.msc.jschneider.actor.common.Message;
import de.hpi.msc.jschneider.actor.common.MockMessage;
import junit.framework.TestCase;
import lombok.val;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class TestActorMessageQueue extends TestCase
{
    private MockMessage dummyMessage()
    {
        return MockMessage.builder().build();
    }

    private MockMessage enqueueMessage(ActorMessageQueue queue, Function<Message, Integer> enqueueFunction)
    {
        val expectedSize = queue.size() + 1;
        val expectedUnacknowledgedMessages = queue.numberOfUnacknowledgedMessages();
        val message = dummyMessage();

        assertThat(enqueueFunction.apply(message)).isEqualTo(expectedSize);
        assertThat(queue.size()).isEqualTo(expectedSize);
        assertThat(queue.numberOfUnacknowledgedMessages()).isEqualTo(expectedUnacknowledgedMessages);

        return message;
    }

    private Message dequeueMessage(ActorMessageQueue queue, Message expectedMessage)
    {
        val expectedSize = queue.size();
        val expectedUnacknowledgedMessages = queue.numberOfUnacknowledgedMessages() < queue.size()
                                             ? queue.numberOfUnacknowledgedMessages() + 1
                                             : queue.numberOfUnacknowledgedMessages();

        val message = queue.dequeue();

        assertThat(queue.size()).isEqualTo(expectedSize);
        assertThat(queue.numberOfUnacknowledgedMessages()).isEqualTo(expectedUnacknowledgedMessages);
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
        assertThat(queue.numberOfUnacknowledgedMessages()).isEqualTo(0);
    }
}
