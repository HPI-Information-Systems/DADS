package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@SuperBuilder
public class BaseEventDispatcherModel implements EventDispatcherModel
{
    private Logger log;
    @NonNull @Getter @Setter @Builder.Default
    private Map<Class<?>, Set<ActorRef>> eventSubscribers = new HashMap<>();
    @Setter
    private Function<MessageExchangeMessages.MessageExchangeMessage, ActorRef> messageDispatcherProvider;

    protected final Logger getLog()
    {
        if (log == null)
        {
            log = LogManager.getLogger(getClass());
        }

        return log;
    }

    public final ActorRef getMessageDispatcher(MessageExchangeMessages.MessageExchangeMessage message)
    {
        try
        {
            return messageDispatcherProvider.apply(message);
        }
        catch (Exception exception)
        {
            getLog().error("Unable to retrieve MessageDispatcher!", exception);
            return ActorRef.noSender();
        }
    }

    public static EventDispatcherModel create(Class<?>... eventTypes)
    {
        val subscribers = new HashMap<Class<?>, Set<ActorRef>>();
        for (val eventType : eventTypes)
        {
            subscribers.put(eventType, new HashSet<>());
        }

        return builder()
                .eventSubscribers(subscribers)
                .build();
    }
}
