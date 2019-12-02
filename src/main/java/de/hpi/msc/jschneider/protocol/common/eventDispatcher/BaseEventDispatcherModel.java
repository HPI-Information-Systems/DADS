package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.val;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuperBuilder
public class BaseEventDispatcherModel extends AbstractProtocolParticipantModel implements EventDispatcherModel
{
    @NonNull @Getter @Setter @Builder.Default
    private Map<Class<?>, Set<ActorRef>> eventSubscribers = new HashMap<>();

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
