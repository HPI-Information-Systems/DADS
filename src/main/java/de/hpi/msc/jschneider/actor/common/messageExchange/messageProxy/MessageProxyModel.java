package de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import de.hpi.msc.jschneider.actor.common.AbstractActorModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.agrona.collections.MutableLong;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class MessageProxyModel extends AbstractActorModel
{
    @NonNull @Getter
    private ActorRef remoteMessageDispatcher;
    @NonNull @Getter
    private final MutableLong totalNumberOfEnqueuedMessages = new MutableLong();
    @NonNull @Getter
    private final Map<ActorPath, ActorMessageQueue> messageQueues = new HashMap<>();
    @Getter
    private int singleQueueBackPressureThreshold = 100;
    @Getter
    private int totalQueueBackPressureThreshold = 10000;
}
