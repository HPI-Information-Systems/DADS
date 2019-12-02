package de.hpi.msc.jschneider.protocol.messageExchange.messageProxy;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.agrona.collections.MutableLong;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class MessageProxyModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final MutableLong totalNumberOfEnqueuedMessages = new MutableLong();
    @NonNull @Getter
    private final Map<ActorPath, ActorMessageQueue> messageQueues = new HashMap<>();
    @NonNull @Getter
    private ActorRef messageDispatcher;
    @Getter @Setter @Builder.Default
    private int singleQueueBackPressureThreshold = 100;
    @Getter @Setter @Builder.Default
    private int totalQueueBackPressureThreshold = 10000;
}
