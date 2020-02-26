package de.hpi.msc.jschneider.protocol.actorPool;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

public class ActorPoolEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class UtilizationEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 6505248302544304168L;
        @NonNull
        private LocalDateTime dateTime;
        private int numberOfWorkers;
        private int numberOfAvailableWorkers;
        private int workQueueSize;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .dateTime(getDateTime())
                            .numberOfWorkers(getNumberOfWorkers())
                            .numberOfAvailableWorkers(getNumberOfAvailableWorkers())
                            .workQueueSize(getWorkQueueSize())
                            .build();
        }
    }
}
