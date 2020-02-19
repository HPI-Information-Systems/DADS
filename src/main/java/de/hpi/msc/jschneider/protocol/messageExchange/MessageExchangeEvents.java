package de.hpi.msc.jschneider.protocol.messageExchange;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

public class MessageExchangeEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class UtilizationEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 1251225437281827998L;
        @NonNull
        private LocalDateTime dateTime;
        @NonNull
        private ProcessorId remoteProcessor;
        private long totalNumberOfEnqueuedMessages;
        private long totalNumberOfUnacknowledgedMessages;
        private long largestMessageQueueSize;
        @NonNull
        private ActorPath largestMessageQueueReceiver;
        private double averageMessageQueueSize;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .dateTime(getDateTime())
                            .remoteProcessor(getRemoteProcessor())
                            .totalNumberOfEnqueuedMessages(getTotalNumberOfEnqueuedMessages())
                            .totalNumberOfUnacknowledgedMessages(getTotalNumberOfUnacknowledgedMessages())
                            .largestMessageQueueSize(getLargestMessageQueueSize())
                            .largestMessageQueueReceiver(getLargestMessageQueueReceiver())
                            .averageMessageQueueSize(getAverageMessageQueueSize())
                            .build();
        }
    }
}
