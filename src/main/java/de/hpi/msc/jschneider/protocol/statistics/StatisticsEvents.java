package de.hpi.msc.jschneider.protocol.statistics;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

public class StatisticsEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class DataTransferCompletedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 4803800742224247453L;
        private Class<? extends DataTransferMessages.InitializeDataTransferMessage> initializationMessageType;
        private ProcessorId source;
        private ProcessorId sink;
        private LocalDateTime startTime;
        private LocalDateTime endTime;
        private long transferredBytes;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .initializationMessageType(getInitializationMessageType())
                            .source(getSource())
                            .sink(getSink())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .transferredBytes(getTransferredBytes())
                            .build();
        }
    }
}
