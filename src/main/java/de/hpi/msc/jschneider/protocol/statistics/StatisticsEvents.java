package de.hpi.msc.jschneider.protocol.statistics;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

public class StatisticsEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class DataTransferCompletedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 4803800742224247453L;
        @NonNull
        private ProcessorId processor;
        @NonNull
        private Class<? extends DataTransferMessages.InitializeDataTransferMessage> initializationMessageType;
        @NonNull
        private ProcessorId source;
        @NonNull
        private ProcessorId sink;
        @NonNull
        private LocalDateTime startTime;
        @NonNull
        private LocalDateTime endTime;
        private long transferredBytes;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .processor(getProcessor())
                            .initializationMessageType(getInitializationMessageType())
                            .source(getSource())
                            .sink(getSink())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .transferredBytes(getTransferredBytes())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class UtilizationEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 8203555156785547843L;
        @NonNull
        private LocalDateTime dateTime;
        private long maximumMemoryInBytes;
        private long usedMemoryInBytes;
        private double cpuUtilization;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .dateTime(getDateTime())
                            .maximumMemoryInBytes(getMaximumMemoryInBytes())
                            .usedMemoryInBytes(getUsedMemoryInBytes())
                            .cpuUtilization(getCpuUtilization())
                            .build();
        }
    }
}
