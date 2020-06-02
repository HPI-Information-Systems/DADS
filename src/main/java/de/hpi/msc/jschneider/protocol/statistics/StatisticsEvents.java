package de.hpi.msc.jschneider.protocol.statistics;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.val;

import java.time.Duration;
import java.time.LocalDateTime;

public class StatisticsEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class StatisticsEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -6627947611026621248L;

        @Override
        public String toString()
        {
            return String.format("Unspecified Statistics Event! (Missing implementation of \"toString\" in %1$s)", getClass().getName());
        }

        protected StatisticsEvent startStringCreation(StringBuilder sb, String eventName)
        {
            sb.append(eventName)
              .append(" { ");

            return this.appendProperty(sb, "Processor", ProcessorId.of(getSender()));
        }

        protected final StatisticsEvent appendProperty(StringBuilder sb, String key, LocalDateTime value)
        {
            return appendProperty(sb, key, value.format(StatisticsProtocol.DATE_FORMAT));
        }

        protected final StatisticsEvent appendProperty(StringBuilder sb, String key, Object value)
        {
            sb.append(key)
              .append(" = ")
              .append(value)
              .append("; ");

            return this;
        }

        protected final String createString(StringBuilder sb)
        {
            return sb.append("}")
                     .toString();
        }
    }

    // region Duration Events

    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class DurationEvent extends StatisticsEvent
    {
        private static final long serialVersionUID = -6610806357256119923L;
        @NonNull
        private LocalDateTime startTime;
        @NonNull
        private LocalDateTime endTime;

        @Override
        public String toString()
        {
            val simpleClassName = getClass().getSimpleName();
            return toSimpleString(simpleClassName.substring(0, simpleClassName.length() - "Event".length()));
        }

        protected final String toSimpleString(String eventName)
        {
            val sb = new StringBuilder();
            return this.startStringCreation(sb, eventName)
                       .createString(sb);
        }

        @Override
        protected StatisticsEvent startStringCreation(StringBuilder sb, String eventName)
        {
            return super.startStringCreation(sb, eventName)
                        .appendProperty(sb, "StartTime", getStartTime())
                        .appendProperty(sb, "EndTime", getEndTime())
                        .appendProperty(sb, "Duration", Duration.between(getStartTime(), getEndTime()));
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class DataTransferredEvent extends DurationEvent
    {
        private static final long serialVersionUID = 4803800742224247453L;
        @NonNull
        private String initializationMessageClassName;
        @NonNull
        private ProcessorId source;
        @NonNull
        private ProcessorId sink;
        private long transferredBytes;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .initializationMessageClassName(getInitializationMessageClassName())
                            .source(getSource())
                            .sink(getSink())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .transferredBytes(getTransferredBytes())
                            .build();
        }

        @Override
        public String toString()
        {
            val sb = new StringBuilder();
            return this.startStringCreation(sb, "DataTransferred")
                       .appendProperty(sb, "Type", getInitializationMessageClassName())
                       .appendProperty(sb, "Source", getSource())
                       .appendProperty(sb, "Sink", getSink())
                       .appendProperty(sb, "Duration", Duration.between(getStartTime(), getEndTime()))
                       .appendProperty(sb, "Bytes", getTransferredBytes())
                       .createString(sb);
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class ProjectionCreatedEvent extends DurationEvent
    {
        private static final long serialVersionUID = -7506116895891841006L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class PCACreatedEvent extends DurationEvent
    {
        private static final long serialVersionUID = 2594365276313650559L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class DimensionReductionCreatedEvent extends DurationEvent
    {
        private static final long serialVersionUID = -4447700778495669999L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class IntersectionsCreatedEvent extends DurationEvent
    {
        private static final long serialVersionUID = -4391959924831129092L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class NodesExtractedEvent extends DurationEvent
    {
        private static final long serialVersionUID = 570337156660046464L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class EdgePartitionCreatedEvent extends DurationEvent
    {
        private static final long serialVersionUID = -4156964801073580719L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class PathScoresCreatedEvent extends DurationEvent
    {
        private static final long serialVersionUID = 105573551477837831L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class PathScoresNormalizedEvent extends DurationEvent
    {
        private static final long serialVersionUID = 2400245890773520483L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class CalculationCompletedEvent extends DurationEvent
    {
        private static final long serialVersionUID = -2861743088470949911L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class ResultsPersistedEvent extends DurationEvent
    {
        private static final long serialVersionUID = 4513984949550267205L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }

    // endregion

    // region Measurement Events

    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class MeasurementEvent extends StatisticsEvent
    {
        private static final long serialVersionUID = -3979224022816785881L;
        @NonNull
        private LocalDateTime dateTime;

        @Override
        protected StatisticsEvent startStringCreation(StringBuilder sb, String eventName)
        {
            return super.startStringCreation(sb, eventName)
                        .appendProperty(sb, "DateTime", getDateTime());
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class MachineUtilizationEvent extends MeasurementEvent
    {
        private static final long serialVersionUID = 8203555156785547843L;
        private long maximumMemoryInBytes;
        private long usedHeapInBytes;
        private long usedStackInBytes;
        private double cpuUtilization;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .dateTime(getDateTime())
                            .maximumMemoryInBytes(getMaximumMemoryInBytes())
                            .usedHeapInBytes(getUsedHeapInBytes())
                            .usedStackInBytes(getUsedStackInBytes())
                            .cpuUtilization(getCpuUtilization())
                            .build();
        }

        @Override
        public String toString()
        {
            val sb = new StringBuilder();
            return this.startStringCreation(sb, "MachineUtilization")
                       .appendProperty(sb, "MaximumMemory", getMaximumMemoryInBytes())
                       .appendProperty(sb, "UsedHeap", getUsedHeapInBytes())
                       .appendProperty(sb, "UsedStack", getUsedStackInBytes())
                       .appendProperty(sb, "CPULoad", getCpuUtilization())
                       .createString(sb);
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class MessageExchangeUtilizationEvent extends MeasurementEvent
    {
        private static final long serialVersionUID = 1251225437281827998L;
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

        @Override
        public String toString()
        {
            val sb = new StringBuilder();
            return this.startStringCreation(sb, "MessageExchangeUtilization")
                       .appendProperty(sb, "RemoteProcessor", getRemoteProcessor())
                       .appendProperty(sb, "TotalNumberOfEnqueuedMessages", getTotalNumberOfEnqueuedMessages())
                       .appendProperty(sb, "TotalNumberOfUnacknowledgedMessages", getTotalNumberOfUnacknowledgedMessages())
                       .appendProperty(sb, "LargestMessageQueueSize", getLargestMessageQueueSize())
                       .appendProperty(sb, "LargestMessageQueueReceiver", getLargestMessageQueueReceiver())
                       .appendProperty(sb, "AverageNumberOfEnqueuedMessages", getAverageMessageQueueSize())
                       .createString(sb);
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class ActorPoolUtilizationEvent extends MeasurementEvent
    {
        private static final long serialVersionUID = 6505248302544304168L;
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

        @Override
        public String toString()
        {
            val sb = new StringBuilder();
            return this.startStringCreation(sb, "ActorPoolUtilization")
                       .appendProperty(sb, "Workers", getNumberOfWorkers())
                       .appendProperty(sb, "AvailableWorkers", getNumberOfAvailableWorkers())
                       .appendProperty(sb, "QueueSize", getWorkQueueSize())
                       .createString(sb);
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class ActorSystemUtilizationEvent extends MeasurementEvent
    {
        private static final long serialVersionUID = 5218986191293213196L;
        private int numberOfActors;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .dateTime(getDateTime())
                            .numberOfActors(getNumberOfActors())
                            .build();
        }

        @Override
        public String toString()
        {
            val sb = new StringBuilder();
            return this.startStringCreation(sb, "ActorSystemUtilization")
                       .appendProperty(sb, "Actors", getNumberOfActors())
                       .createString(sb);
        }
    }

    // endregion
}
