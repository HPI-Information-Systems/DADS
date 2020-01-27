package de.hpi.msc.jschneider.protocol.principalComponentAnalysis;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

public class PCAMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializePCACalculationMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 7959217715683067548L;
        private Map<Long, ProcessorId> processorIndices;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .processorIndices(new HashMap<>(getProcessorIndices()))
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeColumnMeansTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = -8531945015558247291L;
        private long processorIndex;
        private long numberOfRows;
        private float minimumRecord;
        private float maximumRecord;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .processorIndex(getProcessorIndex())
                            .numberOfRows(getNumberOfRows())
                            .minimumRecord(getMinimumRecord())
                            .maximumRecord(getMaximumRecord())
                            .operationId(getOperationId())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeRTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = -5113830341484647213L;
        private long processorIndex;
        private long currentStepNumber;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .operationId(getOperationId())
                            .processorIndex(getProcessorIndex())
                            .currentStepNumber(getCurrentStepNumber())
                            .build();
        }
    }
}
