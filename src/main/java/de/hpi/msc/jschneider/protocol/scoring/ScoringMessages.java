package de.hpi.msc.jschneider.protocol.scoring;

import akka.actor.ActorRef;
import com.esotericsoftware.kryo.NotNull;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.List;

public class ScoringMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ScoringParametersMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 1117346665010797573L;
        private int queryPathLength;
        private int subSequenceLength;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .queryPathLength(getQueryPathLength())
                            .subSequenceLength(getSubSequenceLength())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class OverlappingEdgeCreationOrderMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -5955687701293096759L;
        @NotNull
        private int[][] overlappingEdgeCreationOrder;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .overlappingEdgeCreationOrder(getOverlappingEdgeCreationOrder())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class OverlappingPathScoresMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 3583198169495096939L;
        @NonNull
        private List<Double> overlappingPathScores;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .overlappingPathScores(getOverlappingPathScores())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class MinimumAndMaximumScoreMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -6601476448525010953L;
        private double minimumScore;
        private double maximumScore;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .minimumScore(getMinimumScore())
                            .maximumScore(getMaximumScore())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializePathScoresTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = 6054549294608329361L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .operationId(getOperationId())
                            .build();
        }
    }
}
