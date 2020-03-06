package de.hpi.msc.jschneider.protocol.dimensionReduction;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

import java.time.LocalDateTime;

public class DimensionReductionEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ReducedProjectionCreatedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -5115390658605851185L;
        private MatrixStore<Double> reducedProjection;
        private long firstSubSequenceIndex;
        private boolean isLastSubSequenceChunk;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .reducedProjection(getReducedProjection())
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .isLastSubSequenceChunk(isLastSubSequenceChunk())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class DimensionReductionCompletedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -4447700778495669999L;
        @NonNull
        private LocalDateTime startTime;
        @NonNull
        private LocalDateTime endTime;

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
}
