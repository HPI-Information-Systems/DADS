package de.hpi.msc.jschneider.protocol.nodeCreation.worker.intersectionCalculator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

import java.util.List;

public class IntersectionCalculatorMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class CalculateIntersectionsMessage extends ActorPoolMessages.WorkMessage
    {
        private static final long serialVersionUID = -4528791212461532270L;
        @NonNull
        private MatrixStore<Double> projection;
        private long chunkStart;
        private long chunkLength;
        @NonNull
        private List<MatrixStore<Double>> intersectionPoints;
        @NonNull
        private long firstSubSequenceIndex;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .consumer(getConsumer())
                            .projection(getProjection())
                            .chunkStart(getChunkStart())
                            .chunkLength(getChunkLength())
                            .intersectionPoints(getIntersectionPoints())
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class IntersectionsCalculatedMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -6115466045930415882L;
        @NonNull
        private IntersectionCollection[] intersectionCollections;
    }
}
