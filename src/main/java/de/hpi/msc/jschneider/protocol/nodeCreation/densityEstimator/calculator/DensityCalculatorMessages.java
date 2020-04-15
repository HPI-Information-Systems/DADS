package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

public class DensityCalculatorMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class EvaluateDensityProbabilitiesMessage extends ActorPoolMessages.WorkMessage
    {
        private static final long serialVersionUID = -5411554227609332575L;
        @NonNull
        private DoubleBigList samples;
        @NonNull
        private double[] pointsToEvaluate;
        private double weight;
        private double whitening;
        private double startFraction;
        private double endFraction;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .consumer(getConsumer())
                            .samples(getSamples())
                            .pointsToEvaluate(getPointsToEvaluate())
                            .weight(getWeight())
                            .whitening(getWhitening())
                            .startFraction(getStartFraction())
                            .endFraction(getEndFraction())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class DensityProbabilitiesEstimatedMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -8528585974013084112L;
        @NonNull
        private double[] probabilities;
        private int startIndex;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class NodeCollectionCreatedMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -5466152489823604099L;
        private int intersectionSegment;
        @NonNull
        private NodeCollection nodeCollection;
    }
}
