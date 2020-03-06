package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator;

import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerControl;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import lombok.val;
import lombok.var;

import java.util.Arrays;

public class LessPointsThanSamplesCalculator implements WorkConsumer
{
    @Override
    public void process(ActorPoolWorkerControl control, ActorPoolMessages.WorkMessage workLoad)
    {
        assert workLoad instanceof DensityCalculatorMessages.EvaluateDensityProbabilitiesMessage : "Unexpected WorkLoad!";
        process(control, (DensityCalculatorMessages.EvaluateDensityProbabilitiesMessage) workLoad);
    }

    private void process(ActorPoolWorkerControl control, DensityCalculatorMessages.EvaluateDensityProbabilitiesMessage message)
    {
        val startIndex = (int) Math.floor(message.getStartFraction() * message.getPointsToEvaluate().length);
        val endIndex = (int) Math.floor(message.getEndFraction() * message.getPointsToEvaluate().length);

        val results = new double[endIndex - startIndex];

        for (var resultsIndex = startIndex; resultsIndex < endIndex; ++resultsIndex)
        {
            val scaledPointsIndex = resultsIndex;
            results[resultsIndex - startIndex] = Arrays.stream(message.getSamples())
                                                       .map(sample -> sample - message.getPointsToEvaluate()[scaledPointsIndex])
                                                       .map(diff -> (diff * diff) * 0.5d)
                                                       .map(energy -> Math.exp(-energy) * message.getWeight())
                                                       .sum();
        }

        control.send(DensityCalculatorMessages.DensityProbabilitiesEstimatedMessage.builder()
                                                                                   .sender(control.getModel().getSelf())
                                                                                   .receiver(message.getSender())
                                                                                   .probabilities(results)
                                                                                   .startIndex(startIndex)
                                                                                   .build());
    }
}
