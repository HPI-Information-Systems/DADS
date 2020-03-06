package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator;

import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerControl;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import lombok.val;
import lombok.var;

import java.util.Arrays;

public class MorePointsThanSamplesCalculator implements WorkConsumer
{
    @Override
    public void process(ActorPoolWorkerControl control, ActorPoolMessages.WorkMessage workLoad)
    {
        assert workLoad instanceof DensityCalculatorMessages.EvaluateDensityProbabilitiesMessage : "Unexpected WorkLoad!";
        process(control, (DensityCalculatorMessages.EvaluateDensityProbabilitiesMessage) workLoad);
    }

    private void process(ActorPoolWorkerControl control, DensityCalculatorMessages.EvaluateDensityProbabilitiesMessage message)
    {
        val results = new double[message.getPointsToEvaluate().length];

        val startIndex = (int) Math.floor(message.getStartFraction() * message.getSamples().length);
        val endIndex = (int) Math.floor(message.getEndFraction() * message.getSamples().length);

        for (var i = startIndex; i < endIndex; ++i)
        {
            val samplesIndex = i;
            val temp = Arrays.stream(message.getPointsToEvaluate())
                             .map(point -> message.getSamples()[samplesIndex] - point)
                             .map(diff -> (diff * diff) * 0.5d)
                             .map(energy -> Math.exp(-energy) * message.getWeight())
                             .toArray();

            for (var resultsIndex = 0; resultsIndex < results.length; ++resultsIndex)
            {
                results[resultsIndex] += temp[resultsIndex];
            }
        }

        control.send(DensityCalculatorMessages.DensityProbabilitiesEstimatedMessage.builder()
                                                                                   .sender(control.getModel().getSelf())
                                                                                   .receiver(message.getSender())
                                                                                   .probabilities(results)
                                                                                   .startIndex(startIndex)
                                                                                   .build());
    }
}
