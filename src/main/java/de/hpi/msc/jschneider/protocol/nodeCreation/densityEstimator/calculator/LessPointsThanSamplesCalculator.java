package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator;

import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerControl;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import lombok.val;
import lombok.var;

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
        val startIndex = message.getCalculationRange().getFrom();
        val endIndex = message.getCalculationRange().getTo();

        val results = new DoubleBigArrayBigList(endIndex - startIndex);

        for (var resultsIndex = startIndex; resultsIndex < endIndex; ++resultsIndex)
        {
            val pointToEvaluate = message.getPointsToEvaluate().getDouble(resultsIndex) * message.getWhitening();

            results.add(message.getSamples().stream()
                               .mapToDouble(value -> value)
                               .map(sample -> sample * message.getWhitening() - pointToEvaluate)
                               .map(diff -> diff * diff * 0.5d)
                               .map(energy -> Math.exp(-energy) * message.getWeight())
                               .sum());
        }

        control.send(DensityCalculatorMessages.DensityProbabilitiesEstimatedMessage.builder()
                                                                                   .sender(control.getModel().getSelf())
                                                                                   .receiver(message.getSender())
                                                                                   .probabilities(results)
                                                                                   .startIndex(startIndex)
                                                                                   .build());
    }
}
