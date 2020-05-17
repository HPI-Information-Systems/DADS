package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator;

import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerControl;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import lombok.val;
import lombok.var;

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
        val results = new DoubleBigArrayBigList(message.getPointsToEvaluate().size64());
        for (var i = 0; i < message.getPointsToEvaluate().size64(); ++i)
        {
            results.add(0.0d);
        }

        val startIndex = message.getCalculationRange().getFrom();
        val endIndex = message.getCalculationRange().getTo();

        for (var i = startIndex; i < endIndex; ++i)
        {
            val sample = message.getSamples().getDouble(i) * message.getWhitening();
            val temp = message.getPointsToEvaluate().stream()
                              .map(point -> sample - point * message.getWhitening())
                              .map(diff -> (diff * diff) * 0.5d)
                              .map(energy -> Math.exp(-energy) * message.getWeight())
                              .iterator();

            for (var resultsIndex = 0L; resultsIndex < results.size64(); ++resultsIndex)
            {
                results.set(resultsIndex, results.getDouble(resultsIndex) + temp.next());
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
