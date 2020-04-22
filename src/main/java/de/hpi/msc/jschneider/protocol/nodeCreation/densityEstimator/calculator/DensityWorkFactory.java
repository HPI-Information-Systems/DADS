package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkFactory;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import lombok.SneakyThrows;
import lombok.val;

import java.util.concurrent.Callable;

public class DensityWorkFactory implements WorkFactory
{
    private static final double CHUNK_SIZE = 1.0d / 250.0d;

    private final ActorRef supervisor;
    private final DoubleBigList samples;
    private final DoubleBigList pointsToEvaluate;
    private final double weight;
    private final double whitening;
    private final Callable<WorkConsumer> workConsumerFactory;
    private double nextChunkStart = 0.0d;

    public DensityWorkFactory(ActorRef supervisor, DoubleBigList samples, DoubleBigList pointsToEvaluate, double weight, double whitening)
    {
        this.supervisor = supervisor;
        this.samples = samples;
        this.pointsToEvaluate = pointsToEvaluate;
        this.weight = weight;
        this.whitening = whitening;

        workConsumerFactory = pointsToEvaluate.size64() >= samples.size64()
                              ? MorePointsThanSamplesCalculator::new
                              : LessPointsThanSamplesCalculator::new;
    }

    public long expectedNumberOfCalculations()
    {
        return (long) Math.ceil(1.0d / CHUNK_SIZE);
    }

    @Override
    public boolean hasNext()
    {
        return nextChunkStart < 1.0d;
    }

    @SneakyThrows
    @Override
    public ActorPoolMessages.WorkMessage next(ActorRef worker)
    {
        val chunkEndFraction = Math.min(1.0d, nextChunkStart + CHUNK_SIZE);

        val message = DensityCalculatorMessages.EvaluateDensityProbabilitiesMessage.builder()
                                                                                   .sender(supervisor)
                                                                                   .receiver(worker)
                                                                                   .consumer(workConsumerFactory.call())
                                                                                   .samples(samples)
                                                                                   .pointsToEvaluate(pointsToEvaluate)
                                                                                   .weight(weight)
                                                                                   .whitening(whitening)
                                                                                   .startFraction(nextChunkStart)
                                                                                   .endFraction(chunkEndFraction)
                                                                                   .build();

        nextChunkStart += CHUNK_SIZE;
        return message;
    }
}
