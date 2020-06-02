package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkFactory;
import de.hpi.msc.jschneider.utility.Int64Range;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigList;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

import java.util.concurrent.Callable;

public class DensityWorkFactory implements WorkFactory
{
    private final ActorRef supervisor;
    private final DoubleBigList samples;
    private final DoubleBigList pointsToEvaluate;
    private final double weight;
    private final double whitening;
    private final Callable<WorkConsumer> workConsumerFactory;
    private final ObjectBigList<Int64Range> calculationRanges;

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

        calculationRanges = pointsToEvaluate.size64() >= samples.size64()
                            ? createCalculationRanges(samples.size64())
                            : createCalculationRanges(pointsToEvaluate.size64());
    }

    private ObjectBigList<Int64Range> createCalculationRanges(long maximum)
    {
        val desiredNumberOfChunks = SystemParameters.getNumberOfWorkers() * 100L;
        var chunkSize = (long) Math.max(1L, maximum / (double) desiredNumberOfChunks);
        val numberOfCalculations = (long) Math.ceil(maximum / (double) chunkSize);
        chunkSize = (long) Math.floor(maximum / (double) numberOfCalculations);
        val calculationRanges = new ObjectBigArrayBigList<Int64Range>(numberOfCalculations);
        var nextChunkStart = 0L;

        for (var i = 0L; i < numberOfCalculations; ++i)
        {
            var nextChunkEnd = Math.max(nextChunkStart + 1L, nextChunkStart + chunkSize);
            if (i == numberOfCalculations - 1)
            {
                // last chunk takes all the rest
                nextChunkEnd = maximum;
            }

            calculationRanges.add(Int64Range.builder()
                                            .from(nextChunkStart)
                                            .to(nextChunkEnd)
                                            .build());
            nextChunkStart = nextChunkEnd;
        }

        return calculationRanges;
    }

    public long expectedNumberOfCalculations()
    {
        return calculationRanges.size64();
    }

    @Override
    public boolean hasNext()
    {
        return !calculationRanges.isEmpty();
    }

    @SneakyThrows
    @Override
    public ActorPoolMessages.WorkMessage next(ActorRef worker)
    {
        return DensityCalculatorMessages.EvaluateDensityProbabilitiesMessage.builder()
                                                                            .sender(supervisor)
                                                                            .receiver(worker)
                                                                            .consumer(workConsumerFactory.call())
                                                                            .samples(samples)
                                                                            .pointsToEvaluate(pointsToEvaluate)
                                                                            .weight(weight)
                                                                            .whitening(whitening)
                                                                            .calculationRange(calculationRanges.remove(0L))
                                                                            .build();
    }
}
