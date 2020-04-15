package de.hpi.msc.jschneider.protocol.nodeCreation.worker.intersectionCalculator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkFactory;
import de.hpi.msc.jschneider.utility.Counter;
import lombok.Getter;
import lombok.val;
import lombok.var;
import org.ojalgo.function.aggregator.Aggregator;
import org.ojalgo.matrix.store.MatrixStore;

import java.util.List;

public class IntersectionWorkFactory implements WorkFactory
{
    private static final int CHUNK_SIZE = 10000;

    private final ActorRef supervisor;
    private final MatrixStore<Double> projection;
    private final int numberOfIntersectionSegments;
    private final Counter nextChunkStartIndex = new Counter(0L);
    private final Counter nextChunkFirstSubSequenceIndex;
    private final List<MatrixStore<Double>> intersectionPoints;
    @Getter
    private final int numberOfIntersectionChunks;
    private int producedChunks = 0;

    public IntersectionWorkFactory(ActorRef supervisor, MatrixStore<Double> projection, int numberOfIntersectionSegments, long firstSubSequenceIndex)
    {
        this.supervisor = supervisor;
        this.projection = projection;
        this.numberOfIntersectionSegments = numberOfIntersectionSegments;
        nextChunkFirstSubSequenceIndex = new Counter(firstSubSequenceIndex);
        intersectionPoints = createIntersectionPoints();
        numberOfIntersectionChunks = (int) Math.max(1, Math.floor(projection.countColumns() / (double) CHUNK_SIZE));
    }

    private List<MatrixStore<Double>> createIntersectionPoints()
    {
        val radiusX = Math.max(projection.aggregateRow(0L, Aggregator.MAXIMUM), Math.abs(projection.aggregateRow(0L, Aggregator.MINIMUM)));
        val radiusY = Math.max(projection.aggregateRow(1L, Aggregator.MAXIMUM), Math.abs(projection.aggregateRow(1L, Aggregator.MINIMUM)));
        val radiusLength = Math.sqrt(radiusX * radiusX + radiusY * radiusY);
        return Calculate.makeIntersectionPoints(radiusLength, numberOfIntersectionSegments);
    }

    @Override
    public boolean hasNext()
    {
        return producedChunks < numberOfIntersectionChunks;
    }

    @Override
    public ActorPoolMessages.WorkMessage next(ActorRef worker)
    {
        val start = nextChunkStartIndex.get();
        var end = Math.min(projection.countColumns(), start + CHUNK_SIZE);
        if (isLastChunk())
        {
            end = projection.countColumns();
        }

        val chunkLength = end - start;
        val message = IntersectionCalculatorMessages.CalculateIntersectionsMessage.builder()
                                                                                  .sender(supervisor)
                                                                                  .receiver(worker)
                                                                                  .consumer(new IntersectionCalculator())
                                                                                  .projection(projection)
                                                                                  .chunkStart(start)
                                                                                  .chunkLength(chunkLength)
                                                                                  .intersectionPoints(intersectionPoints)
                                                                                  .firstSubSequenceIndex(nextChunkFirstSubSequenceIndex.get())
                                                                                  .build();

        nextChunkStartIndex.increment(chunkLength - 1);
        nextChunkFirstSubSequenceIndex.increment(chunkLength - 1);
        producedChunks++;

        return message;
    }

    private boolean isLastChunk()
    {
        return producedChunks == numberOfIntersectionChunks - 1;
    }
}
