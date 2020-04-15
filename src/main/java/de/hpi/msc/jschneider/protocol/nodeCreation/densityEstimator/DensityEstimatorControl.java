package de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator;

import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator.DensityCalculatorMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.densityEstimator.calculator.DensityWorkFactory;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import lombok.val;
import lombok.var;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

public class DensityEstimatorControl extends AbstractProtocolParticipantControl<DensityEstimatorModel>
{
    public DensityEstimatorControl(DensityEstimatorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(DensityCalculatorMessages.DensityProbabilitiesEstimatedMessage.class, this::onDensityProbabilities);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        val bandwidth = Calculate.scottsFactor(getModel().getSamples().size64(), 1L);
        val dataVariance = variance();
        val squaredBandwidth = Math.pow(bandwidth, 2.0d);
        val covariance = dataVariance * squaredBandwidth;
        val inverseCovariance = (1.0d / dataVariance) / squaredBandwidth;

        getModel().setWhitening(Math.sqrt(inverseCovariance));
        getModel().setNormalizationFactor(Math.sqrt(2 * Math.PI * covariance));
        getModel().setWeight(1.0d / getModel().getSamples().size64());
        getModel().setProbabilities(new double[getModel().getPointsToEvaluate().length]);

        dispatchWork();
    }

    private double variance()
    {
        assert getModel().getSamples().size64() > 1 : "Can not calculate variance with less than 2 samples!";

        var sum = 0.0D;
        var sumsq = 0.0D;
        for (var value : getModel().getSamples())
        {
            sum += value;
            sumsq += value * value;
        }

        var n = getModel().getSamples().size64() - 1;
        return sumsq / (double) n - sum / (double) getModel().getSamples().size64() * (sum / (double) n);
    }

    private void dispatchWork()
    {
        val actorPool = getLocalProtocol(ProtocolType.ActorPool);
        assert actorPool.isPresent() : "ActorPooling is not supported!";

        val workFactory = new DensityWorkFactory(getModel().getSelf(),
                                                 getModel().getSamples(),
                                                 getModel().getPointsToEvaluate(),
                                                 getModel().getWeight(),
                                                 getModel().getWhitening());
        getModel().setExpectedNumberOfResults(workFactory.expectedNumberOfCalculations());
        getModel().setResultChunkMerger(getModel().getPointsToEvaluate().length >= getModel().getSamples().size64()
                                        ? this::mergeMorePointsThanSamplesResultChunks
                                        : this::mergeLessPointsThanSamplesResultChunks);

        send(ActorPoolMessages.ExecuteDistributedFromFactoryMessage.builder()
                                                                   .sender(getModel().getSelf())
                                                                   .receiver(actorPool.get().getRootActor())
                                                                   .workFactory(workFactory)
                                                                   .build());
    }

    private void onDensityProbabilities(DensityCalculatorMessages.DensityProbabilitiesEstimatedMessage message)
    {
        try
        {
            getModel().getProbabilityChunks().put(message.getStartIndex(), message.getProbabilities());

            getLog().info("Received result chunk for intersection segment {} ({} / {}).",
                          getModel().getIntersectionSegment(),
                          getModel().getProbabilityChunks().size(),
                          getModel().getExpectedNumberOfResults());

            getModel().getResultChunkMerger().accept(message);
        }
        finally
        {
            complete(message);
        }
    }

    private void mergeMorePointsThanSamplesResultChunks(DensityCalculatorMessages.DensityProbabilitiesEstimatedMessage message)
    {
        assert getModel().getProbabilities().length == message.getProbabilities().length : "Unexpected number of results delivered!";

        for (var resultsIndex = 0; resultsIndex < message.getProbabilities().length; ++resultsIndex)
        {
            getModel().getProbabilities()[resultsIndex] += message.getProbabilities()[resultsIndex];
        }

        if (getModel().getProbabilityChunks().size() != getModel().getExpectedNumberOfResults())
        {
            return;
        }

        finalizeCalculation();
    }

    private void mergeLessPointsThanSamplesResultChunks(DensityCalculatorMessages.DensityProbabilitiesEstimatedMessage message)
    {
        if (getModel().getProbabilityChunks().size() != getModel().getExpectedNumberOfResults())
        {
            return;
        }

        getModel().setProbabilities(Doubles.concat(getModel().getProbabilityChunks().entrySet().stream()
                                                             .sorted(Comparator.comparingInt(Map.Entry::getKey))
                                                             .map(Map.Entry::getValue)
                                                             .toArray(double[][]::new)));
        finalizeCalculation();
    }

    private void finalizeCalculation()
    {
        val normalizedProbabilities = Arrays.stream(getModel().getProbabilities()).map(p -> p / getModel().getNormalizationFactor()).toArray();
        val localMaximumIndices = Calculate.localMaximumIndices(normalizedProbabilities);
        val nodeCollection = new NodeCollection(getModel().getIntersectionSegment(), localMaximumIndices.length);

        for (val localMaximumIndex : localMaximumIndices)
        {
            nodeCollection.getNodes().add(getModel().getPointsToEvaluate()[localMaximumIndex]);
        }

        publishNodeCollection(nodeCollection);
    }

    private void publishNodeCollection(NodeCollection nodeCollection)
    {
        send(DensityCalculatorMessages.NodeCollectionCreatedMessage.builder()
                                                                   .sender(getModel().getSelf())
                                                                   .receiver(getModel().getSupervisor())
                                                                   .intersectionSegment(getModel().getIntersectionSegment())
                                                                   .nodeCollection(nodeCollection)
                                                                   .build());

        for (val participant : getModel().getParticipants())
        {
            val protocol = getProtocol(participant, ProtocolType.EdgeCreation);
            assert protocol.isPresent() : "Node creation processors must also implement the Edge creation protocol!";

            getModel().getDataTransferManager().transfer(DataSource.create(nodeCollection),
                                                         (dataDistributor, operationId) -> NodeCreationMessages.InitializeNodesTransferMessage.builder()
                                                                                                                                              .sender(dataDistributor)
                                                                                                                                              .receiver(protocol.get().getRootActor())
                                                                                                                                              .operationId(operationId)
                                                                                                                                              .intersectionSegment(nodeCollection.getIntersectionSegment())
                                                                                                                                              .build());
        }

        isReadyToBeTerminated();
    }
}
