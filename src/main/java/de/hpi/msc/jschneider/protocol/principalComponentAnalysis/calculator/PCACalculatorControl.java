package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAMessages;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.PrimitiveMatrix;
import org.ojalgo.matrix.decomposition.QR;
import org.ojalgo.matrix.decomposition.SingularValue;
import org.ojalgo.structure.Access2D;

public class PCACalculatorControl extends AbstractProtocolParticipantControl<PCACalculatorModel>
{
    public PCACalculatorControl(PCACalculatorModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.SequenceSliceDistribution, SequenceSliceDistributionEvents.ProjectionCreatedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(PCAMessages.InitializePCACalculationMessage.class, this::onInitializeCalculation)
                    .match(SequenceSliceDistributionEvents.ProjectionCreatedEvent.class, this::onProjectionCreated)
                    .match(PCAMessages.InitializeColumnMeansTransferMessage.class, this::onInitializeColumnMeansTransfer)
                    .match(PCAMessages.InitializeRTransferMessage.class, this::onInitializeRTransfer);
    }

    private void onInitializeCalculation(PCAMessages.InitializePCACalculationMessage message)
    {
        try
        {
            getModel().setProcessorIndices(message.getProcessorIndices());
            for (val keyValuePair : message.getProcessorIndices().entrySet())
            {
                if (keyValuePair.getValue().equals(getModel().getSelf().path().root()))
                {
                    getModel().setMyProcessorIndex(keyValuePair.getKey());
                    break;
                }
            }
            startCalculation();
        }
        finally
        {
            complete(message);
        }
    }

    private void onProjectionCreated(SequenceSliceDistributionEvents.ProjectionCreatedEvent message)
    {
        try
        {
            getModel().setProjection(message.getProjection());
            startCalculation();
        }
        finally
        {
            complete(message);
        }
    }

    private void startCalculation()
    {
        if (!isReadyToStart())
        {
            return;
        }

        val transposedColumnMeans = Calculate.transposedColumnMeans(getModel().getProjection());
        transferColumnMeans(transposedColumnMeans);

        val dataMatrix = Calculate.columnCenteredDataMatrix(getModel().getProjection(), transposedColumnMeans);
        calculateAndTransferR(dataMatrix);

        getModel().getCurrentCalculationStep().increment();
        continueCalculation();
    }

    private boolean isReadyToStart()
    {
        return getModel().getProcessorIndices() != null && getModel().getProjection() != null;
    }

    private void transferColumnMeans(PrimitiveMatrix columnMeans)
    {
        if (getModel().getMyProcessorIndex() == 0)
        {
            getModel().getTransposedColumnMeans().put(getModel().getSelf().path().root(), columnMeans);
            getModel().getNumberOfRows().put(getModel().getSelf().path().root(), columnMeans.countRows());
            return;
        }

        val receiverProtocol = getPCAProtocolAtProcessorWithIndex(0);
        getModel().getDataTransferManager().transfer(columnMeans, dataDistributor -> PCAMessages.InitializeColumnMeansTransferMessage.builder()
                                                                                                                                     .sender(getModel().getSelf())
                                                                                                                                     .receiver(receiverProtocol.getRootActor())
                                                                                                                                     .operationId(dataDistributor.getOperationId())
                                                                                                                                     .processorIndex(getModel().getMyProcessorIndex())
                                                                                                                                     .numberOfRows(columnMeans.countRows())
                                                                                                                                     .build());
    }

    private void calculateAndTransferR(Access2D<Double> matrix)
    {
        val qrDecomposition = QR.PRIMITIVE.make(matrix);
        getModel().setLocalR(qrDecomposition.getR());
        transferR();
    }

    private void transferR()
    {
        val stepNumber = getModel().getCurrentCalculationStep().get();
        val receiverIndex = nextRReceiverIndex();

        if (receiverIndex < 0 || receiverIndex == getModel().getMyProcessorIndex())
        {
            return;
        }

        val receiverProtocol = getPCAProtocolAtProcessorWithIndex(receiverIndex);
        getModel().getDataTransferManager().transfer(getModel().getLocalR(), dataDistributor -> PCAMessages.InitializeRTransferMessage.builder()
                                                                                                                                      .sender(getModel().getSelf())
                                                                                                                                      .receiver(receiverProtocol.getRootActor())
                                                                                                                                      .processorIndex(getModel().getMyProcessorIndex())
                                                                                                                                      .currentStepNumber(stepNumber)
                                                                                                                                      .operationId(dataDistributor.getOperationId())
                                                                                                                                      .build());
    }

    private long nextRReceiverIndex()
    {
        return getModel().getMyProcessorIndex() - numberOfInvolvedProcessors(getModel().getCurrentCalculationStep().get() + 1);
    }

    private long numberOfInvolvedProcessors(long step)
    {
        return (int) (numberOfProcessors() * Math.pow(0.5d, step));
    }

    private long numberOfProcessors()
    {
        return Calculate.nextPowerOfTwo(getModel().getProcessorIndices().size());
    }

    private Protocol getPCAProtocolAtProcessorWithIndex(long index)
    {
        val remoteRootPath = getModel().getProcessorIndices().get(index);
        val protocol = getProtocol(remoteRootPath, ProtocolType.PrincipalComponentAnalysis);

        assert protocol.isPresent() : String.format("Unable to find PCA protocol for processor at index %1$d, which should never happen!", index);
        return protocol.get();
    }

    private void continueCalculation()
    {
        val currentStep = getModel().getCurrentCalculationStep().get();
        if (getModel().getMyProcessorIndex() >= numberOfInvolvedProcessors(currentStep))
        {
            // we dont have to do anything anymore
            // TODO: terminate self?!
            return;
        }

        val involvedProcessors = numberOfInvolvedProcessors(currentStep);
        val expectedRSenderIndex = getModel().getMyProcessorIndex() + involvedProcessors;
        Access2D<Double> dataMatrix = getModel().getLocalR();
        if (expectedRSenderIndex < getModel().getProcessorIndices().size())
        {
            // we expect to receive a R from an other processor
            val remoteR = getModel().getRemoteRsByProcessStep().get(currentStep);
            if (remoteR == null)
            {
                // not yet received
                return;
            }
            getModel().getRemoteRsByProcessStep().remove(currentStep);
            dataMatrix = MatrixInitializer.concat(dataMatrix, remoteR);
        }

        calculateAndTransferR(dataMatrix);

        getModel().getCurrentCalculationStep().increment();
        if (isLastStep())
        {
            finalizeCalculation();
        }
        else
        {
            continueCalculation();
        }
    }

    private void finalizeCalculation()
    {
        if (getModel().getMyProcessorIndex() != 0)
        {
            return;
        }

        if (!isLastStep())
        {
            return;
        }

        if (!receivedAllColumnMeans())
        {
            return;
        }

        val totalColumnMeans = totalColumnMeans();
        val matrixInitializer = new MatrixInitializer(getModel().getProjection().countColumns());
        for (var processorIndex = 0; processorIndex < getModel().getProcessorIndices().size(); ++processorIndex)
        {
            val diff = getTransposedColumnMeans(processorIndex).subtract(totalColumnMeans);
            matrixInitializer.append(diff.multiply(Math.sqrt(getNumberOfRows(processorIndex))));
        }
        matrixInitializer.append(getModel().getLocalR());

        val qrDecomposition = QR.PRIMITIVE.make(matrixInitializer.create());
        val svd = SingularValue.PRIMITIVE.make(qrDecomposition.getR());
    }

    private boolean isLastStep()
    {
        val currentStep = getModel().getCurrentCalculationStep().get();
        val lastStep = (int) Math.ceil(Calculate.log2(numberOfProcessors()));

        return currentStep >= lastStep;
    }

    private void onInitializeColumnMeansTransfer(PCAMessages.InitializeColumnMeansTransferMessage message)
    {
        assert getModel().getMyProcessorIndex() == 0 : String.format("%1$s trying to send column means, although we are not processor#0!", message.getSender().path());

        getModel().getNumberOfRows().put(message.getSender().path().root(), message.getNumberOfRows());

        getModel().getDataTransferManager().accept(message, dataReceiver ->
        {
            val sink = new MatrixInitializer(getModel().getProjection().countColumns());
            dataReceiver.addSink(sink);
            dataReceiver.whenFinished(receiver -> columnMeansTransferFinished(message.getSender().path().root(), sink));
            return dataReceiver;
        });
    }

    private void columnMeansTransferFinished(RootActorPath sender, MatrixInitializer sink)
    {
        getLog().info(String.format("Received column means from %1$s.", sender));
        getModel().getTransposedColumnMeans().put(sender, sink.create());

        finalizeCalculation();
    }

    private void onInitializeRTransfer(PCAMessages.InitializeRTransferMessage message)
    {
        getModel().getDataTransferManager().accept(message, dataReceiver ->
        {
            val sink = new MatrixInitializer(getModel().getProjection().countColumns());
            dataReceiver.addSink(sink);
            dataReceiver.whenFinished(receiver -> whenRTransferFinished(message.getCurrentStepNumber(), sink));
            return dataReceiver;
        });
    }

    private void whenRTransferFinished(long stepNumber, MatrixInitializer matrixInitializer)
    {
        getModel().getRemoteRsByProcessStep().put(stepNumber, matrixInitializer.create());
        continueCalculation();
    }

    private boolean receivedAllColumnMeans()
    {
        return getModel().getTransposedColumnMeans().size() == getModel().getProcessorIndices().size();
    }

    private PrimitiveMatrix totalColumnMeans()
    {
        val columnMeans = Calculate.filledVector(getModel().getProjection().countColumns(), 0.0d);
        for (val processor : getModel().getNumberOfRows().keySet())
        {
            val numberOfRows = getModel().getNumberOfRows().get(processor);
            val transposedColumnMeans = getModel().getTransposedColumnMeans().get(processor);

            columnMeans.add(transposedColumnMeans.multiply(numberOfRows));
        }

        return columnMeans.multiply(1.0d / getModel().getProcessorIndices().size());
    }

    private PrimitiveMatrix getTransposedColumnMeans(int processorIndex)
    {
        val processor = getModel().getProcessorIndices().get(processorIndex);
        return getModel().getTransposedColumnMeans().get(processor);
    }

    private long getNumberOfRows(int processorIndex)
    {
        val processor = getModel().getProcessorIndices().get(processorIndex);
        return getModel().getNumberOfRows().get(processor);
    }
}
