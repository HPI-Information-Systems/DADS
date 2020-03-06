package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator;

import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAEvents;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.PrimitiveMatrixSink;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.decomposition.QR;
import org.ojalgo.matrix.decomposition.SingularValue;
import org.ojalgo.matrix.store.MatrixStore;

import java.time.LocalDateTime;

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
                if (keyValuePair.getValue().equals(ProcessorId.of(getModel().getSelf())))
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
            getModel().setMinimumRecord(Math.min(getModel().getMinimumRecord(), message.getMinimumRecord()));
            getModel().setMaximumRecord(Math.max(getModel().getMaximumRecord(), message.getMaximumRecord()));
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

        getModel().setStartTime(LocalDateTime.now());
        getLog().info("Starting PCA calculation.");

        val transposedColumnMeans = Calculate.transposedColumnMeans(getModel().getProjection());
        transferColumnMeans(transposedColumnMeans);

        val dataMatrix = Calculate.columnCenteredDataMatrix(getModel().getProjection(), transposedColumnMeans);
        calculateAndTransferR(dataMatrix);

        getLog().debug("PCA calculation for step {} done.", getModel().getCurrentCalculationStep().get());
        getModel().getCurrentCalculationStep().increment();

        if (numberOfProcessors() > 1)
        {
            continueCalculation();
        }
        else
        {
            finalizeCalculation();
        }
    }

    private boolean isReadyToStart()
    {
        return getModel().getProcessorIndices() != null && getModel().getProjection() != null;
    }

    private void transferColumnMeans(MatrixStore<Double> columnMeans)
    {
        val numberOfRows = getModel().getProjection().countRows();

        if (getModel().getMyProcessorIndex() == 0)
        {
            getModel().getTransposedColumnMeans().put(ProcessorId.of(getModel().getSelf()), columnMeans);
            getModel().getNumberOfRows().put(ProcessorId.of(getModel().getSelf()), numberOfRows);
            return;
        }

        val receiverProtocol = getPCAProtocolAtProcessorWithIndex(0);
        getModel().getDataTransferManager().transfer(GenericDataSource.create(columnMeans),
                                                     (dataDistributor, operationId) -> PCAMessages.InitializeColumnMeansTransferMessage.builder()
                                                                                                                                       .sender(dataDistributor)
                                                                                                                                       .receiver(receiverProtocol.getRootActor())
                                                                                                                                       .operationId(operationId)
                                                                                                                                       .processorIndex(getModel().getMyProcessorIndex())
                                                                                                                                       .numberOfRows(numberOfRows)
                                                                                                                                       .minimumRecord(getModel().getMinimumRecord())
                                                                                                                                       .maximumRecord(getModel().getMaximumRecord())
                                                                                                                                       .build());

        getLog().info("Transferring PCA column means to {}.", ProcessorId.of(receiverProtocol.getRootActor()));
    }

    private void calculateAndTransferR(MatrixStore<Double> matrix)
    {
        val qrDecomposition = QR.PRIMITIVE.make();
        qrDecomposition.compute(matrix);
        getModel().setLocalR(qrDecomposition.getR());
        transferR();
    }

    private void transferR()
    {
        val stepNumber = getModel().getCurrentCalculationStep().get();
        val receiverIndex = nextRReceiverIndex();

        if (receiverIndex < 0)
        {
            return;
        }

        if (receiverIndex == getModel().getMyProcessorIndex())
        {
            getModel().getRemoteRsByProcessStep().put(stepNumber, getModel().getLocalR());
            return;
        }

        val receiverProtocol = getPCAProtocolAtProcessorWithIndex(receiverIndex);
        getModel().getDataTransferManager().transfer(GenericDataSource.create(getModel().getLocalR()),
                                                     (dataDistributor, operationId) -> PCAMessages.InitializeRTransferMessage.builder()
                                                                                                                             .sender(dataDistributor)
                                                                                                                             .receiver(receiverProtocol.getRootActor())
                                                                                                                             .processorIndex(getModel().getMyProcessorIndex())
                                                                                                                             .currentStepNumber(stepNumber)
                                                                                                                             .operationId(operationId)
                                                                                                                             .build());

        getLog().info("Transferring local R (step = {}) to {}.", stepNumber, ProcessorId.of(receiverProtocol.getRootActor()));
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
            getLog().info("Stopping PCA calculation at step {}, because we (index = {}) are no longer involved.",
                          currentStep,
                          getModel().getMyProcessorIndex());
            // we dont have to do anything anymore
            getModel().setEndTime(LocalDateTime.now());

            trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.PCACreatedEvent.builder()
                                                                                                     .sender(getModel().getSelf())
                                                                                                     .receiver(eventDispatcher)
                                                                                                     .startTime(getModel().getStartTime())
                                                                                                     .endTime(getModel().getEndTime())
                                                                                                     .build());
            return;
        }

        val involvedProcessors = numberOfInvolvedProcessors(currentStep);
        val expectedRSenderIndex = getModel().getMyProcessorIndex() + involvedProcessors;
        var dataMatrix = getModel().getLocalR();
        if (expectedRSenderIndex < getModel().getProcessorIndices().size())
        {
            getLog().debug("Expecting remote R (step = {}).", currentStep - 1);

            // we expect to receive a R from an other processor
            val remoteR = getModel().getRemoteRsByProcessStep().get(currentStep - 1);
            if (remoteR == null)
            {
                // not yet received
                return;
            }
            getModel().getRemoteRsByProcessStep().remove(currentStep - 1);
            dataMatrix = MatrixInitializer.concat(dataMatrix, remoteR);
        }

        calculateAndTransferR(dataMatrix);

        getLog().debug("PCA calculation for step {} done.", getModel().getCurrentCalculationStep().get());
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

        getLog().info("Finalizing PCA calculation.");

        val totalColumnMeans = totalColumnMeans();
        val matrixInitializer = new MatrixInitializer(getModel().getProjection().countColumns());
        for (var processorIndex = 0; processorIndex < getModel().getProcessorIndices().size(); ++processorIndex)
        {
            val diff = getTransposedColumnMeans(processorIndex).subtract(totalColumnMeans);
            matrixInitializer.append(diff.multiply(Math.sqrt(getNumberOfRows(processorIndex))));
        }
        matrixInitializer.append(getModel().getLocalR());

        val qrDecomposition = QR.PRIMITIVE.make();
        qrDecomposition.compute(matrixInitializer.create());
        val svd = SingularValue.PRIMITIVE.make();
        svd.compute(qrDecomposition.getR());
        val principalComponents = normalizePrincipalComponents(svd.getV().logical().column(0, 1, 2).get());
        val referenceVector = createReferenceVector(principalComponents, totalColumnMeans);
        val rotation = Calculate.rotation(referenceVector, Calculate.makeRowVector(0.0d, 0.0d, 1.0d));

        getModel().setEndTime(LocalDateTime.now());

        trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.PCACreatedEvent.builder()
                                                                                                 .sender(getModel().getSelf())
                                                                                                 .receiver(eventDispatcher)
                                                                                                 .startTime(getModel().getStartTime())
                                                                                                 .endTime(getModel().getEndTime())
                                                                                                 .build());

        trySendEvent(ProtocolType.PrincipalComponentAnalysis, eventDispatcher -> PCAEvents.PrincipalComponentsCreatedEvent.builder()
                                                                                                                          .sender(getModel().getSelf())
                                                                                                                          .receiver(eventDispatcher)
                                                                                                                          .principalComponents(principalComponents)
                                                                                                                          .rotation(rotation)
                                                                                                                          .columnMeans(totalColumnMeans)
                                                                                                                          .build());
    }

    private boolean isLastStep()
    {
        val currentStep = getModel().getCurrentCalculationStep().get();
        val lastStep = (int) Math.ceil(Calculate.log2(numberOfProcessors())) + 1;

        return currentStep == lastStep;
    }

    private MatrixStore<Double> normalizePrincipalComponents(MatrixStore<Double> principalComponents)
    {
        val result = principalComponents.copy();
        for (var columnIndex = 0L; columnIndex < result.countColumns(); ++columnIndex)
        {
            if (result.get(0L, columnIndex) >= 0.0d)
            {
                continue;
            }

            for (var rowIndex = 0L; rowIndex < result.countRows(); ++rowIndex)
            {
                result.set(rowIndex, columnIndex, principalComponents.get(rowIndex, columnIndex) * -1.0d);
            }
        }

        return result;
    }

    private MatrixStore<Double> createReferenceVector(MatrixStore<Double> principalComponents, MatrixStore<Double> columnMeans)
    {
        assert getModel().getLocalProcessor().isMaster() : "Only the master is able to create the reference vector!";

        return Calculate.makeFilledRowVector(getModel().getProjection().countColumns(), getModel().getConvolutionSize() * getModel().getMinimumRecord())
                        .subtract(columnMeans)
                        .multiply(principalComponents);
    }

    private void onInitializeColumnMeansTransfer(PCAMessages.InitializeColumnMeansTransferMessage message)
    {
        assert getModel().getMyProcessorIndex() == 0 : String.format("%1$s trying to send column means, although we are not processor#0!", message.getSender().path());

        getModel().getNumberOfRows().put(ProcessorId.of(message.getSender()), message.getNumberOfRows());
        getModel().setMinimumRecord(Math.min(getModel().getMinimumRecord(), message.getMinimumRecord()));
        getModel().setMaximumRecord(Math.max(getModel().getMaximumRecord(), message.getMaximumRecord()));

        getModel().getDataTransferManager().accept(message, dataReceiver ->
        {
            val sink = new PrimitiveMatrixSink();
            dataReceiver.addSink(sink);
            dataReceiver.whenFinished(receiver -> columnMeansTransferFinished(ProcessorId.of(message.getSender()), sink));
            return dataReceiver;
        });
    }

    private void columnMeansTransferFinished(ProcessorId sender, PrimitiveMatrixSink sink)
    {
        getLog().debug("Received column means from {}.", sender);
        getModel().getTransposedColumnMeans().put(sender, sink.getMatrix(getModel().getProjection().countColumns()));

        finalizeCalculation();
    }

    private void onInitializeRTransfer(PCAMessages.InitializeRTransferMessage message)
    {
        getModel().getDataTransferManager().accept(message, dataReceiver ->
        {
            val sink = new PrimitiveMatrixSink();
            dataReceiver.addSink(sink);
            dataReceiver.whenFinished(receiver -> whenRTransferFinished(message.getCurrentStepNumber(), sink));
            return dataReceiver;
        });
    }

    private void whenRTransferFinished(long stepNumber, PrimitiveMatrixSink sink)
    {
        getLog().debug("Received R for step {}.", stepNumber);

        getModel().getRemoteRsByProcessStep().put(stepNumber, sink.getMatrix(getModel().getProjection().countColumns()));
        continueCalculation();
    }

    private boolean receivedAllColumnMeans()
    {
        return getModel().getTransposedColumnMeans().size() == getModel().getProcessorIndices().size();
    }

    private MatrixStore<Double> totalColumnMeans()
    {
        var columnMeans = Calculate.makeFilledRowVector(getModel().getProjection().countColumns(), 0.0d);
        val totalNumberOfRows = getModel().getNumberOfRows().values().stream().mapToLong(numberOfRows -> numberOfRows).sum();
        for (val processor : getModel().getNumberOfRows().keySet())
        {
            val numberOfRows = getModel().getNumberOfRows().get(processor);
            val transposedColumnMeans = getModel().getTransposedColumnMeans().get(processor);

            columnMeans = columnMeans.add(transposedColumnMeans.multiply(numberOfRows / (double) totalNumberOfRows));
        }

        return columnMeans;
    }

    private MatrixStore<Double> getTransposedColumnMeans(long processorIndex)
    {
        val processor = getModel().getProcessorIndices().get(processorIndex);
        return getModel().getTransposedColumnMeans().get(processor);
    }

    private long getNumberOfRows(long processorIndex)
    {
        val processor = getModel().getProcessorIndices().get(processorIndex);
        return getModel().getNumberOfRows().get(processor);
    }
}
