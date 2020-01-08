package de.hpi.msc.jschneider.protocol.dimensionReduction.receiver;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionEvents;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionMessages;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import lombok.val;

public class DimensionReductionReceiverControl extends AbstractProtocolParticipantControl<DimensionReductionReceiverModel>
{
    public DimensionReductionReceiverControl(DimensionReductionReceiverModel model)
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
                    .match(SequenceSliceDistributionEvents.ProjectionCreatedEvent.class, this::onProjectionCreated)
                    .match(DimensionReductionMessages.InitializePrincipalComponentsTransferMessage.class, this::onInitializePrincipalComponentsTransfer)
                    .match(DimensionReductionMessages.InitializeRotationTransferMessage.class, this::onInitializeRotationTransfer);
    }

    private void onProjectionCreated(SequenceSliceDistributionEvents.ProjectionCreatedEvent message)
    {
        try
        {
            getModel().setProjection(message.getProjection());
            getModel().setFirstSubSequenceIndex(message.getFirstSubSequenceIndex());
        }
        finally
        {
            complete(message);
        }
    }

    private void onInitializePrincipalComponentsTransfer(DimensionReductionMessages.InitializePrincipalComponentsTransferMessage message)
    {
        getModel().getDataTransferManager().accept(message, dataReceiver ->
                dataReceiver.addSink(getModel().getPrincipalComponentsSink())
                            .whenFinished(this::onPrincipalComponentTransferFinished));
    }

    private void onInitializeRotationTransfer(DimensionReductionMessages.InitializeRotationTransferMessage message)
    {
        getModel().getDataTransferManager().accept(message, dataReceiver ->
                dataReceiver.addSink(getModel().getRotationSink())
                            .whenFinished(this::onRotationTransferFinished));
    }

    private void onPrincipalComponentTransferFinished(DataReceiver dataReceiver)
    {
        getModel().setPrincipalComponents(getModel().getPrincipalComponentsSink().getMatrix(3L));
        performDimensionReduction();
    }

    private void onRotationTransferFinished(DataReceiver dataReceiver)
    {
        getModel().setRotation(getModel().getRotationSink().getMatrix(3L));
        performDimensionReduction();
    }

    private void performDimensionReduction()
    {
        if (getModel().getPrincipalComponents() == null)
        {
            return;
        }

        if (getModel().getRotation() == null)
        {
            return;
        }

        val reducedProjection = getModel().getProjection().multiply(getModel().getPrincipalComponents());
        val rotatedProjection = getModel().getRotation().multiply(reducedProjection.transpose());
        val projection2d = rotatedProjection.logical().row(1, 2).get();

        trySendEvent(ProtocolType.DimensionReduction, eventDispatcher ->
                DimensionReductionEvents.ReducedProjectionCreatedEvent.builder()
                                                                      .sender(getModel().getSelf())
                                                                      .receiver(eventDispatcher)
                                                                      .reducedProjection(projection2d)
                                                                      .firstSubSequenceIndex(getModel().getFirstSubSequenceIndex())
                                                                      .build());
        // TODO: terminate self?!
    }
}
