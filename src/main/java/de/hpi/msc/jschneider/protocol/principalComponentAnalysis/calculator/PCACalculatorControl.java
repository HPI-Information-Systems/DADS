package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator;

import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAMessages;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import org.ojalgo.matrix.decomposition.QR;

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
        return builder.match(PCAMessages.InitializePCACalculationMessage.class, this::onInitializeCalculation)
                      .match(SequenceSliceDistributionEvents.ProjectionCreatedEvent.class, this::onProjectionCreated);
    }

    private void onInitializeCalculation(PCAMessages.InitializePCACalculationMessage message)
    {
        try
        {
            getModel().setProcessorIndices(message.getProcessorIndices());
            getModel().setMyProcessorIndex(message.getProcessorIndices().get(getModel().getSelf().path().root()));

            if (!isReadyToStart())
            {
                return;
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
            if (!isReadyToStart())
            {
                return;
            }

            startCalculation();
        }
        finally
        {
            complete(message);
        }
    }

    private boolean isReadyToStart()
    {
        return getModel().getProcessorIndices() != null && getModel().getProjection() != null;
    }

    private void startCalculation()
    {
        val dataMatrix = Calculate.columnCenteredDataMatrix(getModel().getProjection());
        val qrDecomposition = QR.PRIMITIVE.make(dataMatrix);
    }
}
