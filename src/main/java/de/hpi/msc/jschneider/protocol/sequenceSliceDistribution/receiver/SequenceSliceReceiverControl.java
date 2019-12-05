package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import lombok.val;
import lombok.var;

public class SequenceSliceReceiverControl extends AbstractProtocolParticipantControl<SequenceSliceReceiverModel>
{
    public SequenceSliceReceiverControl(SequenceSliceReceiverModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(SequenceSliceDistributionMessages.InitializeSliceTransferMessage.class, this::onInitializeTransfer)
                      .match(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class, this::onSlicePart);
    }

    private void onInitializeTransfer(SequenceSliceDistributionMessages.InitializeSliceTransferMessage message)
    {
        try
        {
            getModel().setFirstSubSequenceIndex(message.getFirstSubSequenceIndex());
            getModel().setSubSequenceLength(message.getSubSequenceLength());
            getModel().setConvolutionSize(message.getConvolutionSize());
            getModel().setProjectionInitializer(new MatrixInitializer(getModel().getSubSequenceLength() - getModel().getConvolutionSize()));

            send(SequenceSliceDistributionMessages.RequestNextSlicePartMessage.builder()
                                                                              .sender(getModel().getSelf())
                                                                              .receiver(message.getSender())
                                                                              .build());
        }
        finally
        {
            complete(message);
        }
    }

    private void onSlicePart(SequenceSliceDistributionMessages.SequenceSlicePartMessage message)
    {
        try
        {
            if (message.getSlicePart().length < 1)
            {
                getLog().error("Received empty sequence slice part!");
                return;
            }

            getModel().getSequenceWriter().write(message.getSlicePart());

            val newUnusedRecords = new float[getModel().getUnusedRecords().length + message.getSlicePart().length];
            System.arraycopy(getModel().getUnusedRecords(), 0, newUnusedRecords, 0, getModel().getUnusedRecords().length);
            System.arraycopy(message.getSlicePart(), 0, newUnusedRecords, getModel().getUnusedRecords().length, message.getSlicePart().length);
            getModel().setUnusedRecords(newUnusedRecords);

            embedSubSequences();

            if (message.isLastPart())
            {
                trySendEvent(ProtocolType.SequenceSliceDistribution, eventDispatcher -> SequenceSliceDistributionEvents.ProjectionCreatedEvent.builder()
                                                                                                                                              .sender(getModel().getSelf())
                                                                                                                                              .receiver(eventDispatcher)
                                                                                                                                              .firstSubSequenceIndex(getModel().getFirstSubSequenceIndex())
                                                                                                                                              .projectionSpace(getModel().getProjectionInitializer().create())
                                                                                                                                              .build());
                return;
            }

            send(SequenceSliceDistributionMessages.RequestNextSlicePartMessage.builder()
                                                                              .sender(getModel().getSelf())
                                                                              .receiver(message.getSender())
                                                                              .build());
        }
        finally
        {
            complete(message);
        }
    }

    private void embedSubSequences()
    {
        val lastSubSequenceStart = getModel().getUnusedRecords().length - getModel().getSubSequenceLength();
        for (var subSequenceStart = 0; subSequenceStart <= lastSubSequenceStart; ++subSequenceStart)
        {
            embedSubSequence(subSequenceStart);
        }

        val newUnusedRecords = new float[getModel().getSubSequenceLength() - 1];
        System.arraycopy(getModel().getUnusedRecords(), getModel().getUnusedRecords().length - newUnusedRecords.length, newUnusedRecords, 0, newUnusedRecords.length);
        getModel().setUnusedRecords(newUnusedRecords);
    }

    private void embedSubSequence(int subSequenceStartIndex)
    {
        val vector = new float[getModel().getSubSequenceLength() - getModel().getConvolutionSize()];
        for (var vectorIndex = 0; vectorIndex < vector.length; ++vectorIndex)
        {
            var value = 0.0f;
            for (var convolutionIndex = 0; convolutionIndex < getModel().getConvolutionSize(); ++convolutionIndex)
            {
                value += getModel().getUnusedRecords()[convolutionIndex + subSequenceStartIndex];
            }
            vector[vectorIndex] = value;
        }

        getModel().getProjectionInitializer().appendRow(vector);
    }
}
