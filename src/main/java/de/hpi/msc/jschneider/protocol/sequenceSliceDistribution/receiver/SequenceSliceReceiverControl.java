package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
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
        return super.complementReceiveBuilder(builder)
                    .match(SequenceSliceDistributionMessages.InitializeSequenceSliceTransferMessage.class, this::onInitializeTransfer);
    }

    private void onInitializeTransfer(SequenceSliceDistributionMessages.InitializeSequenceSliceTransferMessage message)
    {
        getModel().setFirstSubSequenceIndex(message.getFirstSubSequenceIndex());
        getModel().setSubSequenceLength(message.getSubSequenceLength());
        getModel().setConvolutionSize(message.getConvolutionSize());
        getModel().setProjectionInitializer(new MatrixInitializer(getModel().getSubSequenceLength() - getModel().getConvolutionSize()));

        getModel().getDataTransferManager().accept(message,
                                                   dataReceiver -> dataReceiver.onReceive(this::onSlicePart)
                                                                               .whenFinished(this::whenFinished)
                                                                               .addSink(getModel().getSequenceWriter()));
    }

    private void onSlicePart(DataTransferMessages.DataPartMessage message)
    {
        if (message.getPart().length < 1)
        {
            getLog().error("Received empty sequence slice part!");
            return;
        }

        val newUnusedRecords = new float[getModel().getUnusedRecords().length + message.getPart().length];
        System.arraycopy(getModel().getUnusedRecords(), 0, newUnusedRecords, 0, getModel().getUnusedRecords().length);
        System.arraycopy(message.getPart(), 0, newUnusedRecords, getModel().getUnusedRecords().length, message.getPart().length);
        getModel().setUnusedRecords(newUnusedRecords);

        embedSubSequences();
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

    private void whenFinished(DataReceiver receiver)
    {
        trySendEvent(ProtocolType.SequenceSliceDistribution, eventDispatcher -> SequenceSliceDistributionEvents.ProjectionCreatedEvent.builder()
                                                                                                                                      .sender(getModel().getSelf())
                                                                                                                                      .receiver(eventDispatcher)
                                                                                                                                      .firstSubSequenceIndex(getModel().getFirstSubSequenceIndex())
                                                                                                                                      .projection(getModel().getProjectionInitializer().create())
                                                                                                                                      .build());
    }
}
