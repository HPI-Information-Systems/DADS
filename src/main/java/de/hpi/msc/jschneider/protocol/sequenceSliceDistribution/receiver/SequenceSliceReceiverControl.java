package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.SequenceMatrixInitializer;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;

import java.time.LocalDateTime;

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
        getModel().setLastSubSequenceChunk(message.isLastSubSequenceChunk());
        getModel().setProjectionInitializer(new SequenceMatrixInitializer(getModel().getSubSequenceLength(), getModel().getConvolutionSize()));

        trySendEvent(ProtocolType.SequenceSliceDistribution, eventDispatcher -> SequenceSliceDistributionEvents.SubSequenceParametersReceivedEvent.builder()
                                                                                                                                                  .sender(getModel().getSelf())
                                                                                                                                                  .receiver(eventDispatcher)
                                                                                                                                                  .firstSubSequenceIndex(getModel().getFirstSubSequenceIndex())
                                                                                                                                                  .isLastSubSequenceChunk(getModel().isLastSubSequenceChunk())
                                                                                                                                                  .convolutionSize(getModel().getConvolutionSize())
                                                                                                                                                  .subSequenceLength(getModel().getSubSequenceLength())
                                                                                                                                                  .build());

        getModel().getDataTransferManager().accept(message,
                                                   dataReceiver -> dataReceiver.whenDataPartReceived(this::onSlicePart)
                                                                               .whenFinished(this::whenFinished)
                                                                               .addSink(getModel().getSequenceWriter()));

        getModel().setStartTime(LocalDateTime.now());
        getLog().debug("Start receiving sequence slice from {}.", message.getSender().path());
    }

    private void onSlicePart(DataTransferMessages.DataPartMessage message)
    {
        if (message.getPart().length < 1)
        {
            getLog().error("Received empty sequence slice part!");
            return;
        }

        val doubles = Serialize.toDoubles(message.getPart());

        getModel().setMinimumRecord(Math.min(getModel().getMinimumRecord(), Doubles.min(doubles)));
        getModel().setMaximumRecord(Math.max(getModel().getMaximumRecord(), Doubles.max(doubles)));
        getModel().getProjectionInitializer().append(doubles);
    }

    private void whenFinished(DataReceiver receiver)
    {
        val projection = getModel().getProjectionInitializer().create();

        getModel().setEndTime(LocalDateTime.now());

        trySendEvent(ProtocolType.Statistics, eventDispatcher -> StatisticsEvents.ProjectionCreatedEvent.builder()
                                                                                                        .sender(getModel().getSelf())
                                                                                                        .receiver(eventDispatcher)
                                                                                                        .startTime(getModel().getStartTime())
                                                                                                        .endTime(getModel().getEndTime())
                                                                                                        .build());

        getLog().info("Local projection ({} x {}) for sub sequences [{}, {}) created (isLastSubSequenceChunk = {}).",
                      projection.countRows(),
                      projection.countColumns(),
                      getModel().getFirstSubSequenceIndex(),
                      getModel().getFirstSubSequenceIndex() + projection.countRows(),
                      getModel().isLastSubSequenceChunk());

        trySendEvent(ProtocolType.SequenceSliceDistribution, eventDispatcher -> SequenceSliceDistributionEvents.ProjectionCreatedEvent.builder()
                                                                                                                                      .sender(getModel().getSelf())
                                                                                                                                      .receiver(eventDispatcher)
                                                                                                                                      .firstSubSequenceIndex(getModel().getFirstSubSequenceIndex())
                                                                                                                                      .isLastSubSequenceChunk(getModel().isLastSubSequenceChunk())
                                                                                                                                      .minimumRecord(getModel().getMinimumRecord())
                                                                                                                                      .maximumRecord(getModel().getMaximumRecord())
                                                                                                                                      .projection(projection)
                                                                                                                                      .build());
    }
}
