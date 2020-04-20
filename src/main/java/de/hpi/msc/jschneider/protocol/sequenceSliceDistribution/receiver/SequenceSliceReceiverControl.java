package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.Debug;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.ImprovedSequenceMatrixSink;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.NaiveSequenceMatrixSink;
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

        trySendEvent(ProtocolType.SequenceSliceDistribution, eventDispatcher -> SequenceSliceDistributionEvents.SubSequenceParametersReceivedEvent.builder()
                                                                                                                                                  .sender(getModel().getSelf())
                                                                                                                                                  .receiver(eventDispatcher)
                                                                                                                                                  .firstSubSequenceIndex(getModel().getFirstSubSequenceIndex())
                                                                                                                                                  .isLastSubSequenceChunk(getModel().isLastSubSequenceChunk())
                                                                                                                                                  .convolutionSize(getModel().getConvolutionSize())
                                                                                                                                                  .subSequenceLength(getModel().getSubSequenceLength())
                                                                                                                                                  .build());

        getModel().getDataTransferManager().accept(message,
                                                   dataReceiver ->
                                                   {
                                                       if (SystemParameters.getCommand().isDisableSequenceMatrix())
                                                       {
                                                           getModel().setProjectionSink(new NaiveSequenceMatrixSink(getModel().getSubSequenceLength(), getModel().getConvolutionSize()));
                                                       }
                                                       else
                                                       {
                                                           getModel().setProjectionSink(new ImprovedSequenceMatrixSink(getModel().getSubSequenceLength(), getModel().getConvolutionSize()));
                                                       }
                                                       return dataReceiver.addSink(getModel().getProjectionSink())
                                                                          .whenFinished(this::whenFinished);
                                                   });

        getModel().setStartTime(LocalDateTime.now());
        getLog().debug("Start receiving sequence slice from {}.", message.getSender().path());
    }

    private void whenFinished(DataReceiver receiver)
    {
        val projection = getModel().getProjectionSink().getMatrix();
        val minimumRecord = getModel().getProjectionSink().getMinimumRecord();
        val maximumRecord = getModel().getProjectionSink().getMaximumRecord();

        Debug.print(projection, "projection.txt");
        Debug.printBinary(projection, "projection.bin");

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
                                                                                                                                      .minimumRecord(minimumRecord)
                                                                                                                                      .maximumRecord(maximumRecord)
                                                                                                                                      .projection(projection)
                                                                                                                                      .build());

        isReadyToBeTerminated();
    }
}
