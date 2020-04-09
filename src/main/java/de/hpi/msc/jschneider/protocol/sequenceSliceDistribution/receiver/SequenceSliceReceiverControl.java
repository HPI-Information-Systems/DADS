package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.protocol.statistics.StatisticsEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import de.hpi.msc.jschneider.utility.matrix.MatrixBuilder;
import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import de.hpi.msc.jschneider.utility.matrix.SequenceMatrixBuilder;
import lombok.val;
import lombok.var;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

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
        getModel().setProjectionBuilder(createProjectionBuilder());

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
                                                                               .whenFinished(this::whenFinished));

        getModel().setStartTime(LocalDateTime.now());
        getLog().debug("Start receiving sequence slice from {}.", message.getSender().path());
    }

    private MatrixBuilder createProjectionBuilder()
    {
        if (!SystemParameters.getCommand().isDisableSequenceMatrix())
        {
            return new SequenceMatrixBuilder(getModel().getSubSequenceLength(), getModel().getConvolutionSize());
        }
        else
        {
            return new RowMatrixBuilder(getModel().getSubSequenceLength() - getModel().getConvolutionSize());
        }
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
        appendNewValuesToProjection(doubles);
    }

    private void appendNewValuesToProjection(double[] values)
    {
        if (!SystemParameters.getCommand().isDisableSequenceMatrix())
        {
            getModel().getProjectionBuilder().append(values);
            return;
        }

        getModel().setUnusedRecords(Doubles.concat(getModel().getUnusedRecords(), values));
        embedSubSequences();
    }


    private void embedSubSequences()
    {
        val unusedRecords = getModel().getUnusedRecords();
        val lastSubSequenceStart = unusedRecords.length - getModel().getSubSequenceLength();
        var subSequence = getModel().getRawSubSequence();
        for (var subSequenceStart = 0; subSequenceStart <= lastSubSequenceStart; ++subSequenceStart)
        {
            if (subSequence == null)
            {
                subSequence = createFirstSubSequence();
            }
            else
            {
                subSequence.remove(0);
                var value = 0.0d;
                for (var convolutionIndex = 0; convolutionIndex < getModel().getConvolutionSize(); ++convolutionIndex)
                {
                    value += unusedRecords[subSequenceStart + getModel().getSubSequenceLength() - getModel().getConvolutionSize() - 1 + convolutionIndex];
                }
                subSequence.add(value);
            }

            getModel().getProjectionBuilder().append(Doubles.toArray(subSequence));
        }

        getModel().setRawSubSequence(subSequence);
        val newUnusedRecords = new double[getModel().getSubSequenceLength() - 1];
        System.arraycopy(getModel().getUnusedRecords(), getModel().getUnusedRecords().length - newUnusedRecords.length, newUnusedRecords, 0, newUnusedRecords.length);
        getModel().setUnusedRecords(newUnusedRecords);
    }

    private List<Double> createFirstSubSequence()
    {
        val unusedRecords = getModel().getUnusedRecords();
        val vectorLength = getModel().getSubSequenceLength() - getModel().getConvolutionSize();
        val vector = new ArrayList<Double>(vectorLength);
        for (var vectorIndex = 0; vectorIndex < vectorLength; ++vectorIndex)
        {
            var value = 0.0d;
            for (var convolutionIndex = 0; convolutionIndex < getModel().getConvolutionSize(); ++convolutionIndex)
            {
                value += unusedRecords[vectorIndex + convolutionIndex];
            }
            vector.add(value);
        }

        return vector;
    }

    private void whenFinished(DataReceiver receiver)
    {
        val projection = getModel().getProjectionBuilder().build();

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

        isReadyToBeTerminated();
    }
}
