package de.hpi.msc.jschneider.protocol.scoring.receiver;

import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.fileHandling.writing.ClearSequenceWriter;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.DoublesSink;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class ScoringReceiverControl extends AbstractProtocolParticipantControl<ScoringReceiverModel>
{
    public ScoringReceiverControl(ScoringReceiverModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.SequenceSliceDistribution, SequenceSliceDistributionEvents.SubSequenceParametersReceivedEvent.class);
        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(SequenceSliceDistributionEvents.SubSequenceParametersReceivedEvent.class, this::onSubSequenceParametersReceived)
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onResponsibilitiesReceived)
                    .match(ScoringMessages.InitializePathScoresTransferMessage.class, this::onInitializePathScoresTransfer);
    }

    private void onSubSequenceParametersReceived(SequenceSliceDistributionEvents.SubSequenceParametersReceivedEvent message)
    {
        try
        {
            assert getModel().getSubSequenceLength() == 0 : "Length of sub sequences was received already!";

            getModel().setSubSequenceLength(message.getSubSequenceLength());
            calculateRunningMean();
        }
        finally
        {
            complete(message);
        }
    }

    private void onResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            assert !getModel().isResponsibilitiesReceived() : "Responsibilities received already!";

            getModel().setResponsibilitiesReceived(true);
            getModel().getRunningDataTransfers().addAll(message.getSubSequenceResponsibilities().keySet());
            getModel().setSubSequenceResponsibilities(new HashMap<>(message.getSubSequenceResponsibilities()));
        }
        finally
        {
            complete(message);
        }
    }

    private void onInitializePathScoresTransfer(ScoringMessages.InitializePathScoresTransferMessage message)
    {
        assert getModel().isResponsibilitiesReceived() : "Responsibilities were not received yet!";

        getModel().getDataTransferManager().accept(message, dataReceiver ->
        {
            val sink = new DoublesSink();
            dataReceiver.setState(ProcessorId.of(message.getSender()));
            return dataReceiver.addSink(sink)
                               .whenFinished(this::onPathScoresTransferFinished);
        });
    }

    private void onPathScoresTransferFinished(DataReceiver dataReceiver)
    {
        assert dataReceiver.getState() instanceof ProcessorId : "The data receiver state should be a ProcessorId!";
        val workerSystem = (ProcessorId) dataReceiver.getState();

        val doublesSink = dataReceiver.getDataSinks().stream().filter(sink -> sink instanceof DoublesSink).findFirst();
        assert doublesSink.isPresent() : "The data receiver should have a DoublesSink!";

        getModel().getPathScores().put(workerSystem, ((DoublesSink) doublesSink.get()).getDoubles());
        getModel().getRunningDataTransfers().remove(workerSystem);

        calculateRunningMean();
    }

    private void calculateRunningMean()
    {
        if (!isReadyToCalculateRunningMean())
        {
            return;
        }

        val sortedScores = Doubles.concat(getModel().getPathScores().entrySet()
                                                    .stream()
                                                    .sorted(Comparator.comparingLong(entry -> getModel().getSubSequenceResponsibilities().get(entry.getKey()).getFrom()))
                                                    .map(Map.Entry::getValue)
                                                    .toArray(double[][]::new));

        val minimumPathScore = Doubles.max(sortedScores) * -1.0f;
        val maximumPathScore = Doubles.min(sortedScores) * -1.0f;

        val normalizedScores = new double[sortedScores.length];
        val scoreRange = maximumPathScore - minimumPathScore;
        for (var scoreIndex = 0; scoreIndex < normalizedScores.length; ++scoreIndex)
        {
            normalizedScores[scoreIndex] = (-sortedScores[scoreIndex] - minimumPathScore) / scoreRange;
        }

        var runningMean = 0.0d;
        val runningMeans = new ArrayList<Double>();
        final double windowSizeAsDouble = getModel().getSubSequenceLength();
        for (var runningMeanStartIndex = 0; runningMeanStartIndex <= normalizedScores.length - getModel().getSubSequenceLength(); ++runningMeanStartIndex)
        {
            if (runningMeans.isEmpty())
            {
                for (var windowIndex = 0; windowIndex < getModel().getSubSequenceLength(); ++windowIndex)
                {
                    runningMean += normalizedScores[runningMeanStartIndex + windowIndex] / windowSizeAsDouble;
                }
            }
            else
            {
                runningMean -= normalizedScores[runningMeanStartIndex - 1] / windowSizeAsDouble;
                runningMean += normalizedScores[runningMeanStartIndex + getModel().getSubSequenceLength() - 1] / windowSizeAsDouble;
            }

            runningMeans.add(runningMean);
        }

        storeResults(Doubles.toArray(runningMeans));
    }

    private boolean isReadyToCalculateRunningMean()
    {
        return getModel().getSubSequenceLength() > 0
               && getModel().isResponsibilitiesReceived()
               && getModel().getRunningDataTransfers().isEmpty();
    }

    private void storeResults(double[] normalityScores)
    {
        assert SystemParameters.getCommand() instanceof MasterCommand : "Only the master processor can store the results!";
        val filePath = ((MasterCommand) SystemParameters.getCommand()).getOutputFilePath();

        getLog().info("================================================================================================");
        getLog().info("================================================================================================");
        getLog().info(String.format("Writing %1$d results to %2$s.", normalityScores.length, filePath.toString()));
        getLog().info("================================================================================================");
        getLog().info("================================================================================================");

        val writer = ClearSequenceWriter.fromFile(filePath.toFile());
        writer.write(normalityScores);
        writer.close();

        trySendEvent(ProtocolType.Scoring, eventDispatcher -> ScoringEvents.ReadyForTerminationEvent.builder()
                                                                                                    .sender(getModel().getSelf())
                                                                                                    .receiver(eventDispatcher)
                                                                                                    .build());
    }
}
