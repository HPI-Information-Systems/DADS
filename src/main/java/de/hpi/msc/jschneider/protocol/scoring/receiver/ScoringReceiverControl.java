package de.hpi.msc.jschneider.protocol.scoring.receiver;

import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.fileHandling.writing.ClearSequenceWriter;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.scoring.ScoringEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.DoublesSink;
import lombok.val;

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

        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onResponsibilitiesReceived)
                    .match(ScoringMessages.InitializePathScoresTransferMessage.class, this::onInitializePathScoresTransfer);
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

        storeResults();
    }

    private void storeResults()
    {
        if (!isReadyToStoreResults())
        {
            return;
        }

        assert SystemParameters.getCommand() instanceof MasterCommand : "Only the master processor can store the results!";
        trySendEvent(ProtocolType.Scoring, eventDispatcher -> ScoringEvents.ReadyForTerminationEvent.builder()
                                                                                                    .sender(getModel().getSelf())
                                                                                                    .receiver(eventDispatcher)
                                                                                                    .build());

        val filePath = ((MasterCommand) SystemParameters.getCommand()).getOutputFilePath();
        val writer = ClearSequenceWriter.fromFile(filePath.toFile());
        val numberOfPathScores = new Counter(0L);

        for (val scores : getModel().getPathScores().entrySet().stream()
                                    .sorted(Comparator.comparingLong(entry -> getModel().getSubSequenceResponsibilities().get(entry.getKey()).getFrom()))
                                    .map(Map.Entry::getValue)
                                    .toArray(double[][]::new))
        {
            writer.write(scores);
            numberOfPathScores.increment(scores.length);
        }

        getLog().info("================================================================================================");
        getLog().info("================================================================================================");
        getLog().info(String.format("$1%d results written to %2$s.", numberOfPathScores.get(), filePath.toString()));
        getLog().info("================================================================================================");
        getLog().info("================================================================================================");

        writer.close();
    }

    private boolean isReadyToStoreResults()
    {
        return getModel().isResponsibilitiesReceived()
               && getModel().getRunningDataTransfers().isEmpty();
    }
}
