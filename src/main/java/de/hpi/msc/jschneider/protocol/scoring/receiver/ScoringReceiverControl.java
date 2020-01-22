package de.hpi.msc.jschneider.protocol.scoring.receiver;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.FloatsSink;
import lombok.val;

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
            getModel().setRunningDataTransfers(message.getSubSequenceResponsibilities().keySet());
            getModel().setSubSequenceResponsibilities(message.getSubSequenceResponsibilities());
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
            val sink = new FloatsSink();
            dataReceiver.setState(message.getSender().path().root());
            return dataReceiver.addSink(sink)
                               .whenFinished(this::onPathScoresTransferFinished);
        });
    }

    private void onPathScoresTransferFinished(DataReceiver dataReceiver)
    {
        assert dataReceiver.getState() instanceof RootActorPath : "The data receiver state should be a RootActorPath!";
        val workerSystem = (RootActorPath) dataReceiver.getState();

        val floatsSink = dataReceiver.getDataSinks().stream().filter(sink -> sink instanceof FloatsSink).findFirst();
        assert floatsSink.isPresent() : "The data receiver should have a FloatsSink!";

        getModel().getPathScores().put(workerSystem, ((FloatsSink) floatsSink.get()).getFloats());
        getModel().getRunningDataTransfers().remove(workerSystem);

        calculateRunningMean();
    }

    private void calculateRunningMean()
    {
        if (!isReadyToCalculateRunningMean())
        {
            return;
        }


    }

    private boolean isReadyToCalculateRunningMean()
    {
        return getModel().isResponsibilitiesReceived() &&
               getModel().getRunningDataTransfers().isEmpty();
    }
}
