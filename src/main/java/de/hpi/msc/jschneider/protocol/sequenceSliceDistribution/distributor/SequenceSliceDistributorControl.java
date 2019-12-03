package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

public class SequenceSliceDistributorControl extends AbstractProtocolParticipantControl<SequenceSliceDistributorModel>
{
    public SequenceSliceDistributorControl(SequenceSliceDistributorModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();
        sendInitialSlicePart();
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(SequenceSliceDistributionMessages.AcknowledgeSequenceSlicePartMessage.class, this::onAcknowledgeSlicePart);
    }

    private void onAcknowledgeSlicePart(SequenceSliceDistributionMessages.AcknowledgeSequenceSlicePartMessage message)
    {
        try
        {
            sendNextSlicePart(message.getSender());
        }
        finally
        {
            complete(message);
        }
    }

    private void sendInitialSlicePart()
    {
        val protocol = getProtocol(getModel().getSliceReceiverActorSystem(), ProtocolType.SequenceSliceDistribution);
        if (!protocol.isPresent())
        {
            getLog().error(String.format("Unable to send initial slice part to %1$s, because the receiver does not provide the SequenceSliceDistribution protocol!",
                                         getModel().getSliceReceiverActorSystem()));
            return;
        }

        sendNextSlicePart(protocol.get().getRootActor());
    }

    private void sendNextSlicePart(ActorRef receiver)
    {
        val index = getModel().getNextSliceIndex().get();
        val startIndex = getModel().getNextSliceStartIndex().get();
        val length = (int) Math.min(getModel().getSliceSizeFactor() * getModel().getMaximumMessageSize() / Float.BYTES,
                                    getModel().getSequenceReader().getSize() - startIndex);
        val records = getModel().getSequenceReader().read(startIndex, length);
        val isLast = startIndex + records.length >= getModel().getSequenceReader().getSize();

        val message = SequenceSliceDistributionMessages.SequenceSlicePartMessage.builder()
                                                                                .sender(getModel().getSelf())
                                                                                .receiver(receiver)
                                                                                .partIndex(index)
                                                                                .slicePart(records)
                                                                                .isLastPart(isLast)
                                                                                .build();
        send(message);

        getModel().getNextSliceIndex().set(index + 1);
        getModel().getNextSliceStartIndex().set(startIndex + records.length);
    }
}
