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

        val protocol = getProtocol(getModel().getSliceReceiverActorSystem(), ProtocolType.SequenceSliceDistribution);
        if (!protocol.isPresent())
        {
            getLog().error(String.format("Unable to initialize slice part transfer to %1$s, because the receiver does not provide the SequenceSliceDistribution protocol!",
                                         getModel().getSliceReceiverActorSystem()));
            return;
        }

        send(SequenceSliceDistributionMessages.InitializeSliceTransferMessage.builder()
                                                                             .sender(getModel().getSelf())
                                                                             .receiver(protocol.get().getRootActor())
                                                                             .subSequenceLength(getModel().getSubSequenceLength())
                                                                             .convolutionSize(getModel().getConvolutionSize())
                                                                             .firstSubSequenceIndex(getModel().getFirstSubSequenceIndex())
                                                                             .build());
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(SequenceSliceDistributionMessages.RequestNextSlicePartMessage.class, this::onRequestNextSlicePart);
    }

    private void onRequestNextSlicePart(SequenceSliceDistributionMessages.RequestNextSlicePartMessage message)
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

    private void sendNextSlicePart(ActorRef receiver)
    {
        val readLength = (int) Math.min(getModel().getSliceSizeFactor() * getModel().getMaximumMessageSize() / Float.BYTES,
                                        getModel().getSequenceReader().getSize() - getModel().getSequenceReader().getPosition());

        if (readLength < 1)
        {
            return;
        }

        val records = getModel().getSequenceReader().read(readLength);
        val isLast = getModel().getSequenceReader().isAtEnd();

        getLog().info(String.format("Sending sequence slice part (length = %1$d, isLast = %2$s) to %3$s.",
                                    records.length,
                                    isLast,
                                    receiver.path()));

        val message = SequenceSliceDistributionMessages.SequenceSlicePartMessage.builder()
                                                                                .sender(getModel().getSelf())
                                                                                .receiver(receiver)
                                                                                .slicePart(records)
                                                                                .isLastPart(isLast)
                                                                                .build();
        send(message);

        if (isLast)
        {
            getLog().info(String.format("Done sending sequence slice parts to %1$s.", receiver.path()));
        }
    }
}
