package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

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
            getModel().getNextSubSequenceIndex().set(message.getFirstSubSequenceIndex());
            getModel().setSubSequenceLength(message.getSubSequenceLength());

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
            trySendEvent(ProtocolType.SequenceSliceDistribution, eventDispatcher -> SequenceSliceDistributionEvents.SequenceSlicePartReceivedEvent.builder()
                                                                                                                                                  .sender(getModel().getSelf())
                                                                                                                                                  .receiver(eventDispatcher)
                                                                                                                                                  .slicePart(message.getSlicePart())
                                                                                                                                                  .build());

            if (message.isLastPart())
            {
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
}
