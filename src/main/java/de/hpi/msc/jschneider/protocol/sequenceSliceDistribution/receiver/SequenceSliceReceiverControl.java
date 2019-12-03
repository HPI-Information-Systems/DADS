package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.var;

import java.util.function.Consumer;

public class SequenceSliceReceiverControl extends AbstractProtocolParticipantControl<SequenceSliceReceiverModel>
{
    public SequenceSliceReceiverControl(SequenceSliceReceiverModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class, this::onSlicePart);
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

            getModel().getSliceParts().put(message.getPartIndex(), message.getSlicePart());
            forNextSlices(slicePart ->
                          {
                              getModel().getSequenceWriter().write(slicePart);
                              sendEvent(ProtocolType.SequenceSliceDistribution, SequenceSliceDistributionEvents.SequenceSlicePartReceivedEvent.builder()
                                                                                                                                              .slicePart(slicePart)
                                                                                                                                              .build());
                          });

        }
        finally
        {
            send(SequenceSliceDistributionMessages.AcknowledgeSequenceSlicePartMessage.builder()
                                                                                      .sender(getModel().getSelf())
                                                                                      .receiver(message.getSender())
                                                                                      .build());
            complete(message);
        }
    }

    private void forNextSlices(Consumer<float[]> callback)
    {
        var slicePartIndex = getModel().getExpectedNextSliceIndex().get();
        var slicePart = getModel().getSliceParts().get(slicePartIndex);
        while (slicePart != null)
        {
            callback.accept(slicePart);
            getModel().getSliceParts().remove(slicePartIndex);
            slicePartIndex++;
            slicePart = getModel().getSliceParts().get(slicePartIndex);
        }

        getModel().getExpectedNextSliceIndex().set(slicePartIndex);
    }
}
