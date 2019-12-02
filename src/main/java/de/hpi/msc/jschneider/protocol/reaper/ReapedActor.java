package de.hpi.msc.jschneider.protocol.reaper;

import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;

public class ReapedActor<TModel extends ProtocolParticipantModel, TControl extends ProtocolParticipantControl<TModel>> extends ProtocolParticipant<TModel, TControl>
{
    public static <TModel extends ProtocolParticipantModel, TControl extends ProtocolParticipantControl<TModel>> Props props(TControl control)
    {
        return Props.create(ReapedActor.class, () -> new ReapedActor(control));
    }

    protected ReapedActor(TControl control)
    {
        super(control);
    }

    @Override
    public void preStart() throws Exception
    {
        super.preStart();
        getControl().getLocalProtocol(ProtocolType.Reaper).ifPresent(protocol ->
                                                                     {
                                                                         getControl().send(ReaperMessages.WatchMeMessage.builder()
                                                                                                                        .sender(getSelf())
                                                                                                                        .receiver(protocol.getRootActor())
                                                                                                                        .build());
                                                                     });
    }
}
