package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class PCARootActorModel extends AbstractProtocolParticipantModel
{
    @Getter @Setter
    private ActorRef calculator;
    @Getter @Setter
    private ActorRef coordinator;
}
