package de.hpi.msc.jschneider.protocol.nodeCreation.worker.intersectionCalculator;

import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerControl;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import lombok.val;

public class IntersectionCalculator implements WorkConsumer
{
    @Override
    public void process(ActorPoolWorkerControl control, ActorPoolMessages.WorkMessage workLoad)
    {
        assert workLoad instanceof IntersectionCalculatorMessages.CalculateIntersectionsMessage : "Unexpected workload!";
        process(control, (IntersectionCalculatorMessages.CalculateIntersectionsMessage) workLoad);
    }

    private void process(ActorPoolWorkerControl control, IntersectionCalculatorMessages.CalculateIntersectionsMessage message)
    {
        val intersections = Calculate.intersections(message.getProjectionChunk(), message.getIntersectionPoints(), message.getFirstSubSequenceIndex());
        control.send(IntersectionCalculatorMessages.IntersectionsCalculatedMessage.builder()
                                                                                  .sender(control.getModel().getSelf())
                                                                                  .receiver(message.getSender())
                                                                                  .intersectionCollections(intersections)
                                                                                  .build());
    }
}
