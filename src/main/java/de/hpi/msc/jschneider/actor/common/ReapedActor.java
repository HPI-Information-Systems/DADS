package de.hpi.msc.jschneider.actor.common;

import de.hpi.msc.jschneider.actor.common.reaper.Reaper;

public abstract class ReapedActor<TController extends BaseActorController> extends BaseActor<TController>
{
    @Override
    public void preStart() throws Exception
    {
        super.preStart();
        Reaper.watch(this);
    }
}
