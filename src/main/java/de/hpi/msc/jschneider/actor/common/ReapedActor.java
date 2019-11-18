package de.hpi.msc.jschneider.actor.common;

public abstract class ReapedActor extends BaseActor
{
    @Override
    public void preStart() throws Exception
    {
        super.preStart();
        Reaper.watch(this);
    }
}
