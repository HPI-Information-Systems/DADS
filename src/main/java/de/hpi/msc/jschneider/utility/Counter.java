package de.hpi.msc.jschneider.utility;

public class Counter
{
    private long value;

    public Counter(long initialValue)
    {
        value = initialValue;
    }

    public long get()
    {
        return value;
    }

    public void increment()
    {
        value += 1;
    }

    public long getAndIncrement()
    {
        increment();
        return value - 1;
    }

    public long incrementAndGet()
    {
        increment();
        return get();
    }
}
