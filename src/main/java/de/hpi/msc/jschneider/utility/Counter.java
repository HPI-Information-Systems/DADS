package de.hpi.msc.jschneider.utility;

import lombok.val;

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

    public void decrement() { value = Math.max(0L, value - 1);}

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

    public long getAndDecrement()
    {
        val value = get();
        decrement();
        return value;
    }

    public long decrementAndGet()
    {
        decrement();
        return get();
    }
}
