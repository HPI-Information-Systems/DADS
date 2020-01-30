package de.hpi.msc.jschneider.utility;

import lombok.val;

public class Counter
{
    private long value;
    private final long minimum;
    private final long maximum;

    public Counter(long initialValue)
    {
        this(initialValue, 0L, Long.MAX_VALUE);
    }

    public Counter(long initialValue, long minValue, long maxValue)
    {
        minimum = minValue;
        maximum = maxValue;

        set(initialValue);
    }

    public long get()
    {
        return value;
    }

    public void set(long value)
    {
        this.value = Math.min(maximum, Math.max(minimum, value));
    }

    public void increment(long value)
    {
        set(get() + value);
    }

    public void increment()
    {
        increment(1L);
    }

    public void decrement(long value)
    {
        set(get() - value);
    }

    public void decrement()
    {
        decrement(1L);
    }

    public long getAndIncrement(long value)
    {
        val result = get();
        set(get() + value);
        return result;
    }

    public long incrementAndGet(long value)
    {
        set(get() + value);
        return get();
    }

    public long getAndDecrement(long value)
    {
        val result = get();
        set(get() - value);
        return result;
    }

    public long decrementAndGet(long value)
    {
        set(get() - value);
        return get();
    }

    public long getAndIncrement()
    {
        return getAndIncrement(1L);
    }

    public long incrementAndGet()
    {
        return incrementAndGet(1L);
    }

    public long getAndDecrement()
    {
        return getAndDecrement(1L);
    }

    public long decrementAndGet()
    {
        return decrementAndGet(1L);
    }
}
