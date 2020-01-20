package de.hpi.msc.jschneider.utility.dataTransfer.source;

@FunctionalInterface
public interface Serializer<TData>
{
    byte[] serialize(TData data, long from, long to);
}
