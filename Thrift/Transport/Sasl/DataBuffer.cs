using System;
using System.Buffers;

namespace Thrift.Transport.Sasl;

public class DataBuffer : IDisposable
{
    public Memory<byte> Buffer => _buffer;

    public int Remaining => _length - _position;

    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(1024);
    private int _position;
    private int _length;

    public void EnsureSize(int size)
    {
        if (_buffer.Length < size)
        {
            var newBuffer = ArrayPool<byte>.Shared.Rent(size);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = newBuffer;
        }

        _length = size;
        _position = 0;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        ArrayPool<byte>.Shared.Return(_buffer);
    }

    public int Read(Memory<byte> buffer, int count)
    {
        if (Remaining is 0)
            return 0;

        var bytesToRead = Math.Min(count, Remaining);
        _buffer.AsSpan(_position, bytesToRead).CopyTo(buffer.Span);
        _position += bytesToRead;

        return bytesToRead;
    }
}
