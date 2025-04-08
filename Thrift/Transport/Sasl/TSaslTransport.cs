using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Thrift.Transport.Sasl
{
    public class TSaslTransport : TEndpointTransport
    {
        private bool _isDisposed;

        private byte[] _lengthBuffer = new byte[4];
        private byte[] _dest = Array.Empty<byte>();
        private readonly DataBuffer _dataBuffer = new();

        private int _lastIndex;

        protected TSaslTransport(TConfiguration config)
            : base(config)
        {
        }

        public TSaslTransport(Stream inputStream, Stream outputStream, TConfiguration config)
            : base(config)
        {
            InputStream = inputStream;
            OutputStream = outputStream;
        }

        protected Stream OutputStream { get; set; }

        private Stream _InputStream;

        protected Stream InputStream
        {
            get => _InputStream;
            set
            {
                _InputStream = value;
                ResetConsumedMessageSize();
            }
        }

        public override bool IsOpen => true;

        public override Task OpenAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override void Close()
        {
            if (InputStream != null)
            {
                InputStream.Dispose();
                InputStream = null;
            }

            if (OutputStream != null)
            {
                OutputStream.Dispose();
                OutputStream = null;
            }
        }

        public override async ValueTask<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            if (InputStream == null)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen,
                    "Cannot read from null inputstream");
            }

            if (InputStream == null)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen,
                    "Cannot read from null inputstream");
            }

            var memory = new Memory<byte>(buffer, offset, length);
            var readsCount = _dataBuffer.Read(memory, length);

            if (readsCount > 0)
                return readsCount;

            // Read Frame (4 bytes)
            await InputStream.ReadExactlyAsync(_lengthBuffer, cancellationToken);
            var frameLength = BinaryPrimitives.ReadInt32BigEndian(_lengthBuffer);

            // DataBuffer Size Ensure
            _dataBuffer.EnsureSize(frameLength);
            await InputStream.ReadExactlyAsync(_dataBuffer.Buffer[..frameLength], cancellationToken);

            // Fill Data to Buffer
            var bytesRead = _dataBuffer.Read(memory, length);
            return bytesRead;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            if (OutputStream == null)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen,
                    "Cannot write to null outputstream");
            }

            Array.Resize(ref _dest, _dest.Length + length);

            buffer[offset..length].CopyTo(_dest, _lastIndex == 0 ? 0 : _lastIndex + 1);

            _lastIndex = _dest.Length - 1;

            return Task.CompletedTask;
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            byte[] length = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(length, _dest.Length);

            await OutputStream.WriteAsync(length, cancellationToken);

            await OutputStream.WriteAsync(_dest, cancellationToken);
            await OutputStream.FlushAsync(cancellationToken);
            _dest = Array.Empty<byte>();
            _lastIndex = 0;
            ResetConsumedMessageSize();
        }

        public override async Task FlushAsync(bool isSasl, CancellationToken cancellationToken)
        {
            if (!isSasl)
            {
                byte[] length = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(length, _dest.Length);

                await OutputStream.WriteAsync(length, cancellationToken);
            }

            await OutputStream.WriteAsync(_dest, cancellationToken);
            await OutputStream.FlushAsync(cancellationToken);
            _dest = Array.Empty<byte>();
            _lastIndex = 0;
            ResetConsumedMessageSize();
        }

        // IDisposable
        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    InputStream?.Dispose();
                    OutputStream?.Dispose();
                }
            }

            _isDisposed = true;
        }
    }
}
