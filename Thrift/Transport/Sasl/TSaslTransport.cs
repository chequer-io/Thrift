using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Thrift.Transport.Sasl
{
    public class TSaslTransport : TEndpointTransport
    {
        private bool _isDisposed;

        private byte[] _dest = Array.Empty<byte>();

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

            return await InputStream.ReadAsync(new Memory<byte>(buffer, offset, length), cancellationToken);
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
