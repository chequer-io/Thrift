// Licensed to the Apache Software Foundation(ASF) under one
// or more contributor license agreements.See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocol.Sasl;

namespace Thrift.Transport
{
    //TODO: think about client info 
    // ReSharper disable once InconsistentNaming
    public abstract class TTransport : IDisposable
    {
        public byte[] PeekBuffer { get; set; }

        private bool _hasPeekByte => PeekBuffer is { Length: > 0 };

        public abstract bool IsOpen { get; }

        public abstract TConfiguration Configuration { get; }

        public abstract void UpdateKnownMessageSize(long size);

        public abstract void CheckReadBytesAvailable(long numBytes);

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public async ValueTask<bool> PeekAsync(CancellationToken cancellationToken)
        {
            return await PeekAsync(1, cancellationToken);
        }

        public async ValueTask<bool> PeekAsync(int length, CancellationToken cancellationToken)
        {
            //If we already have a byte read but not consumed, do nothing.
            if (_hasPeekByte)
            {
                return true;
            }

            //If transport closed we can't peek.
            if (!IsOpen)
            {
                return false;
            }

            //Try to read one byte. If succeeds we will need to store it for the next read.
            try
            {
                PeekBuffer = new byte[length];
                var bytes = await ReadAsync(PeekBuffer, 0, length, cancellationToken);

                if (bytes == 0)
                    return false;
            }
            catch (IOException)
            {
                return false;
            }

            return true;
        }

        public virtual async ValueTask<bool> HasSaslRequestAsync(CancellationToken cancellationToken = default)
        {
            if (!await PeekAsync(5, cancellationToken))
                return false;

            return Enum.IsDefined(typeof(NegotiationStatus), (int)PeekBuffer[0]);
        }

        public abstract Task OpenAsync(CancellationToken cancellationToken = default);

        public abstract void Close();

        protected static void ValidateBufferArgs(byte[] buffer, int offset, int length)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

#if DEBUG // let it fail with OutOfRange in RELEASE mode
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(offset), "Buffer offset must be >= 0");
            }

            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length), "Buffer length must be >= 0");
            }

            if (offset + length > buffer.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(buffer), "Not enough data");
            }
#endif
        }

        public abstract ValueTask<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken);

        public virtual async ValueTask<int> ReadAllAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ValidateBufferArgs(buffer, offset, length);

            if (length <= 0)
                return 0;

            // If we previously peeked a byte, we need to use that first.
            var totalBytes = 0;

            if (_hasPeekByte)
            {
                while (PeekBuffer.Length > 0)
                {
                    buffer[offset++] = PeekBuffer[0];
                    ++totalBytes;
                    PeekBuffer = PeekBuffer.Length > 1 ? PeekBuffer[1..] : Array.Empty<byte>();

                    if (length == totalBytes)
                        return totalBytes;
                }
            }

            var remaining = length - totalBytes;
            Debug.Assert(remaining > 0); // any other possible cases should have been handled already 

            while (true)
            {
                var numBytes = await ReadAsync(buffer, offset, remaining, cancellationToken);
                totalBytes += numBytes;

                if (totalBytes >= length)
                    return totalBytes; // we're done

                if (numBytes <= 0)
                    throw new TTransportException(
                        TTransportException.ExceptionType.EndOfFile,
                        "Cannot read, Remote side has closed"
                    );

                remaining -= numBytes;
                offset += numBytes;
            }
        }

        public virtual async Task WriteAsync(byte[] buffer, CancellationToken cancellationToken)
        {
            await WriteAsync(buffer, 0, buffer.Length, CancellationToken.None);
        }

        public virtual async Task WriteAsync(byte[] buffer, int offset, int length)
        {
            await WriteAsync(buffer, offset, length, CancellationToken.None);
        }

        public abstract Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken);

        public abstract Task FlushAsync(CancellationToken cancellationToken);

        public virtual Task FlushAsync(bool isSasl, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException();
        }

        protected abstract void Dispose(bool disposing);
    }
}
