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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocol.Entities;
using Thrift.Transport;

namespace Thrift.Protocol
{
    // ReSharper disable once InconsistentNaming
    public abstract class TProtocol : IDisposable
    {
        private bool _isDisposed;
        protected int RecursionDepth;

        protected readonly TTransport Trans;

        protected static readonly TStruct AnonymousStruct = new(string.Empty);
        protected static readonly TField StopField = new() { Type = TType.Stop };

        protected TProtocol(TTransport trans)
        {
            Trans = trans;
            RecursionLimit = trans.Configuration.RecursionLimit;
            RecursionDepth = 0;
        }

        public TTransport Transport => Trans;

        protected int RecursionLimit { get; }

        public void Dispose()
        {
            Dispose(true);
        }

        public void IncrementRecursionDepth()
        {
            if (RecursionDepth < RecursionLimit)
            {
                ++RecursionDepth;
            }
            else
            {
                throw new TProtocolException(TProtocolException.DEPTH_LIMIT, "Depth limit exceeded");
            }
        }

        public void DecrementRecursionDepth()
        {
            --RecursionDepth;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    Trans?.Dispose();
                }
            }

            _isDisposed = true;
        }

        protected void CheckReadBytesAvailable(TSet set)
        {
            Transport.CheckReadBytesAvailable(set.Count * GetMinSerializedSize(set.ElementType));
        }

        protected void CheckReadBytesAvailable(TList list)
        {
            Transport.CheckReadBytesAvailable(list.Count * GetMinSerializedSize(list.ElementType));
        }

        protected void CheckReadBytesAvailable(TMap map)
        {
            var elmSize = GetMinSerializedSize(map.KeyType) + GetMinSerializedSize(map.ValueType);
            Transport.CheckReadBytesAvailable(map.Count * elmSize);
        }

        // Returns the minimum amount of bytes needed to store the smallest possible instance of TType.
        public abstract int GetMinSerializedSize(TType type);

        public abstract Task WriteMessageBeginAsync(TMessage message, CancellationToken cancellationToken = default);

        public abstract Task WriteMessageEndAsync(CancellationToken cancellationToken = default);

        public abstract Task WriteStructBeginAsync(TStruct @struct, CancellationToken cancellationToken = default);

        public abstract Task WriteStructEndAsync(CancellationToken cancellationToken = default);

        public abstract Task WriteFieldBeginAsync(TField field, CancellationToken cancellationToken = default);

        public abstract Task WriteFieldEndAsync(CancellationToken cancellationToken = default);

        public abstract Task WriteFieldStopAsync(CancellationToken cancellationToken = default);

        public abstract Task WriteMapBeginAsync(TMap map, CancellationToken cancellationToken = default);

        public abstract Task WriteMapEndAsync(CancellationToken cancellationToken = default);

        public abstract Task WriteListBeginAsync(TList list, CancellationToken cancellationToken = default);

        public abstract Task WriteListEndAsync(CancellationToken cancellationToken = default);

        public abstract Task WriteSetBeginAsync(TSet set, CancellationToken cancellationToken = default);

        public abstract Task WriteSetEndAsync(CancellationToken cancellationToken = default);

        public abstract Task WriteBoolAsync(bool b, CancellationToken cancellationToken = default);

        public abstract Task WriteByteAsync(sbyte b, CancellationToken cancellationToken = default);

        public abstract Task WriteI16Async(short i16, CancellationToken cancellationToken = default);

        public abstract Task WriteI32Async(int i32, CancellationToken cancellationToken = default);

        public abstract Task WriteI64Async(long i64, CancellationToken cancellationToken = default);

        public abstract Task WriteDoubleAsync(double d, CancellationToken cancellationToken = default);

        public virtual async Task WriteStringAsync(string s, CancellationToken cancellationToken = default)
        {
            var bytes = Encoding.UTF8.GetBytes(s);
            await WriteBinaryAsync(bytes, cancellationToken);
        }

        public abstract Task WriteBinaryAsync(byte[] bytes, CancellationToken cancellationToken = default);

        public abstract ValueTask<TMessage> ReadMessageBeginAsync(CancellationToken cancellationToken = default);

        public abstract Task ReadMessageEndAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<TStruct> ReadStructBeginAsync(CancellationToken cancellationToken = default);

        public abstract Task ReadStructEndAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<TField> ReadFieldBeginAsync(CancellationToken cancellationToken = default);

        public abstract Task ReadFieldEndAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<TMap> ReadMapBeginAsync(CancellationToken cancellationToken = default);

        public abstract Task ReadMapEndAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<TList> ReadListBeginAsync(CancellationToken cancellationToken = default);

        public abstract Task ReadListEndAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<TSet> ReadSetBeginAsync(CancellationToken cancellationToken = default);

        public abstract Task ReadSetEndAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<bool> ReadBoolAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<sbyte> ReadByteAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<short> ReadI16Async(CancellationToken cancellationToken = default);

        public abstract ValueTask<int> ReadI32Async(CancellationToken cancellationToken = default);

        public abstract ValueTask<long> ReadI64Async(CancellationToken cancellationToken = default);

        public abstract ValueTask<double> ReadDoubleAsync(CancellationToken cancellationToken = default);

        public virtual async ValueTask<string> ReadStringAsync(CancellationToken cancellationToken = default)
        {
            var buf = await ReadBinaryAsync(cancellationToken);
            return Encoding.UTF8.GetString(buf, 0, buf.Length);
        }

        public virtual async ValueTask<string> ReadStringAsync(int length, CancellationToken cancellationToken = default)
        {
            var buf = new byte[length];
            await Trans.ReadAllAsync(buf, 0, length, cancellationToken);

            return Encoding.UTF8.GetString(buf, 0, buf.Length);
        }

        public abstract ValueTask<byte[]> ReadBinaryAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<byte[]> ReadSaslHeaderAsync(CancellationToken cancellationToken = default);

        public abstract ValueTask<(string username, string password)> ReadSaslLDAPAuthenticationInfoAsync(int packetLength, CancellationToken cancellationToken = default);

        public virtual async ValueTask<bool> PeekAsync(CancellationToken cancellationToken = default)
        {
            return await Transport.PeekAsync(cancellationToken);
        }

        public virtual ValueTask<bool> HasSaslRequestAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return new ValueTask<bool>(false);
        }
    }
}
