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
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocol.Entities;
using Thrift.Protocol.Sasl;
using Thrift.Transport;

#pragma warning disable IDE0079 // unnecessary suppression
#pragma warning disable IDE0066 // use switch expression

namespace Thrift.Protocol
{
    // ReSharper disable once InconsistentNaming
    public class TBinaryProtocol : TProtocol
    {
        protected const uint VersionMask = 0xffff0000;
        protected const uint Version1 = 0x80010000;

        protected readonly bool StrictRead;
        protected readonly bool StrictWrite;

        // minimize memory allocations by means of an preallocated bytes buffer
        // The value of 128 is arbitrarily chosen, the required minimum size must be sizeof(long)
        private readonly byte[] PreAllocatedBuffer = new byte[128];

        public TBinaryProtocol(TTransport trans)
            : this(trans, false, true)
        {
        }

        public TBinaryProtocol(TTransport trans, bool strictRead, bool strictWrite)
            : base(trans)
        {
            StrictRead = strictRead;
            StrictWrite = strictWrite;
        }

        public override async Task WriteMessageBeginAsync(TMessage message, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (StrictWrite)
            {
                var version = Version1 | (uint)message.Type;
                await WriteI32Async((int)version, cancellationToken);
                await WriteStringAsync(message.Name, cancellationToken);
                await WriteI32Async(message.SeqID, cancellationToken);
            }
            else
            {
                await WriteStringAsync(message.Name, cancellationToken);
                await WriteByteAsync((sbyte)message.Type, cancellationToken);
                await WriteI32Async(message.SeqID, cancellationToken);
            }
        }

        public override Task WriteMessageEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override Task WriteStructBeginAsync(TStruct @struct, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override Task WriteStructEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async Task WriteFieldBeginAsync(TField field, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await WriteByteAsync((sbyte)field.Type, cancellationToken);
            await WriteI16Async(field.ID, cancellationToken);
        }

        public override Task WriteFieldEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async Task WriteFieldStopAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await WriteByteAsync((sbyte)TType.Stop, cancellationToken);
        }

        public override async Task WriteMapBeginAsync(TMap map, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            PreAllocatedBuffer[0] = (byte)map.KeyType;
            PreAllocatedBuffer[1] = (byte)map.ValueType;
            await Trans.WriteAsync(PreAllocatedBuffer, 0, 2, cancellationToken);

            await WriteI32Async(map.Count, cancellationToken);
        }

        public override Task WriteMapEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async Task WriteListBeginAsync(TList list, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await WriteByteAsync((sbyte)list.ElementType, cancellationToken);
            await WriteI32Async(list.Count, cancellationToken);
        }

        public override Task WriteListEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async Task WriteSetBeginAsync(TSet set, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await WriteByteAsync((sbyte)set.ElementType, cancellationToken);
            await WriteI32Async(set.Count, cancellationToken);
        }

        public override Task WriteSetEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async Task WriteBoolAsync(bool b, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await WriteByteAsync(b ? (sbyte)1 : (sbyte)0, cancellationToken);
        }

        public override async Task WriteByteAsync(sbyte b, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            PreAllocatedBuffer[0] = (byte)b;

            await Trans.WriteAsync(PreAllocatedBuffer, 0, 1, cancellationToken);
        }

        public override async Task WriteI16Async(short i16, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            BinaryPrimitives.WriteInt16BigEndian(PreAllocatedBuffer, i16);

            await Trans.WriteAsync(PreAllocatedBuffer, 0, 2, cancellationToken);
        }

        public override async Task WriteI32Async(int i32, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            BinaryPrimitives.WriteInt32BigEndian(PreAllocatedBuffer, i32);

            await Trans.WriteAsync(PreAllocatedBuffer, 0, 4, cancellationToken);
        }

        public override async Task WriteI64Async(long i64, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            BinaryPrimitives.WriteInt64BigEndian(PreAllocatedBuffer, i64);

            await Trans.WriteAsync(PreAllocatedBuffer, 0, 8, cancellationToken);
        }

        public override async Task WriteDoubleAsync(double d, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await WriteI64Async(BitConverter.DoubleToInt64Bits(d), cancellationToken);
        }

        public override async Task WriteBinaryAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await WriteI32Async(bytes.Length, cancellationToken);
            await Trans.WriteAsync(bytes, 0, bytes.Length, cancellationToken);
        }

        public override async ValueTask<TMessage> ReadMessageBeginAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var message = new TMessage();
            var size = await ReadI32Async(cancellationToken);

            if (size < 0)
            {
                var version = unchecked((uint)size & VersionMask);

                if (version != Version1)
                {
                    throw new TProtocolException(TProtocolException.BAD_VERSION,
                        $"Bad version in ReadMessageBegin: {version}");
                }

                message.Type = (TMessageType)(size & 0x000000ff);
                message.Name = await ReadStringAsync(cancellationToken);
                message.SeqID = await ReadI32Async(cancellationToken);
            }
            else
            {
                if (StrictRead)
                {
                    throw new TProtocolException(TProtocolException.BAD_VERSION,
                        "Missing version in ReadMessageBegin, old client?");
                }

                message.Name = size > 0 ? await ReadStringBodyAsync(size, cancellationToken) : string.Empty;
                message.Type = (TMessageType)await ReadByteAsync(cancellationToken);
                message.SeqID = await ReadI32Async(cancellationToken);
            }

            return message;
        }

        public override Task ReadMessageEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override ValueTask<TStruct> ReadStructBeginAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<TStruct>(AnonymousStruct);
        }

        public override Task ReadStructEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async ValueTask<TField> ReadFieldBeginAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var type = (TType)await ReadByteAsync(cancellationToken);

            if (type == TType.Stop)
            {
                return StopField;
            }

            return new TField
            {
                Type = type,
                ID = await ReadI16Async(cancellationToken)
            };
        }

        public override Task ReadFieldEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async ValueTask<TMap> ReadMapBeginAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var map = new TMap
            {
                KeyType = (TType)await ReadByteAsync(cancellationToken),
                ValueType = (TType)await ReadByteAsync(cancellationToken),
                Count = await ReadI32Async(cancellationToken)
            };

            CheckReadBytesAvailable(map);
            return map;
        }

        public override Task ReadMapEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async ValueTask<TList> ReadListBeginAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var list = new TList
            {
                ElementType = (TType)await ReadByteAsync(cancellationToken),
                Count = await ReadI32Async(cancellationToken)
            };

            CheckReadBytesAvailable(list);
            return list;
        }

        public override Task ReadListEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async ValueTask<TSet> ReadSetBeginAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var set = new TSet
            {
                ElementType = (TType)await ReadByteAsync(cancellationToken),
                Count = await ReadI32Async(cancellationToken)
            };

            CheckReadBytesAvailable(set);
            return set;
        }

        public override Task ReadSetEndAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override async ValueTask<bool> ReadBoolAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return await ReadByteAsync(cancellationToken) == 1;
        }

        public override async ValueTask<sbyte> ReadByteAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Trans.ReadAllAsync(PreAllocatedBuffer, 0, 1, cancellationToken);
            return (sbyte)PreAllocatedBuffer[0];
        }

        public override async ValueTask<short> ReadI16Async(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Trans.ReadAllAsync(PreAllocatedBuffer, 0, 2, cancellationToken);
            var result = BinaryPrimitives.ReadInt16BigEndian(PreAllocatedBuffer);
            return result;
        }

        public override async ValueTask<int> ReadI32Async(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Trans.ReadAllAsync(PreAllocatedBuffer, 0, 4, cancellationToken);

            var result = BinaryPrimitives.ReadInt32BigEndian(PreAllocatedBuffer);

            return result;
        }

        public override async ValueTask<long> ReadI64Async(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Trans.ReadAllAsync(PreAllocatedBuffer, 0, 8, cancellationToken);
            return BinaryPrimitives.ReadInt64BigEndian(PreAllocatedBuffer);
        }

        public override async ValueTask<double> ReadDoubleAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var d = await ReadI64Async(cancellationToken);
            return BitConverter.Int64BitsToDouble(d);
        }

        public override ValueTask ReadAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override ValueTask ReadResponseAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override ValueTask ReadAuthenticationMethodAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override ValueTask<(string username, string password)> ReadAuthRequestAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override ValueTask SendAuthRequestAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override ValueTask SendAuthenticationMethodAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override ValueTask SendDataToLocalAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override async ValueTask<byte[]> ReadBinaryAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var size = await ReadI32Async(cancellationToken);
            Transport.CheckReadBytesAvailable(size);
            var buf = new byte[size];
            await Trans.ReadAllAsync(buf, 0, size, cancellationToken);
            return buf;
        }

        public override async ValueTask<(NegotiationStatus status, int length)> ReadSaslHeaderAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Transport.CheckReadBytesAvailable(5);
            var statusBuf = new byte[1];
            await Trans.ReadAllAsync(statusBuf, 0, 1, cancellationToken);

            var lengthBuf = new byte[4];
            await Trans.ReadAllAsync(lengthBuf, 0, 4, cancellationToken);

            return ((NegotiationStatus)statusBuf[0], BinaryPrimitives.ReadInt32BigEndian(lengthBuf));
        }

        public override async ValueTask<(string username, string password)> ReadSaslLDAPAuthenticationInfoAsync(int packetLength, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await ReadByteAsync(cancellationToken);

            var bytes = new byte[packetLength - 1];
            await Transport.ReadAllAsync(bytes, 0, packetLength - 1, cancellationToken);

            var splitIndex = Array.IndexOf(bytes, (byte)0);

            if (splitIndex == -1)
                throw new InvalidDataException();

            var username = bytes[..splitIndex];
            var password = bytes[(splitIndex + 1)..];

            return (Encoding.UTF8.GetString(username), Encoding.UTF8.GetString(password));
        }

        public override async ValueTask<bool> HasSaslRequestAsync(CancellationToken cancellationToken = default)
        {
            return await Transport.HasSaslRequestAsync(cancellationToken);
        }

        public override async ValueTask<string> ReadStringAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var size = await ReadI32Async(cancellationToken);
            return size > 0 ? await ReadStringBodyAsync(size, cancellationToken) : string.Empty;
        }

        private async ValueTask<string> ReadStringBodyAsync(int size, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (size <= PreAllocatedBuffer.Length)
            {
                await Trans.ReadAllAsync(PreAllocatedBuffer, 0, size, cancellationToken);
                return Encoding.UTF8.GetString(PreAllocatedBuffer, 0, size);
            }

            Transport.CheckReadBytesAvailable(size);
            var buf = new byte[size];
            await Trans.ReadAllAsync(buf, 0, size, cancellationToken);
            return Encoding.UTF8.GetString(buf, 0, buf.Length);
        }

        // Return the minimum number of bytes a type will consume on the wire
        public override int GetMinSerializedSize(TType type)
        {
            switch (type)
            {
                case TType.Stop: return 0;
                case TType.Void: return 0;
                case TType.Bool: return sizeof(byte);
                case TType.Byte: return sizeof(byte);
                case TType.Double: return sizeof(double);
                case TType.I16: return sizeof(short);
                case TType.I32: return sizeof(int);
                case TType.I64: return sizeof(long);
                case TType.String: return sizeof(int); // string length
                case TType.Struct: return 0; // empty struct
                case TType.Map: return sizeof(int); // element count
                case TType.Set: return sizeof(int); // element count
                case TType.List: return sizeof(int); // element count
                default: throw new TProtocolException(TProtocolException.NOT_IMPLEMENTED, "unrecognized type code");
            }
        }

        public class Factory : TProtocolFactory
        {
            protected bool StrictRead;
            protected bool StrictWrite;

            public Factory()
                : this(false, true)
            {
            }

            public Factory(bool strictRead, bool strictWrite)
            {
                StrictRead = strictRead;
                StrictWrite = strictWrite;
            }

            public override TProtocol GetProtocol(TTransport trans)
            {
                return new TBinaryProtocol(trans, StrictRead, StrictWrite);
            }
        }
    }
}
