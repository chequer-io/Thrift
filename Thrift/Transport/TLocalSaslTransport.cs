using System;
using System.Buffers.Binary;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocol;
using Thrift.Protocol.Sasl;

namespace Thrift.Transport
{
    public class TLocalSaslTransport
    {
        private readonly TProtocol _protocol;

        private int headerLEngth => Username.Length + Password.Length + 2;

        private NegotiationStatus _status;

        public string Username { get; set; }

        public string Password { get; set; }

        public string AuthType { get; private set; }

        public TLocalSaslTransport(TProtocol protocol)
        {
            _protocol = protocol;
        }

        public async ValueTask<(string username, string password)> ReadLDAPAuthenticationRequestAsync(CancellationToken cancellationToken = default)
        {
            var header = await _protocol.ReadSaslHeaderAsync(cancellationToken);
            var length = BinaryPrimitives.ReadInt32BigEndian(header[1..].AsSpan());
            AuthType = await _protocol.ReadStringAsync(length, cancellationToken);
            // TODO: Send Request

            header = await _protocol.ReadSaslHeaderAsync(cancellationToken);
            _status = (NegotiationStatus)header[0];

            length = BinaryPrimitives.ReadInt32BigEndian(header[1..].AsSpan());

            return await _protocol.ReadSaslLDAPAuthenticationInfoAsync(length, cancellationToken);
        }
    }
}
