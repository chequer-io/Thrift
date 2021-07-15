using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocol;
using Thrift.Protocol.Sasl;
using Thrift.Transport.Sasl;

namespace Thrift.Transport
{
    public class TLocalSaslTransport
    {
        protected readonly TProtocol _input;
        protected readonly TProtocol _output;

        public NegotiationStatus Status { get; set; }

        public string Username
        {
            get => _authTransport?.Username ?? throw new InvalidDataException();
            set
            {
                if (_authTransport != null)
                    _authTransport.Username = value;
            }
        }

        public string Password
        {
            get => _authTransport?.Password ?? throw new InvalidDataException();
            set
            {
                if (_authTransport != null)
                    _authTransport.Password = value;
            }
        }

        public string AuthType { get; private set; }

        public byte[] Data { get; set; }

        private TLocalSaslTransport _authTransport;

        public TLocalSaslTransport(TProtocol input, TProtocol output)
        {
            _input = input;
            _output = output;
        }

        protected TLocalSaslTransport()
        {
        }

        public async ValueTask ReadAsync(CancellationToken cancellationToken = default)
        {
            int length;
            (Status, length) = await _input.ReadSaslHeaderAsync(cancellationToken);

            Data = new byte[length];
            await _input.Transport.ReadAllAsync(Data, 0, length, cancellationToken);
        }

        public async ValueTask ReadResponseAsync(CancellationToken cancellationToken = default)
        {
            int length;
            (Status, length) = await _output.ReadSaslHeaderAsync(cancellationToken);

            Data = new byte[length];
            await _output.Transport.ReadAllAsync(Data, 0, length, cancellationToken);
        }

        public async ValueTask ReadAuthenticationMethodAsync(CancellationToken cancellationToken = default)
        {
            int length;
            (Status, length) = await _input.ReadSaslHeaderAsync(cancellationToken);

            AuthType = await _input.ReadStringAsync(length, cancellationToken);

            CreateAuthTransport();
        }

        private void CreateAuthTransport()
        {
            switch (AuthType)
            {
                case "PLAIN":
                    _authTransport = new TLdapSaslTransport(_input, _output)
                    {
                        Status = Status,
                        AuthType = AuthType
                    };

                    break;

                default:
                    throw new InvalidOperationException();
            }
        }

        public virtual ValueTask<(string username, string password)> ReadAuthRequestAsync(CancellationToken cancellationToken = default)
        {
            return _authTransport.ReadAuthRequestAsync(cancellationToken);
        }

        public virtual ValueTask SendAuthRequestAsync(CancellationToken cancellationToken = default)
        {
            return _authTransport.SendAuthRequestAsync(cancellationToken);
        }

        public async ValueTask SendAuthenticationMethodAsync(CancellationToken cancellationToken = default)
        {
            await _output.WriteByteAsync((sbyte)Status, cancellationToken);
            await _output.WriteStringAsync(AuthType, cancellationToken);
            await _output.Transport.FlushAsync(cancellationToken);
        }

        public async ValueTask SendDataToLocalAsync(CancellationToken cancellationToken = default)
        {
            await _input.WriteByteAsync((sbyte)Status, cancellationToken);
            await _input.WriteBinaryAsync(Data, cancellationToken);
            await _input.Transport.FlushAsync(cancellationToken);
        }
    }
}
