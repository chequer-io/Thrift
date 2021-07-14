using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocol;

namespace Thrift.Transport.Sasl
{
    public class TLdapSaslTransport : TLocalSaslTransport
    {
        public TLdapSaslTransport(TProtocol input, TProtocol output) : base(input, output)
        {
        }

        public TLdapSaslTransport()
        {
        }

        public override async ValueTask<(string username, string password)> ReadAuthRequestAsync(CancellationToken cancellationToken = default)
        {
            int length;
            (Status, length) = await _input.ReadSaslHeaderAsync(cancellationToken);

            (Username, Password) = await _input.ReadSaslLDAPAuthenticationInfoAsync(length, cancellationToken);

            return (Username, Password);
        }

        public override async ValueTask SendAuthRequestAsync(CancellationToken cancellationToken = default)
        {
            await _output.WriteByteAsync((sbyte)Status, cancellationToken);
            await _output.WriteI32Async(Username.Length + Password.Length + 2, cancellationToken);

            await _output.WriteByteAsync(0, cancellationToken);
            await _output.Transport.WriteAsync(Encoding.UTF8.GetBytes(Username), cancellationToken);
            await _output.WriteByteAsync(0, cancellationToken);
            await _output.Transport.WriteAsync(Encoding.UTF8.GetBytes(Password), cancellationToken);

            await _output.Transport.FlushAsync(cancellationToken);
        }
    }
}
