using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Thrift.Transport.Sasl
{
    public class TLdapSaslProtcol : TSaslProtocol
    {
        public override string Username { get; set; }

        public override string Password { get; set; }

        public TLdapSaslProtcol(TTransport transport) : base(transport)
        {
        }

        public override async ValueTask<(string username, string password)> ReadAuthRequestAsync(CancellationToken cancellationToken = default)
        {
            int length;
            (Status, length) = await ReadSaslHeaderAsync(cancellationToken);

            (Username, Password) = await ReadSaslLDAPAuthenticationInfoAsync(length, cancellationToken);

            return (Username, Password);
        }

        public override async ValueTask SendAuthRequestAsync(CancellationToken cancellationToken = default)
        {
            var dest = Server ?? this;

            await dest.WriteByteAsync((sbyte)Status, cancellationToken);
            await dest.WriteI32Async(Username.Length + Password.Length + 2, cancellationToken);

            await dest.WriteByteAsync(0, cancellationToken);
            await dest.Transport.WriteAsync(Encoding.UTF8.GetBytes(Username), cancellationToken);
            await dest.WriteByteAsync(0, cancellationToken);
            await dest.Transport.WriteAsync(Encoding.UTF8.GetBytes(Password), cancellationToken);

            await dest.Transport.FlushAsync(cancellationToken);
        }
    }
}
