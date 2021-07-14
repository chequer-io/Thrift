namespace Thrift.Protocol.Sasl
{
    public enum NegotiationStatus
    {
        TSASL_INVALID = -1,
        TSASL_START = 1,
        TSASL_OK = 2,
        TSASL_BAD = 3,
        TSASL_ERROR = 4,
        TSASL_COMPLETE = 5
    }
}
