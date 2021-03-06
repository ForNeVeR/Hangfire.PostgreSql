using System;

// ReSharper disable MemberCanBePrivate.Global
namespace Hangfire.PostgreSql
{
    [Serializable]
    internal class PostgreSqlDistributedLockException : Exception
    {
        public PostgreSqlDistributedLockException() : base()
        {
        }

        public PostgreSqlDistributedLockException(string message) : base(message)
        {
        }

        public PostgreSqlDistributedLockException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
