using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlStorage : JobStorage
    {
        private readonly Lazy<NpgsqlConnection> _lazyConnection;
        private readonly PostgreSqlStorageOptions _options;
        private readonly string _connectionString;
        private string _storageInfo;

        /// <summary>
        /// Initializes PostgreSqlStorage from the provided PostgreSqlStorageOptions and either the provided connection
        /// string or the connection string with provided name pulled from the application config file.       
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> argument is a valid 
        /// Postgres connection string </exception>
        public PostgreSqlStorage(string connectionString, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionString, nameof(connectionString));
            Guard.ThrowIfNull(options, nameof(options));
            Guard.ThrowIfConnectionStringIsInvalid(connectionString);

            _connectionString = connectionString;
            _options = options;
            _lazyConnection = new Lazy<NpgsqlConnection>(() => new NpgsqlConnection(_connectionString), LazyThreadSafetyMode.ExecutionAndPublication);
            Initialize();
        }

        /// <summary>
        /// Initializes PostgreSqlStorage with default PostgreSqlStorageOptions and either the provided connection
        /// string or the connection string with provided name pulled from the application config file.
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> argument is a valid 
        /// Postgres connection string </exception>
        public PostgreSqlStorage(string connectionString)
            : this(connectionString, new PostgreSqlStorageOptions())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgreSqlStorage"/> class with
        /// explicit instance of the <see cref="NpgsqlConnection"/> class that will be used
        /// to query the data.
        /// </summary>
        /// <param name="existingConnection">Existing connection</param>
        /// <param name="options">PostgreSqlStorageOptions</param>
        public PostgreSqlStorage(NpgsqlConnection existingConnection, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(existingConnection, nameof(existingConnection));
            Guard.ThrowIfNull(options, nameof(options));
            Guard.ThrowIfConnectionContainsEnlist(existingConnection);

            _connectionString = existingConnection.ConnectionString;
            _lazyConnection = new Lazy<NpgsqlConnection>(() => existingConnection);
            _options = options;
            Initialize();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgreSqlStorage"/> class with
        /// explicit instance of the <see cref="NpgsqlConnection"/> class that will be used
        /// to query the data.
        /// </summary>
        /// <param name="existingConnection">Existing connection</param>
        public PostgreSqlStorage(NpgsqlConnection existingConnection)
            : this(existingConnection, new PostgreSqlStorageOptions())
        {
        }

        private void Initialize()
        {
            var builder = new NpgsqlConnectionStringBuilder(_connectionString);
            _storageInfo = $"PostgreSQL Server: Host: {builder.Host}, DB: {builder.Database}, Schema: {_options.SchemaName}";

            if (_options.PrepareSchemaIfNecessary)
            {
                PostgreSqlObjectsInstaller.Install(_lazyConnection.Value, _options.SchemaName);
            }

            var defaultQueueProvider = new PostgreSqlJobQueueProvider(_options);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }

        public PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
            => new PostgreSqlMonitoringApi(_lazyConnection.Value, _options, QueueProviders);

        public override IStorageConnection GetConnection()
            => new PostgreSqlConnection(_lazyConnection.Value, QueueProviders, _options);

        public override IEnumerable<IServerComponent> GetComponents() 
            => new[] { new ExpirationManager(this, _options) };

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _options.QueuePollInterval);
            logger.InfoFormat("    Invisibility timeout: {0}.", _options.InvisibilityTimeout);
            logger.InfoFormat("    Distributed lock timeout: {0}.", _options.DistributedLockTimeout);
        }

        public override string ToString() => _storageInfo;
    }
}
