// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Dapper;

namespace Hangfire.PostgreSql
{
    internal sealed class PostgreSqlDistributedLock : IDisposable
    {
        private readonly string _resource;
        private readonly IDbConnection _connection;
        private readonly PostgreSqlStorageOptions _options;
        private bool _completed;

        public PostgreSqlDistributedLock(string resource, TimeSpan timeout, IDbConnection connection,
            PostgreSqlStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource)) throw new ArgumentNullException(nameof(resource));
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (options == null) throw new ArgumentNullException(nameof(options));

            _resource = resource;
            _connection = connection;
            _options = options;

            if (_options.UseNativeDatabaseTransactions)
                PostgreSqlDistributedLock_Init_Transaction(resource, timeout, connection, options);
            else
                PostgreSqlDistributedLock_Init_UpdateCount(resource, timeout, connection, options);
        }

        public void PostgreSqlDistributedLock_Init_Transaction(string resource, TimeSpan timeout,
            IDbConnection connection, PostgreSqlStorageOptions options)
        {
            var lockAcquiringTime = Stopwatch.StartNew();

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                try
                {
                    int rowsAffected = -1;
                    using (var trx = _connection.BeginTransaction(IsolationLevel.RepeatableRead))
                    {
                        rowsAffected = _connection.Execute($@"
INSERT INTO ""{_options.SchemaName}"".""lock""(""resource"") 
SELECT @resource
WHERE NOT EXISTS (
    SELECT 1 FROM ""{_options.SchemaName}"".""lock"" 
    WHERE ""resource"" = @resource
);
",
                            new
                            {
                                resource = resource
                            }, trx);
                        trx.Commit();
                    }
                    if (rowsAffected > 0) return;
                }
                catch
                {
                }

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    tryAcquireLock = false;
                }
                else
                {
                    int sleepDuration = (int)(timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                    {
                        Thread.Sleep(sleepDuration);
                    }
                    else
                    {
                        tryAcquireLock = false;
                    }
                }

                try
                {
                    var reader = _connection.ExecuteReader(@"
SELECT "
);
                }
                catch (Exception)
                {

                    throw;
                }

            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource \'{_resource}\': Lock timeout.");
        }

        public void PostgreSqlDistributedLock_Init_UpdateCount(string resource, TimeSpan timeout, IDbConnection connection, PostgreSqlStorageOptions options)
        {
            var lockAcquiringTime = Stopwatch.StartNew();

            bool tryAcquireLock = true;

            while (tryAcquireLock)
            {
                try
                {
                    _connection.Execute($@"
INSERT INTO ""{_options.SchemaName}"".""lock""(""resource"", ""updatecount"") 
SELECT @resource, 0
WHERE NOT EXISTS (
    SELECT 1 FROM ""{_options.SchemaName}"".""lock"" 
    WHERE ""resource"" = @resource
);
", new
                    {
                        resource = resource
                    });
                }
                catch (Exception)
                {
                }

                int rowsAffected = _connection.Execute($@"UPDATE ""{_options.SchemaName}"".""lock"" SET ""updatecount"" = 1 WHERE ""updatecount"" = 0");

                if (rowsAffected > 0) return;

                if (lockAcquiringTime.ElapsedMilliseconds > timeout.TotalMilliseconds)
                    tryAcquireLock = false;
                else
                {
                    int sleepDuration = (int)(timeout.TotalMilliseconds - lockAcquiringTime.ElapsedMilliseconds);
                    if (sleepDuration > 1000) sleepDuration = 1000;
                    if (sleepDuration > 0)
                        Thread.Sleep(sleepDuration);
                    else
                        tryAcquireLock = false;
                }
            }

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource '{_resource}': Lock timeout.");
        }

        public void Dispose()
        {
            if (_completed) return;

            _completed = true;

            int rowsAffected = _connection.Execute($@"
DELETE FROM ""{_options.SchemaName}"".""lock"" 
WHERE ""resource"" = @resource;
",
            new
            {
                resource = _resource
            });


            if (rowsAffected <= 0)
            {
                throw new PostgreSqlDistributedLockException(
                    $"Could not release a lock on the resource '{_resource}'. Lock does not exists.");
            }
        }
    }
}
