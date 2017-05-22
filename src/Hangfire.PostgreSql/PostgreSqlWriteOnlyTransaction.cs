// This file is part of Hangfire.PostgreSql.
// Copyright � 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
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
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    public class PostgreSqlWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Queue<Action<NpgsqlConnection, NpgsqlTransaction>> _commandQueue
            = new Queue<Action<NpgsqlConnection, NpgsqlTransaction>>();

        private readonly NpgsqlConnection _connection;
        private readonly PersistentJobQueueProviderCollection _queueProviders;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlWriteOnlyTransaction(
            NpgsqlConnection connection,
            PostgreSqlStorageOptions options,
            PersistentJobQueueProviderCollection queueProviders)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _queueProviders = queueProviders ?? throw new ArgumentNullException(nameof(queueProviders));
        }

        public override void Dispose() { }

        public override void Commit()
        {
            using (var transaction = _connection.BeginTransaction(IsolationLevel.RepeatableRead))
            {
                foreach (var command in _commandQueue)
                {
                    command(_connection, transaction);
                }
                transaction.Commit();
            }
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var sql = $@"
UPDATE ""{_options.SchemaName}"".""job""
SET ""expireat"" = NOW() AT TIME ZONE 'UTC' + INTERVAL '{(long)expireIn.TotalSeconds} SECONDS'
WHERE ""id"" = @id;
";

            QueueCommand((con, trx) => con.Execute(
                sql,
                new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }, trx));
        }

        public override void PersistJob(string jobId)
        {
            var sql = $@"
UPDATE ""{_options.SchemaName}"".""job"" 
SET ""expireat"" = NULL 
WHERE ""id"" = @id;
";
            QueueCommand((con, trx) => con.Execute(
                sql,
                new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) }, trx));
        }

        public override void SetJobState(string jobId, IState state)
        {
            var addAndSetStateSql = $@"
WITH s AS (
    INSERT INTO ""{_options.SchemaName}"".""state"" (""jobid"", ""name"", ""reason"", ""createdat"", ""data"")
    VALUES (@jobId, @name, @reason, @createdAt, @data) RETURNING ""id""
)
UPDATE ""{_options.SchemaName}"".""job"" j
SET ""stateid"" = s.""id"", ""statename"" = @name
FROM s
WHERE j.""id"" = @id;
";

            QueueCommand((con, trx) => con.Execute(
                addAndSetStateSql,
                new
                {
                    jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData()),
                    id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture)
                }, trx));
        }

        public override void AddJobState(string jobId, IState state)
        {
            var addStateSql = $@"
INSERT INTO ""{_options.SchemaName}"".""state"" (""jobid"", ""name"", ""reason"", ""createdat"", ""data"")
VALUES (@jobId, @name, @reason, @createdAt, @data);
";

            QueueCommand((con, trx) => con.Execute(
                addStateSql,
                new
                {
                    jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData())
                }, trx));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var provider = _queueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue(_connection);

            QueueCommand((con, trx) => persistentQueue.Enqueue(queue, jobId));
        }

        public override void IncrementCounter(string key)
        {
            var sql = $@"INSERT INTO ""{_options.SchemaName}"".""counter"" (""key"", ""value"") VALUES (@key, @value);";
            QueueCommand((con, trx) => con.Execute(
                sql,
                new { key, value = +1 }, trx));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            var sql = $@"
INSERT INTO ""{_options.SchemaName}"".""counter""(""key"", ""value"", ""expireat"") 
VALUES (@key, @value, NOW() AT TIME ZONE 'UTC' + INTERVAL '{(long)expireIn.TotalSeconds} SECONDS');";

            QueueCommand((con, trx) => con.Execute(
                sql,
                new { key, value = +1 }, trx));
        }

        public override void DecrementCounter(string key)
        {
            var sql = $@"INSERT INTO ""{_options.SchemaName}"".""counter"" (""key"", ""value"") VALUES (@key, @value);";
            QueueCommand((con, trx) => con.Execute(
                sql,
                new { key, value = -1 }, trx));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            var sql = $@"
INSERT INTO ""{_options.SchemaName}"".""counter""(""key"", ""value"", ""expireat"") 
VALUES (@key, @value, NOW() AT TIME ZONE 'UTC' + INTERVAL '{(long)expireIn.TotalSeconds} SECONDS');";

            QueueCommand((con, trx) => con.Execute(sql
                ,
                new { key, value = -1 }, trx));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            var addSql = $@"
WITH ""inputvalues"" AS (
	SELECT @key ""key"", @value ""value"", @score ""score""
), ""updatedrows"" AS ( 
	UPDATE ""{_options.SchemaName}"".""set"" ""updatetarget""
	SET ""score"" = ""inputvalues"".""score""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""key"" = ""inputvalues"".""key""
	AND ""updatetarget"".""value"" = ""inputvalues"".""value""
	RETURNING ""updatetarget"".""key"", ""updatetarget"".""value""
)
INSERT INTO ""{_options.SchemaName}"".""set""(""key"", ""value"", ""score"")
SELECT ""key"", ""value"", ""score"" FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""key"" = ""insertvalues"".""key"" 
	AND ""updatedrows"".""value"" = ""insertvalues"".""value""
);
";

            QueueCommand((con, trx) => con.Execute(
                addSql,
                new { key, value, score }, trx));
        }

        public override void RemoveFromSet(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                $@"
DELETE FROM ""{_options.SchemaName}"".""set"" 
WHERE ""key"" = @key 
AND ""value"" = @value;
",
                new { key, value }, trx));
        }

        public override void InsertToList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                $@"
INSERT INTO ""{_options.SchemaName}"".""list"" (""key"", ""value"") 
VALUES (@key, @value);
",
                new { key, value }, trx));
        }

        public override void RemoveFromList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(
                $@"
DELETE FROM ""{_options.SchemaName}"".""list"" 
WHERE ""key"" = @key 
AND ""value"" = @value;
",
                new { key, value }, trx));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            var trimSql = $@"
DELETE FROM ""{_options.SchemaName}"".""list"" AS source
WHERE ""key"" = @key
AND ""id"" NOT IN (
    SELECT ""id"" 
    FROM ""{_options.SchemaName}"".""list"" AS keep
    WHERE keep.""key"" = source.""key""
    ORDER BY ""id"" 
    OFFSET @start LIMIT @end
);
";

            QueueCommand((con, trx) => con.Execute(
                trimSql,
                new { key = key, start = keepStartingFrom, end = (keepEndingAt - keepStartingFrom + 1) }, trx));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            var sql = $@"
WITH ""inputvalues"" AS (
	SELECT @key ""key"", @field ""field"", @value ""value""
), ""updatedrows"" AS ( 
	UPDATE ""{_options.SchemaName}"".""hash"" ""updatetarget""
	SET ""value"" = ""inputvalues"".""value""
	FROM ""inputvalues""
	WHERE ""updatetarget"".""key"" = ""inputvalues"".""key""
	AND ""updatetarget"".""field"" = ""inputvalues"".""field""
	RETURNING ""updatetarget"".""key"", ""updatetarget"".""field""
)
INSERT INTO ""{_options.SchemaName}"".""hash""(""key"", ""field"", ""value"")
SELECT ""key"", ""field"", ""value"" 
FROM ""inputvalues"" ""insertvalues""
WHERE NOT EXISTS (
	SELECT 1 
	FROM ""updatedrows"" 
	WHERE ""updatedrows"".""key"" = ""insertvalues"".""key"" 
	AND ""updatedrows"".""field"" = ""insertvalues"".""field""
);
";

            foreach (var keyValuePair in keyValuePairs)
            {
                var pair = keyValuePair;

                QueueCommand((con, trx) => con.Execute(sql, new { key = key, field = pair.Key, value = pair.Value }, trx));
            }
        }

        public override void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var sql = $@"DELETE FROM ""{_options.SchemaName}"".""hash"" WHERE ""key"" = @key";
            QueueCommand((con, trx) => con.Execute(
                sql,
                new { key }, trx));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var sql = $@"UPDATE ""{_options.SchemaName}"".""set"" SET ""expireat"" = @expireAt WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                sql,
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var sql = $@"UPDATE ""{_options.SchemaName}"".""list"" SET ""expireat"" = @expireAt WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                sql,
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var sql = $@"UPDATE ""{_options.SchemaName}"".""hash"" SET expireat = @expireAt WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                sql,
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var sql = $@"UPDATE ""{_options.SchemaName}"".""set"" SET expireat = null WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                sql,
                new { key },
                transaction));
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var sql = $@"UPDATE ""{_options.SchemaName}"".""list"" SET expireat = null WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                sql,
                new { key },
                transaction));
        }

        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var sql = $@"UPDATE ""{_options.SchemaName}"".""hash"" SET expireat = null WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                sql,
                new { key },
                transaction));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            var sql = $@"INSERT INTO ""{_options.SchemaName}"".""set"" (""key"", ""value"", ""score"") VALUES (@key, @value, 0.0)";

            QueueCommand((connection, transaction) => connection.Execute(
                sql,
                items.Select(value => new { key, value }).ToList(),
                transaction));
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var sql = $@"DELETE FROM ""{_options.SchemaName}"".""set"" WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                sql,
                new { key },
                transaction));
        }

        internal void QueueCommand(Action<NpgsqlConnection, NpgsqlTransaction> action)
        {
            _commandQueue.Enqueue(action);
        }
    }
}
