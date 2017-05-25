﻿using System.Collections.Generic;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.States;
using Hangfire.Storage.Monitoring;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal class PostgreSqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlJobQueueMonitoringApi(IPostgreSqlConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));

            _connectionProvider = connectionProvider;
            _options = options;
        }

        public IEnumerable<string> GetQueues()
        {
            string sqlQuery = $@"
SELECT DISTINCT ""queue"" 
FROM ""{_options.SchemaName}"".""jobqueue"";
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query(sqlQuery).Select(x => (string)x.queue).ToList();
            }
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int @from, int perPage)
        {
            var enqueuedJobsQuery = GetQuery(@from, perPage, "IS NULL");

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var jobs = connectionHolder.Connection.Query<SqlJob>(enqueuedJobsQuery).ToList();

                return Utils.DeserializeJobs(
                    jobs,
                    (sqlJob, job, stateData) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = sqlJob.StateName,
                        EnqueuedAt = sqlJob.StateName == EnqueuedState.StateName
                            ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                            : null
                    });
            }
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
        {
            var fetchedJobsQuery = GetQuery(@from, perPage, "IS NOT NULL");

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var jobs = connectionHolder.Connection.Query<SqlJob>(fetchedJobsQuery).ToList();

                return Utils.DeserializeJobs(
                    jobs,
                    (sqlJob, job, stateData) => new FetchedJobDto
                    {
                        Job = Utils.DeserializeJob(sqlJob.InvocationData, sqlJob.Arguments),
                        State = sqlJob.StateName,
                        FetchedAt = sqlJob.FetchedAt
                    });
            }
        }


        private string GetQuery(int @from, int perPage, string fetchCondition) => $@"
SELECT j.id ""Id"",
       j.invocationdata ""InvocationData"", 
       j.arguments ""Arguments"", 
       j.createdat ""CreatedAt"", 
       j.expireat ""ExpireAt"", 
       s.name ""StateName"", 
       s.reason""StateReason"", 
       s.data ""StateData""
FROM ""{_options.SchemaName}"".job j
LEFT JOIN ""{_options.SchemaName}"".state s ON s.id = j.stateid
LEFT JOIN ""{_options.SchemaName}"".jobqueue jq ON jq.jobid = j.id
WHERE jq.fetchedat {fetchCondition}
LIMIT {perPage} OFFSET {@from};";


        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            return GetQueuedOrFetchedJobIds(queue, false, @from, perPage);
        }

        private IEnumerable<int> GetQueuedOrFetchedJobIds(string queue, bool fetched, int @from, int perPage)
        {
            string sqlQuery = string.Format($@"
SELECT j.""id"" 
FROM ""{_options.SchemaName}"".""jobqueue"" jq
LEFT JOIN ""{_options.SchemaName}"".""job"" j ON jq.""jobid"" = j.""id""
WHERE jq.""queue"" = @queue 
AND jq.""fetchedat"" {{0}}
AND j.""id"" IS NOT NULL
LIMIT @count OFFSET @start;
", fetched ? "IS NOT NULL" : "IS NULL");

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<int>(
                        sqlQuery,
                        new { queue = queue, start = @from, count = perPage })
                    .ToList();
            }
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return GetQueuedOrFetchedJobIds(queue, true, @from, perPage);
        }

        public (long? enqueued, long? fetched) GetEnqueuedAndFetchedCount(string queue)
        {
            string sqlQuery = @"
SELECT (
        SELECT COUNT(*) 
        FROM """ + _options.SchemaName + @""".""jobqueue"" 
        WHERE ""fetchedat"" IS NULL 
        AND ""queue"" = @queue
    ) ""EnqueuedCount"", 
    (
        SELECT COUNT(*) 
        FROM """ + _options.SchemaName + @""".""jobqueue"" 
        WHERE ""fetchedat"" IS NOT NULL 
        AND ""queue"" = @queue
    ) ""FetchedCount"";
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query(sqlQuery, new { queue = queue }).Single();

                return (result.EnqueuedCount, result.FetchedCount);
            }
        }
    }
}
