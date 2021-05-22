using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace pgq2
{
    public class BatchQueue
    {
        readonly Queue<List<GetRequest>> queue = new();
        readonly Dictionary<MsgKey, List<GetRequest>> map = new();

        public void EnqueueRequest(GetRequest r)
        {
            lock (queue)
            {
                if (map.TryGetValue(r.Key, out var current))
                    current.Add(r);
                else
                    queue.Enqueue(map[r.Key] = new List<GetRequest> { r });
            }
        }

        public async Task<List<GetRequest>> DequeueBatch()
        {
            while (true)
            {
                while (queue.Count == 0) await Task.Delay(1);
                lock (queue)
                {
                    if (queue.Count == 0) continue;
                    var req = queue.Dequeue();
                    map.Remove(req[0].Key);
                    return req;
                }
            }
        }
    }

    public class MsgKey
    {
        public string QueueName { get; set; }
        public string Partition { get; set; }
        public string Selector { get; set; }
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (obj is MsgKey sk)
                return sk.QueueName == QueueName && sk.Partition == Partition && sk.Selector == Selector;
            return false;
        }

        public override string ToString()
        {
            return $"{QueueName ?? String.Empty}-{Partition ?? string.Empty}-{Selector ?? String.Empty}";
        }
    }

    public class Msg
    {
        public MsgKey Key { get; set; }
        public Guid ID { get; set; }
        public string Payload { get; set; }
    }

    public class GetRequest
    {
        public MsgKey Key { get; set; }
        public TaskCompletionSource<Guid> Tcs { get; set; }
    }


    public class RequestProcessor
    {
        readonly BatchQueue bukets = new();

        public RequestProcessor()
        {
            Task.Factory.StartNew(GetWorker, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(GetWorker, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(GetWorker, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(GetWorker, TaskCreationOptions.LongRunning);

            Task.Factory.StartNew(PutWorker, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(PutWorker, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(PutWorker, TaskCreationOptions.LongRunning);

            Task.Factory.StartNew(AckWorker, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(AckWorker, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(AckWorker, TaskCreationOptions.LongRunning);
        }

        public async Task<Guid> Get(MsgKey key)
        {
            var r = new GetRequest { Key = key, Tcs = new TaskCompletionSource<Guid>() };
            bukets.EnqueueRequest(r);
            return await r.Tcs.Task;
        }

        readonly List<Guid> ack = new List<Guid>();
        TaskCompletionSource<bool> ackTcs = new TaskCompletionSource<bool>();

        public async Task<bool> Ack(Guid id)
        {
            Task<bool> w;
            lock (ack)
            {
                ack.Add(id);
                w = ackTcs.Task;
            }
            return await w;
        }

        public static string GetConnectionString()
        {
            var pg = Environment.GetEnvironmentVariable("PGCONNSTR");
            if (!string.IsNullOrWhiteSpace(pg)) return pg;
            return "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres;";
        }

        public async Task AckWorker()
        {
            using var con = new NpgsqlConnection(GetConnectionString());
            con.Open();
            while (true)
            {
                while (ack.Count == 0) await Task.Delay(1);
                var batch = new List<Guid>();
                TaskCompletionSource<bool> tcs;
                lock (ack)
                {
                    if (ack.Count == 0) continue;
                    batch.AddRange(ack);
                    ack.Clear();
                    tcs = ackTcs;
                    ackTcs = new TaskCompletionSource<bool>();
                }

                await Ack(batch, con);
                tcs.SetResult(true);
            }
        }

        async Task Ack(IEnumerable<Guid> acks, NpgsqlConnection con)
        {
            var sb = new StringBuilder();
            sb.Append('\'');
            sb.AppendJoin("','", acks);
            sb.Append('\'');
            using var cmd = con.CreateCommand();
            cmd.CommandText = $"delete from queue where status = 1 and message_id in({sb})";
            await cmd.ExecuteNonQueryAsync();
        }

        readonly List<Msg> put = new List<Msg>();
        TaskCompletionSource<bool> putTcs = new TaskCompletionSource<bool>();
        public async Task<bool> Put(Msg m)
        {
            Task<bool> w;
            lock (put)
            {
                put.Add(m);
                w = putTcs.Task;
            }
            return await w;
        }

        async Task PutWorker()
        {
            try
            {
                var rnd = new Random();
                using var con = new NpgsqlConnection(GetConnectionString());
                con.Open();
                while (true)
                {
                    while (put.Count == 0) await Task.Delay(1);
                    var batch = new List<Msg>();
                    TaskCompletionSource<bool> tcs;
                    lock (put)
                    {
                        if (put.Count == 0) continue;
                        batch.AddRange(put);
                        put.Clear();
                        tcs = putTcs;
                        putTcs = new TaskCompletionSource<bool>();
                    }

                    await Put(batch, con);
                    tcs.SetResult(true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        public async Task Put(IEnumerable<Msg> batch)
        {
            using var con = new NpgsqlConnection(GetConnectionString());
            con.Open();
            await Put(batch, con);
            con.Close();
        }
        async Task Put(IEnumerable<Msg> batch, NpgsqlConnection con)
        {
            using var w = await con.BeginBinaryImportAsync("COPY queue(queue_name, partition_name, selector, message_id) FROM STDIN (FORMAT BINARY)");
            foreach (var m in batch)
            {
                w.StartRow();
                w.Write(m.Key.QueueName);
                w.Write(m.Key.Partition);
                w.Write(m.Key.Selector);
                w.Write(m.ID);
            }
            await w.CompleteAsync();
        }

        async Task GetWorker()
        {
            using var con = new NpgsqlConnection(GetConnectionString());
            con.Open();

            while (true)
            {
                var t = await bukets.DequeueBatch();

                var messages = await TakeFromDB(con, t[0].Key, t.Count);

                for (var i = 0; i < t.Count; i++)
                    t[i].Tcs.SetResult(i < messages.Count ? messages[i] : Guid.Empty);
            }
        }

        //Random rnd = new Random();
        async Task<List<Guid>> TakeFromDB(NpgsqlConnection con, MsgKey key, int Count)
        {
            var sw = Stopwatch.StartNew();

            var cl = new StringBuilder();
            if (key.Partition != null) cl.Append("and partition_name = @partition_name ");
            if (key.Selector != null) cl.Append("and selector like @selector ");
            using var cmd = con.CreateCommand();
            cmd.CommandText = $@"
            update queue set status = 1 where message_id in (
            	select message_id from queue where queue_name = @queue_name and status = 0
                {cl}
                order by created
                for update skip locked
                limit {Count}
            )
            returning message_id;";

            //if (rnd.NextDouble() < 0.001)
            //if (Count > 1)
            //    Console.WriteLine(Count);

            cmd.Parameters.AddWithValue("queue_name", key.QueueName);
            if (key.Partition != null) cmd.Parameters.AddWithValue("partition_name", key.Partition);
            if (key.Selector != null) cmd.Parameters.AddWithValue("selector", key.Selector);

            using var r = await cmd.ExecuteReaderAsync();
            var m = new List<Guid>();
            while (await r.ReadAsync())
                m.Add((Guid)r[0]);
            r.Close();

            //Console.WriteLine($"GET: {Count}\t{sw.ElapsedMilliseconds}");

            return m;
        }
    }
}
