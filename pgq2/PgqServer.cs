using Npgsql;
using System;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace pgq2
{
    class PgqServer
    {
        static async Task Main(string[] args)
        {
            Init();

            var ctrlPut = new PutProcessor(3);
            var ctrlGet = new GetProcessor(8);
            var ctrlAck = new AckProcessor(3);
            TaskCompletionSource tcs = new();
            var ss = new SocketServer(new IPEndPoint(IPAddress.Any, 88));
            await Task.Factory.StartNew(async () =>
            {
                while (true)
                {
                    //var stream = new CStream(await ss.AcceptAsync());
                    var stream = await ss.AcceptAsync();

                    var requestQueue = new BColl<Message>();
                    var responseQueue = new BColl<Message>();
                    _ = Task.Factory.StartNew(() =>
                    {
                        while (true)
                        {
                            var m = stream.ReadJ<Message>();
                            requestQueue.Add(m);
                        }
                    }, TaskCreationOptions.LongRunning);

                    _ = Task.Factory.StartNew(() =>
                    {
                        while (true)
                        {
                            var r = responseQueue.Take(1000);
                            //Console.WriteLine(r.Length);
                            foreach (var m in r)
                                stream.WriteJ(m);
                            if (responseQueue.Count == 0)
                                stream.Flush();
                        }
                    }, TaskCreationOptions.LongRunning);

                    _ = Task.Factory.StartNew(() =>
                    {
                        while (true)
                        {
                            var req = requestQueue.Take();
                            if (req.Params["method"] == "get")
                            {
                                ctrlGet
                                    .Get(new MsgKey
                                    {
                                        QueueName = req.Params["queueName"],
                                        Partition = req.Params["partition"],
                                        Selector = req.Params["selector"]
                                    })
                                    .ContinueWith(t => {
                                        var resp = new Message { ID = req.ID };
                                        resp.Params["messageId"] = t.Result.ToString();
                                        responseQueue.Add(resp);
                                    });
                            }
                            if (req.Params["method"] == "put")
                            {
                                ctrlPut
                                    .Put(new Msg
                                    {
                                        Key = new MsgKey
                                        {
                                            QueueName = req.Params["queueName"],
                                            Partition = req.Params["partition"],
                                            Selector = req.Params["selector"]
                                        },
                                        ID = Guid.Parse(req.Params["messageId"])
                                    })
                                    .ContinueWith(t => {
                                        var resp = new Message { ID = req.ID };
                                        resp.Params["result"] = t.Result.ToString();
                                        responseQueue.Add(resp);
                                    });
                            }
                            if (req.Params["method"] == "ack")
                            {
                                ctrlAck
                                    .Ack(Guid.Parse(req.Params["messageId"]))
                                    .ContinueWith(t => {
                                        var resp = new Message { ID = req.ID };
                                        resp.Params["result"] = t.Result.ToString();
                                        responseQueue.Add(resp);
                                    });
                            }
                        }
                    }, TaskCreationOptions.LongRunning);
                }
            });
            await tcs.Task;
        }

        static void Init()
        {
            Console.WriteLine("VER 14 no sync commit - session");

            var pgConn = ReqProcessor.GetConnectionString();

            while (true)
            {
                Thread.Sleep(1000);
                try
                {
                    var host = Regex.Match(pgConn, "host=([^;]*)(;|$)", RegexOptions.IgnoreCase).Groups[1].Value;
                    var port = Regex.Match(pgConn, "port=([^;]*)(;|$)", RegexOptions.IgnoreCase).Groups[1].Value;
                    Console.WriteLine($"reaching {host}:{port}...");
                    var c = new TcpClient();
                    c.Connect(host, int.Parse(port));
                    if (c.Connected)
                    {
                        c.Close();
                        Console.WriteLine("POSTGRESQL IS REACHABLE");
                        break;
                    }
                    else
                        Console.WriteLine("NO POSTGRESQL FOUND");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("PG RELATED ERROR:");
                    Console.WriteLine(ex.Message);
                }
            }

            try
            {
                using var con = new NpgsqlConnection(pgConn);
                con.Open();
                TableSQL.Split(";").ToList().ForEach(sql =>
                {
                    if (string.IsNullOrWhiteSpace(sql)) return;
                    using var cmd = con.CreateCommand();
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                });

                con.Close();
                Console.WriteLine("NPGSQL CONNECTED");
            }
            catch (Exception ex)
            {
                Console.WriteLine("NPGSQL RELATED ERROR:");
                Console.WriteLine(ex.Message);
            }
        }

        const string TableSQL = @"
create table queue(
	queue_name varchar,
	partition_name varchar,
	selector varchar,
	status int default 0,
	message_id uuid,
	created timestamp default current_timestamp
);

create index ix_queue_get_name on queue(queue_name, status, created);
create index ix_queue_get_part on queue(queue_name, status, partition_name, created);
create index ix_queue_get_sel on queue(queue_name, status, selector, created);
create index ix_queue_get_part_sel on queue(queue_name, status, partition_name, selector, created);
create index ix_queue_message_id on queue(message_id);
";

    }
}
