using Npgsql;
using System;
using System.Collections.Generic;
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
        static void Main(string[] args)
        {
            Init();

            var ctrl = new RequestProcessor();
            var ss = new SocketServer(new IPEndPoint(IPAddress.Any, 88));
            _ = Task.Factory.StartNew(async () =>
            {
                while (true)
                {
                    var client = await ss.AcceptAsync();
                    var requestQueue = new BColl<Message>();
                    var responseQueue = new BColl<Message>();
                    _ = Task.Factory.StartNew(() =>
                    {
                        while (true)
                        {
                            var m = client.ReadJ<Message>();
                            requestQueue.Add(m);
                        }
                    }, TaskCreationOptions.LongRunning);

                    _ = Task.Factory.StartNew(() =>
                    {
                        while (true)
                        {
                            foreach (var m in responseQueue.Take(16))
                                client.WriteJ(m);
                            client.Flush();
                        }
                    }, TaskCreationOptions.LongRunning);

                    _ = Task.Factory.StartNew(() =>
                    {
                        while (true)
                        {
                            var req = requestQueue.Take();
                            if (req.Params["method"] == "get")
                            {
                                ctrl
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
                                ctrl
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
                        }
                    }, TaskCreationOptions.LongRunning);
                }
            });

            while (true) Thread.Sleep(100);
        }

        static void Init()
        {
            Console.WriteLine("VER 14 no sync commit - session");

            var pgConn = RequestProcessor.GetConnectionString();

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

            return;

            try
            {
                using var con = new NpgsqlConnection(pgConn);
                con.Open();
                InitSQL.Split(";").ToList().ForEach(sql =>
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

        const string InitSQL = @"
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

create table queue2(
	queue_name varchar,
	partition_name varchar,
	selector varchar,
	status int default 0,
	message_id uuid,
	created timestamp default current_timestamp
);

create index ix_queue2_get_name on queue(queue_name, status, created);
create index ix_queue2_get_part on queue(queue_name, status, partition_name, created);
create index ix_queue2_get_sel on queue(queue_name, status, selector, created);
create index ix_queue2_get_part_sel on queue(queue_name, status, partition_name, selector, created);
create index ix_queue2_message_id on queue(message_id);

";

    }
}
