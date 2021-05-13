using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Npgsql;
using System;
using System.Linq;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;

namespace pgq
{
    public class Program
    {
        public static string pgConn { get; private set; } = "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres;";

        public static void Main(string[] args)
        {
            Console.WriteLine("VER 14 no sync commit - session");

            var envc = Environment.GetEnvironmentVariable("PGQX_CONNECTION_STRING");
            if (!string.IsNullOrWhiteSpace(envc))
                pgConn = envc;

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

            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });


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
create index ix_queue_message_id on queue(message_id);";
    }
}
