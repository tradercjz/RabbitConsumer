using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using dolphindb.data;
using dolphindb.route;
using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using dolphindb;

namespace dolphindb_csharpapi_net_core.src
{
    public class BuleDataCommon
    {
        public string DbName { get; set; }
        public string TableName { get; set; }

        public string Measurement { get; set; }
        public DateTime Time { get; set; }
        public int Coefficient { get; set; }
        public int Coefficientext { get; set; }
        public int Decimalplaces { get; set; }
        public string DeviceId { get; set; }

        public string Displayname { get; set; }
        public string Displaytype { get; set; }
        public long ID { get; set; }
        public string Label { get; set; }
        public int Originvalue { get; set; }
        public string Type { get; set; }
        public string TypeName { get; set; }
    }

    public class BuleDataFloat
    {
        public float Value { get; set; }

    }

    public class BuleDataString
    {
        public string Value { get; set; }
    }

    public class BuleDataLong
    {
        public long Value { get; set; }
    }
    /// <summary>
    /// 封装了mtw的写入逻辑，支持写入多个指定的数据库表的数据。，如果表不存在，会在dolphindb中建立对应的表，但是database需要提前建好。
    /// </summary>
    public class MtwConsumer
    {
        ConcurrentDictionary<string, MultithreadedTableWriter> Writers = new ConcurrentDictionary<string, MultithreadedTableWriter>(4, 4);
        public string RabbitQueueName { get; set; }
        public Func<string, int, string, string, MultithreadedTableWriter> MtwConstructor { get; set; }
        public RabbitMQ.Client.IModel RabbitChannel;
        ReaderWriterLockSlim readerWriterLockSlim = new ReaderWriterLockSlim();

        public void insertDdbVoid<T>(string host, int port, string database, string table, string user, string password, BuleDataCommon data, T value, ulong tag)
        {
            // do nothing, just return
        }

        public void insertDdb<T>(string host, int port, string database, string table, string user, string password, BuleDataCommon data, T value, ulong tag)
        {
            var key = host + "/" + port + "/" + database + "/" + table;
            //Console.WriteLine(host + "/" + port + "/" + database + "/" + table);

            if (!Writers.ContainsKey(key))
            {
                DBConnection conn = new DBConnection();

                var newMtw = MtwConstructor(host, port, database, table);
                Writers[key] = newMtw;
            }

            MultithreadedTableWriter mtw = Writers[key];
            bool succesInsert = false;
            while (!succesInsert)
            {
                try
                {
                    var ret = mtw.insert(
                        data.Measurement,
                        data.Time,//time
                        (float)data.Coefficient,//coefficient
                        (float)data.Coefficientext,//coefficientext
                        data.Decimalplaces,//decimalplaces
                        data.DeviceId,
                        data.Displayname,
                        data.Displaytype,
                        data.ID,
                        data.Label,
                        (int)data.Originvalue,
                        data.Type,
                        data.TypeName,
                        value,
                        (long)tag //tag需要写在最后一列。
                        );
                    if (ret.hasError()) Console.WriteLine("insert fail: " + ret.errorInfo);
                    succesInsert = true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(mtw.getStatus().errorInfo);
                    Console.WriteLine("insertDdb error: " + e.Message);

                    var unwrittenData = mtw.getUnwrittenData();

                    if (!Writers.ContainsKey(key))
                    {
                        DBConnection conn = new DBConnection();

                        var newMtw = MtwConstructor(host, port, database, table);
                        Writers[key] = newMtw;
                        mtw.insertUnwrittenData(unwrittenData);
                    }
                    //mtw = MtwConstructor(host, port, database, table);

                    //Writers[key] = mtw;
                }
            }
        }
    }

    /// <summary>
    /// ddbServive封装了mtw写入逻辑，支持多表写入，如果表不存在，会在dolphindb中建立对应的表，但是database需要提前建好。需要提前配置好数据库的ip、端口、用户名以及密码。
    /// </summary>
    public class DdbService
    {
        public Func<object, BasicDeliverEventArgs, bool> Received;
        MtwConsumer MtwConsumer;
        RabbitMQ.Client.IModel RabbitChannel;
        string DolphinDBHost, DolphinDBUser, DolphinDBPassword;
        int DolphinDBPort;
        public static long allValue = 0;
        public static object lockers = new object();
        public Counts count_;
        public long time;
        /// <summary>
        /// 配置用户数据库的ip、端口、用户名以及密码。
        /// </summary>
        public void config(string host, int port, string user, string password, Counts count, long start)
        {
            DolphinDBHost = host;
            DolphinDBUser = user;
            DolphinDBPassword = password;
            DolphinDBPort = port;
            time = start;
            count_ = count;
        }

        public DdbService(RabbitMQ.Client.IModel RabbitChannel, string rabbitmqQueueName)
        {
            MtwConsumer = new MtwConsumer()
            {
                RabbitChannel = RabbitChannel,
                RabbitQueueName = rabbitmqQueueName,
                MtwConstructor = (DolphinDBIP, DolphinDBPort, DbName, TableName) =>
                {
                    var mtw = new MultithreadedTableWriter(DolphinDBIP, DolphinDBPort, DolphinDBUser, DolphinDBPassword, "dfs://" + DbName, TableName, false, false, null, 10000, (float)1, 1);


                    //写入成功后回调该函数。
                    Action<IVector> notifyOnSuccess = (IVector items) =>
                    {
                        BasicLongVector lastCol = (BasicLongVector)items;
                        var darray = lastCol.getdataArray();
                        //统计总时间
                        lock (lockers)
                        {
                            allValue += darray.Count;
                            long endsss = System.DateTime.Now.Ticks;
                            Console.WriteLine("总时间：" + ((endsss - time) / 10000) + " ms");
                            Console.WriteLine("总条数：" + allValue);
                        }
                        //在MTW写入成功并回调以后，开启ACK
                        for (int i = 0; i < darray.Count; i++)
                        {
                            var tag = (ulong)darray[i];
                            RabbitChannel.BasicAck(deliveryTag: tag, multiple: false);
                        }
                        return;
                    };
                    //设置回调函数
                    mtw.setNotifyOnSuccess(notifyOnSuccess, DATA_TYPE.DT_LONG);
                    return mtw;
                }
            };

            Received = (model, ea) =>
            {
                //Interlocked.Increment(ref received);
                var bytes = ea.Body.ToArray();
                BuleDataCommon? data =
                    JsonSerializer.Deserialize<BuleDataCommon>(new ReadOnlySpan<byte>(bytes));
                var tag = ea.DeliveryTag;
                RabbitChannel.BasicAck(deliveryTag: tag, multiple: false);

                count_.count++;

                if (count_.count >= 100000)
                {
                    // print consume time
                    long endsss = System.DateTime.Now.Ticks;
                    TimeSpan elapsedSpan = new TimeSpan(endsss - time);
                    //Console.WriteLine("Total time:" + (endsss - allstart) / 10000 + " ms,  Count:" + count_.count);

                    Console.WriteLine("Total time:" + elapsedSpan.TotalMilliseconds+ " ms,  Count:" + count_.count);
                }
                
                //此处为收到消息后通过DolphinBD的C-Sharp API的MultithreadedTableWriter类写入DolphinDB Server
                //单独统计rabbitmq发送数据的性能时，去掉这段代码

                /*
                if (data.TableName.Contains("FloatTable"))
                {
                    BuleDataFloat? value = JsonSerializer.Deserialize<BuleDataFloat>(new ReadOnlySpan<byte>(bytes));
                    MtwConsumer.insertDdb<float>(DolphinDBHost, DolphinDBPort, data.DbName, data.TableName, DolphinDBUser, DolphinDBPassword, data, value.Value, tag);
                }
                else if (data.TableName.Contains("StringTable"))
                {
                    BuleDataString? value = JsonSerializer.Deserialize<BuleDataString>(new ReadOnlySpan<byte>(bytes));
                    MtwConsumer.insertDdb<string>(DolphinDBHost, DolphinDBPort, data.DbName, data.TableName, DolphinDBUser, DolphinDBPassword, data, value.Value, tag);
                }
                else if (data.TableName.Contains("LongTable"))
                {
                    BuleDataLong? value = JsonSerializer.Deserialize<BuleDataLong>(new ReadOnlySpan<byte>(bytes));
                    MtwConsumer.insertDdb<long>(DolphinDBHost, DolphinDBPort, data.DbName, data.TableName, DolphinDBUser, DolphinDBPassword, data, value.Value, tag);
                }
                */

                return true;
            };
        }
    }

    public class Counts
    {
        public int count = 0;
    }

    public class Consumer
    {
        static string DolphinDBIP = "183.136.170.168";
        static int DolphinDBPort = 3302;
        static string DolphinDBUser = "admin";
        static string DolphinDBPassword = "123456";

        private string channelId;

        private IModel rabbitChan;

        private Counts count;

        public Consumer(string channelId, Counts count)
        {
            this.channelId = channelId;
            this.count = count;

            rabbitChan = new RabbitMQ.Client.ConnectionFactory()
            {
                UserName = "admin",
                Password = "123456",
                HostName = "183.136.170.168",
                Port = 5672
            }.CreateConnection().CreateModel();
        }
        public void run()
        {
            var queueName = "hello";

            rabbitChan.QueueDeclare(queue: queueName,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);
            rabbitChan.BasicQos(0, 10000, true);
            long start = System.DateTime.Now.Ticks;
            DdbService ddbService = new DdbService(rabbitChan, queueName);
            ddbService.config(DolphinDBIP, DolphinDBPort, DolphinDBUser, DolphinDBPassword,count, start);
            
                var consumer = new RabbitMQ.Client.Events.EventingBasicConsumer(rabbitChan);
                consumer.Received += async (ch, ea) =>
                {
                    ddbService.Received(ch, ea);
                };
                rabbitChan.BasicConsume(queue: queueName,
                             autoAck: false,
                             consumer: consumer,
                             consumerTag: channelId.ToString(),
                             noLocal: false,
                             exclusive: false,
                             arguments: null
                             );
            while (true)
            {
                System.Threading.Thread.Sleep(500000000);
            }
        }


        ~Consumer()
        {
            rabbitChan.Close();
        }
    }

    public class MultiConsumer
    {
        public static void run()
        {
            List<Thread> ths = new List<Thread>();
            List<Consumer> lcs = new List<Consumer>();
            List<Counts> css = new List<Counts>();


            int numThreads = 1;

            for(int i = 0; i < numThreads; i++) {
                Counts tc = new Counts();
                css.Add(tc);
            }

            for (int i = 0; i < numThreads; i++)
            {
                Consumer c = new Consumer(i.ToString(), css[i]);
                lcs.Add(c);
            }


            for (int i = 0; i < numThreads; i++)
            {
                Thread th = new Thread(lcs[i].run);
                ths.Add(th);
            }


            for (int i = 0; i < numThreads; i++)
            {
                ths[i].Start();
            }


            long starters = System.DateTime.Now.Ticks;
            while(true){
                int sum = 0; 
                for(int i = 0; i < css.Count; i++){
                    sum += css[i].count;
                }
                if(sum >= 100000){
                    Console.WriteLine("总条数：" + sum);
                    break;
                }
                Thread.Sleep(200);
            }
            
            long allends = System.DateTime.Now.Ticks;
            Console.WriteLine("总时间：" + ((allends - starters) / 10000) + " ms");

            for (int i = 0; i < numThreads; i++)
            {
                ths[i].Join();
            }
        }
    }

    public class Example
    {
        //public static List<DdbService> datasss = new List<DdbService>();
        public static void Main()
        {

            MultiConsumer.run();
            
            
        }
    }
}
