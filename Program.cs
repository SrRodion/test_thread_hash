using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Security.Cryptography;
using System.Data.SqlClient;

namespace test_thread_hash
{
    public class SyncEvents
    {
        public SyncEvents()
        {
            _newItemEvent = new AutoResetEvent(false);
            _exitThreadEvent = new ManualResetEvent(false);
            _eventArray = new WaitHandle[2];
            _eventArray[0] = _newItemEvent;
            _eventArray[1] = _exitThreadEvent;
        }

        public EventWaitHandle ExitThreadEvent
        {
            get { return _exitThreadEvent; }
        }
        public EventWaitHandle NewItemEvent
        {
            get { return _newItemEvent; }
        }
        public WaitHandle[] EventArray
        {
            get { return _eventArray; }
        }

        private EventWaitHandle _newItemEvent;
        private EventWaitHandle _exitThreadEvent;
        private WaitHandle[] _eventArray;
    }

    public class FileAndHash
    {
        public string FileName { get; set; }
        public string HashMD5 { get; set; }
        public string Errors { get; set; }
    }

    class Program
    {
        static private Object thisLock = new Object();
        static Queue<string> bcFiles = new Queue<string>();
        static Queue<FileAndHash> bcFH = new Queue<FileAndHash>();
        static int i = 1;
        private static ManualResetEvent mre = new ManualResetEvent(false);
        private static AutoResetEvent are = new AutoResetEvent(true);

        // устанавливаем метод обратного вызова
        static TimerCallback tm = new TimerCallback(ExecStatus);
        // создаем таймер
        static Timer timer = new Timer(tm, null, 100, 1000);

        static bool bcFilesComplete = false;
        static bool bcFHComplete = false;
        

        static void Main(string[] args)
        {
            Thread thread_MD5_1 = new Thread(ComputeMD5Checksum);
            thread_MD5_1.Name = "thread_MD5_1";
            thread_MD5_1.Start();

            Thread thread_MD5_2 = new Thread(ComputeMD5Checksum);
            thread_MD5_2.Name = "thread_MD5_2";
            thread_MD5_2.Start();

            Thread thread_BD = new Thread(WriteBD);
            thread_BD.Name = "thread_BD";
            thread_BD.Start();

            try
            {
                var EnumFiles = Directory.EnumerateFiles(@"D:\TEST", "*", SearchOption.AllDirectories);
                foreach (string eFile in EnumFiles)
                {
                    lock (((ICollection)bcFiles).SyncRoot)
                    {
                        bcFiles.Enqueue(eFile);
                        //Console.WriteLine("Main! {0}", eFile);
                        mre.Set();
                    }
                    
                }
                //Console.WriteLine("bcFiles готов!");

                while (!bcFilesComplete)
                {
                    lock (((ICollection)bcFiles).SyncRoot)
                        if (!bcFiles.Any())
                            bcFilesComplete = true;
                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }

            thread_MD5_1.Join();
            thread_MD5_2.Join();
            while (!bcFHComplete)
            {
                lock (((ICollection)bcFH).SyncRoot)
                    if (!bcFH.Any())
                        bcFHComplete = true;
            }

            thread_BD.Join();

            Console.WriteLine("Потоки завершили работу!");
            Console.Read();
        }


        static void ComputeMD5Checksum()
        {
            string bcFile;
            mre.WaitOne();
            while (!bcFilesComplete)
            {
                are.WaitOne();
                lock (((ICollection)bcFiles).SyncRoot)
                {
                    if (bcFiles.Any())
                    {
                        bcFile = bcFiles.Dequeue();
                        //Console.WriteLine("{0} - {1}", Thread.CurrentThread.Name, bcFile);
                        try
                        {
                            using (FileStream fs = File.OpenRead(bcFile))
                            {
                                MD5 md5 = new MD5CryptoServiceProvider();
                                byte[] fileData = new byte[fs.Length];
                                fs.Read(fileData, 0, (int)fs.Length);
                                byte[] checkSum = md5.ComputeHash(fileData);
                                string result = BitConverter.ToString(checkSum).Replace("-", String.Empty);
                                lock (((ICollection)bcFH).SyncRoot)
                                {
                                    bcFH.Enqueue(new FileAndHash() { FileName = bcFile, HashMD5 = result, Errors = "" });
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                            lock (((ICollection)bcFH).SyncRoot)
                            {
                                bcFH.Enqueue(new FileAndHash() { FileName = bcFile, HashMD5 = "", Errors = e.Message });
                            }
                        }
                        i++;
                    }
                }
                are.Set();
            }
        }

        static void WriteBD()
        {
            FileAndHash FH;

            using (SqlConnection cn = new SqlConnection())
            {
                cn.ConnectionString = @"Data Source = (LocalDB)\MSSQLLocalDB; AttachDbFilename = D:\Rodion\Documents\TEST_FH.mdf; Integrated Security = True; Connect Timeout = 30";
                cn.Open();

                while (!bcFHComplete)
                {
                    lock (((ICollection)bcFH).SyncRoot)
                        FH = bcFH.Dequeue();

                    //Console.WriteLine("{0} - {1}", Thread.CurrentThread.Name, FH.HashMD5);

                    var cmd = new SqlCommand("INSERT INTO [TEST_FH].[Files] ([FileNameFull], [FileHash], [FileErrors]) VALUES (@fName, @fHash, @fError)", cn);
                    cmd.Parameters.AddWithValue("@fName", FH.FileName);
                    cmd.Parameters.AddWithValue("@fHash", FH.HashMD5);
                    cmd.Parameters.AddWithValue("@fError", FH.Errors);
                    cmd.ExecuteNonQuery();
                }
                cn.Close();
                timer.Dispose();
            }
        }

        static void ExecStatus(object obj)
        {
            Console.WriteLine("Количество обработанных файлов: {0}, Количество файлов для обработки: {1}", i, bcFiles.Count());
        }
    }
}
