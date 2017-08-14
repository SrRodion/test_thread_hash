using System;
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
    public class FileAndHash
    {
        public string FileName { get; set; }
        public string HashMD5 { get; set; }
        public string Errors { get; set; }
    }

    class Program
    {
        static private Object thisLock = new Object();
        static BlockingCollection<string> bcFiles = new BlockingCollection<string>();
        static BlockingCollection<FileAndHash> bcFH = new BlockingCollection<FileAndHash>();
        static Boolean LockThread = true;
        static int i = 1;

        // устанавливаем метод обратного вызова
        static TimerCallback tm = new TimerCallback(ExecStatus);
        // создаем таймер
        static Timer timer = new Timer(tm, null, 100, 1000);

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
                    bcFiles.Add(eFile);

                bcFiles.CompleteAdding();
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }

            Console.Read();
        }


        static void ComputeMD5Checksum()
        {
            string bcFile;

            while (!bcFiles.IsCompleted)
            {
                if (bcFiles.TryTake(out bcFile))
                {
                    try
                    {
                        using (FileStream fs = File.OpenRead(bcFile))
                        {
                            MD5 md5 = new MD5CryptoServiceProvider();
                            byte[] fileData = new byte[fs.Length];
                            fs.Read(fileData, 0, (int)fs.Length);
                            byte[] checkSum = md5.ComputeHash(fileData);
                            string result = BitConverter.ToString(checkSum).Replace("-", String.Empty);

                            bcFH.Add(new FileAndHash() { FileName = bcFile, HashMD5 = result, Errors = "" });
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        bcFH.Add(new FileAndHash() { FileName = bcFile, HashMD5 = "", Errors = e.Message });
                    }
                    i++;

                }
            }

            lock(thisLock)
            { 
                if (LockThread)
                    LockThread = false;
                else
                    bcFH.CompleteAdding();
            }
        }

        static void WriteBD()
        {
            FileAndHash FH;
            
            using (SqlConnection cn = new SqlConnection())
            {
                cn.ConnectionString = @"Data Source = (LocalDB)\MSSQLLocalDB; AttachDbFilename = D:\Rodion\Documents\TEST_FH.mdf; Integrated Security = True; Connect Timeout = 30";
                cn.Open();

                while(!bcFH.IsCompleted)
                {
                    if (bcFH.TryTake(out FH))
                    {
                        var cmd = new SqlCommand("INSERT INTO [TEST_FH].[Files] ([FileNameFull], [FileHash], [FileErrors]) VALUES (@fName, @fHash, @fError)", cn);
                        cmd.Parameters.AddWithValue("@fName", FH.FileName);
                        cmd.Parameters.AddWithValue("@fHash", FH.HashMD5);
                        cmd.Parameters.AddWithValue("@fError", FH.Errors);
                        cmd.ExecuteNonQuery();
                    }
                }
                
                Console.WriteLine("Поток {0} завершил работу!", Thread.CurrentThread.Name);
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
