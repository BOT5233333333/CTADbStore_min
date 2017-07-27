using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CTADbStore_if_ic_ih
{
    class SPTxtToSqlClass
    {
        private string rootPath;
        private string dbName;
        private DBConnection conn;

        private int numAllFiles;
        private static int numFilesFinish;

        private string folderFinishRecordPath = @"E:\测试记录\文件夹记录" + DateTime.Now.ToString("yyyyMMdd HH-mm-ss") + ".txt";
        private string fileFinishRecordPath = @"E:\测试记录\文件记录" + DateTime.Now.ToString("yyyyMMdd HH-mm-ss") + ".txt";
        //private string folderFinishRecordPath = @"E:\期货行情数据导入完成记录\文件夹记录" + DateTime.Now.ToString("yyyyMMdd HH-mm-ss") + ".txt";
        //private string fileFinishRecordPath = @"E:\期货行情数据导入完成记录\文件记录" + DateTime.Now.ToString("yyyyMMdd HH-mm-ss") + ".txt";

        private string connStr = "Server=192.168.2.134;User ID=root;Password=123456;Database=ctatest_deng;CharSet=utf8";
        //private string connStr = "Server=192.168.2.181;User ID=root;Password=123456;Database=CTAHisDBSPFT2016;CharSet=utf8";
        public SPTxtToSqlClass(string rootPath, string dbName, int numAllFiles)
        {
            this.rootPath = rootPath;
            this.dbName = dbName;
            this.conn = new DBConnection(connStr);
            numFilesFinish = 0;
            this.numAllFiles = numAllFiles;
        }

        internal void MainFunc()
        {
            try
            {
                DirectoryInfo rootDir = new DirectoryInfo(this.rootPath);

                foreach (var monthDir in rootDir.GetDirectories())
                {
                    List<Task<int>> tasks = new List<Task<int>>();
                    Console.WriteLine("*************");
                    Console.WriteLine("文件夹:" + monthDir.FullName + "开始");
                    Console.WriteLine("时间:" + DateTime.Now);
                    Console.WriteLine("*************");
                    foreach (var dir in monthDir.GetDirectories()
                        .Where(d => (d.Name == "if" || d.Name == "ic" || d.Name == "ih") && d.GetFiles().Length > 0))
                    {
                        Task<int> t = new Task<int>(new Func<object, int>(DataProcessDir), dir, TaskCreationOptions.LongRunning);
                        t.Start();
                        tasks.Add(t);
                    }
                    Task.WaitAll(tasks.ToArray());
                    Log.AppendAllLines(this.folderFinishRecordPath, new string[4] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), "导入完成:" + monthDir.FullName, "-----------------------------------" });

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("There is a exception." + e.Message);
                Log.AppendAllLines(new string[4] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), e.Message, "-----------------------------------" });
            }
        }

        private int DataProcessDir(object obj)
        {
            DirectoryInfo dir = (DirectoryInfo)obj;
            FileInfo[] files = dir.GetFiles();
            if (files.Length > 0)
            {
                foreach (var file in files)
                {
                    try
                    {
                        DataProcessFunc(file, file.Name, dir.Name);
                    }
                    catch (IndexOutOfRangeException e)
                    {
                        Console.WriteLine("There is a exception." + e.Message);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("There is a exception." + e.Message);
                        Log.AppendAllLines(new string[5] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), "文件:" + file.FullName, e.Message, "-----------------------------------" });
                    }

                }
            }
            return 0;
        }


        private void DataProcessFunc(FileInfo file, string fName, string dirName)
        {
            FileStream fs = file.Open(FileMode.Open, FileAccess.Read);
            StreamReader sr = new StreamReader(fs, Encoding.UTF8);

            string tableName = "CTA_" + fName.Substring(0, 4 + dirName.Count()).ToUpper() + "_TBL";
            Console.WriteLine("Table Name is: " + tableName);
            TableProcess(tableName, this.dbName);

            Console.WriteLine("--------");
            Console.WriteLine("开始导入该文件的数据：" + file.FullName);
            Console.WriteLine("--------");
            List<string> querys = new List<string>();
            string line = null;

            int count = 0;
            string query = InitSqlString(tableName);
            while ((line = sr.ReadLine()) != null)
            {
                line = line.Replace("None", "null");

                string[] tmpList = line.Split(',');

                if (tmpList.Length != 53)
                {
                    continue;
                }

                CTASPWordDefine DataUnit = new CTASPWordDefine();

                DataUnit.IFCD = tmpList[0];
                DataUnit.TDATE = tmpList[1];
                DataUnit.TTIME = tmpList[2];
                DataUnit.UPDATEMILLISEC = tmpList[3];
                DataUnit.CP = tmpList[4];
                DataUnit.CHG = tmpList[5];
                DataUnit.CHGPCT = tmpList[6];
                DataUnit.CQ = tmpList[7];
                DataUnit.OC = tmpList[8];
                DataUnit.S5 = tmpList[9];
                DataUnit.S4 = tmpList[10];
                DataUnit.S3 = tmpList[11];
                DataUnit.S2 = tmpList[12];
                DataUnit.S1 = tmpList[13];
                DataUnit.B1 = tmpList[14];
                DataUnit.B2 = tmpList[15];
                DataUnit.B3 = tmpList[16];
                DataUnit.B4 = tmpList[17];
                DataUnit.B5 = tmpList[18];
                DataUnit.SV5 = tmpList[19];
                DataUnit.SV4 = tmpList[20];
                DataUnit.SV3 = tmpList[21];
                DataUnit.SV2 = tmpList[22];
                DataUnit.SV1 = tmpList[23];
                DataUnit.BV1 = tmpList[24];
                DataUnit.BV2 = tmpList[25];
                DataUnit.BV3 = tmpList[26];
                DataUnit.BV4 = tmpList[27];
                DataUnit.BV5 = tmpList[28];
                DataUnit.BS = tmpList[29];
                DataUnit.BSRATIO = tmpList[30];
                DataUnit.PRECLOSEPRC = tmpList[31];
                DataUnit.OPENPRC = tmpList[32];
                DataUnit.HP = tmpList[33];
                DataUnit.LP = tmpList[34];
                DataUnit.CLOSEPRC = tmpList[35];
                DataUnit.UPPERLMTPRC = tmpList[36];
                DataUnit.LOWERLMTPRC = tmpList[37];
                DataUnit.TQ = tmpList[38];
                DataUnit.TM = tmpList[39];
                DataUnit.PREOPNINTRST = tmpList[40];
                DataUnit.OPNINTRST = tmpList[41];
                DataUnit.PRESTLMTPRC = tmpList[42];
                DataUnit.STLMTPRC = tmpList[43];
                DataUnit.PREDELTA = tmpList[44];
                DataUnit.DELTA = tmpList[45];
                DataUnit.SETTLEGROUPID = tmpList[46];
                DataUnit.SETTLEID = tmpList[47];
                DataUnit.IFLXID = tmpList[48];
                DataUnit.IFLXNAME = tmpList[49];
                DataUnit.UNIX = tmpList[50];
                DataUnit.MATKET = tmpList[51];

                if (++count >= 200)
                {
                    query += SqlStringConcat(DataUnit);
                    querys.Add(query);
                    query = InitSqlString(tableName);
                    count = 0;
                }
                else
                {
                    query += SqlStringConcat(DataUnit) + ",";
                }

            }

            if (count != 0)
            {
                query = query.TrimEnd(',');
                querys.Add(query);
            }

            BatchInsert_MySql bm = new BatchInsert_MySql(this.connStr);
            bm.ExecuteSqlTran(querys);
            Console.WriteLine("-------------");
            Console.WriteLine("导入完成：" + file.FullName);
            Log.AppendAllLines(this.fileFinishRecordPath, new string[4] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), "导入完成:" + file.FullName, "-----------------------------------" });
            ++numFilesFinish;
            Console.WriteLine("导入进度：" + ((double)numFilesFinish / (double)numAllFiles) * 100 + "%");
            Console.WriteLine("-------------");
        }

        private string InitSqlString(string tableName)
        {
            return "INSERT INTO " + tableName
                 + "(IFCD, TDATE ,TTIME ,UPDATEMILLISEC ,CP , CHG , CHGPCT , CQ , OC , "
                            + "S5 , S4 , S3 , S2 , S1, B1 , B2 , B3 , B4 , B5 ,"
                            + "SV5 , SV4 , SV3 , SV2 , SV1 , BV1 , BV2 , BV3 , BV4 , BV5 , BS , BSRATIO ,"
                            + "PRECLOSEPRC , OPENPRC , HP , LP , CLOSEPRC , UPPERLMTPRC , LOWERLMTPRC , "
                            + "TQ , TM , PREOPNINTRST , OPNINTRST , PRESTLMTPRC , STLMTPRC , PREDELTA , DELTA ,"
                            + "SETTLEGROUPID , SETTLEID , IFLXID , IFLXNAME, UNIX, MATKET)VALUES";
        }

        private string SqlStringConcat(CTASPWordDefine DataUnit)
        {
            return string.Format("({0},{1},{2},{3},{3},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20,{21},{22},{23},{24},{25},{26},{27},{28},{29},{30},{31},{32},{33},{34},{35},{36},{37},{38},{39},{40},{41},{42},{43},{44},{45},{46},{47},{48},{49},{50},{51},{52})"
                DataUnit.IFCD, DataUnit.TDATE, DataUnit.TTIME, DataUnit.UPDATEMILLISEC, DataUnit.CP, DataUnit.CHG, DataUnit.CHGPCT, DataUnit.CQ, DataUnit.OC, DataUnit.S5, DataUnit.S4, DataUnit.S3, DataUnit.S2, DataUnit.S1, DataUnit.B1, DataUnit.B2, DataUnit.B3, DataUnit.B4, DataUnit.B5, DataUnit.SV5, DataUnit.SV4, DataUnit.SV3, DataUnit.SV2, DataUnit.SV1, DataUnit.BV1, DataUnit.BV2, DataUnit.BV3, DataUnit.BV4, DataUnit.BV5, DataUnit.BS
                                                                                                   , DataUnit.BSRATIO, DataUnit.PRECLOSEPRC, DataUnit.OPENPRC, DataUnit.HP, DataUnit.LP, DataUnit.CLOSEPRC, DataUnit.UPPERLMTPRC, DataUnit.LOWERLMTPRC, DataUnit.TQ, DataUnit.TM, DataUnit.PREOPNINTRST, DataUnit.OPNINTRST, DataUnit.PRESTLMTPRC, DataUnit.STLMTPRC, DataUnit.PREDELTA, DataUnit.DELTA
                                                                                                                                                  , DataUnit.SETTLEGROUPID, DataUnit.SETTLEID, DataUnit.IFLXID, DataUnit.IFLXNAME, DataUnit.UNIX, DataUnit.MATKET);
        }

        private void TableProcess(string tableName, string dbName)
        {
            string SqlQryTbl = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA =" + "\"" + dbName + "\""
                    + " and TABLE_NAME = " + "\"" + tableName + "\"";
            Object res = this.conn.ExecuteScalar(SqlQryTbl);
            string SqlCreateTbl = "";
            if (res == null)
            {
                Console.WriteLine("创建表:" + tableName);
                SqlCreateTbl = "CREATE TABLE " + tableName + "(recordID INT NOT NULL auto_increment,IFCD NVARCHAR(6),TDATE NVARCHAR(8),"
                            + "TTIME NVARCHAR(6),UPDATEMILLISEC INT,CP DOUBLE, CHG DOUBLE, CHGPCT DOUBLE, CQ INT, OC NVARCHAR(9), "
                            + "S5 DOUBLE, S4 DOUBLE, S3 DOUBLE, S2 DOUBLE, S1 DOUBLE, B1 DOUBLE, B2 DOUBLE, B3 DOUBLE, B4 DOUBLE, B5 DOUBLE,"
                            + "SV5 INT, SV4 INT, SV3 INT, SV2 INT, SV1 INT, BV1 INT, BV2 INT, BV3 INT, BV4 INT, BV5 INT, BS NVARCHAR(1), BSRATIO DOUBLE,"
                            + "PRECLOSEPRC DOUBLE, OPENPRC DOUBLE, HP DOUBLE, LP DOUBLE, CLOSEPRC DOUBLE, UPPERLMTPRC DOUBLE, LOWERLMTPRC DOUBLE, "
                            + "TQ INT, TM DOUBLE, PREOPNINTRST INT, OPNINTRST INT, PRESTLMTPRC DOUBLE, STLMTPRC DOUBLE, PREDELTA DOUBLE, DELTA DOUBLE,"
                            + "SETTLEGROUPID NVARCHAR(9), SETTLEID INT, IFLXID NVARCHAR(6), IFLXNAME NVARCHAR(8), UNIX BIGINT, MATKET CHAR(4)"
                            + ", PRIMARY KEY (recordID)) ENGINE = InnoDB DEFAULT CHARSET = utf8; ";
            }

            this.conn.ExecuteNonQuery(SqlCreateTbl);
        }
    }
}
