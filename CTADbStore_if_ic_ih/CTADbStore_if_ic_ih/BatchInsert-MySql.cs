using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySql.Data;
using System.Data;

namespace CTADbStore_if_ic_ih
{
    class BatchInsert_MySql
    {
        //public static string connStr = "Server=192.168.2.180;User ID=root;Password=123456;Database=CTAHisDBSPFT;CharSet=utf8";
        //public static string connStr = "Server=192.168.2.134;User ID=root;Password=123456;Database=CTATest_deng;CharSet=utf8";
        public static string connStr = "";

        public BatchInsert_MySql(string constr)
        {
            connStr = constr;
        }

        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。
        /// </summary>mysql数据库
        /// <param name="SQLStringList">多条SQL语句</param>
        public void ExecuteSqlTran(List<string> SQLStringList)
        {
            using (MySqlConnection conn = new MySqlConnection(connStr))
            {
                conn.Open();
                MySqlCommand cmd = new MySqlCommand();
                cmd.Connection = conn;
                MySqlTransaction tx = conn.BeginTransaction();
                cmd.Transaction = tx;
                try
                {
                    for (int n = 0; n<SQLStringList.Count; n++)
                    {
                        string strsql = SQLStringList[n].ToString();
                        if (strsql.Trim().Length > 1)
                        {
                            cmd.CommandText = strsql;
                            try
                            {
                                
                                cmd.ExecuteNonQuery();
                            }
                            catch (Exception e)
                            {
                                //tx.Rollback();
                                Console.WriteLine("Exception: " + e.Message);
                                Log.AppendAllLines(new string[5] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), e.Message, "出错的SQL语句" + strsql, "-----------------------------------" });

                            }

                        }
                        //后来加上的
                        if ((n > 0 && n % 500 == 0) || n == SQLStringList.Count - 1)
                        {
                            try
                            {
                                tx.Commit();
                                tx = conn.BeginTransaction();
                            }
                            catch (Exception e)
                            {
                                Log.AppendAllLines(new string[5] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), e.Message, "事务Commit出错，出错的SQL语句" + strsql, "-----------------------------------" });
                            }

                        }
                    }
                    //tx.Commit();//原来一次性提交
                }
                catch (System.Data.SqlClient.SqlException e)
                {
                    //tx.Rollback();
                    Console.WriteLine("Exception: " + e.Message);
                    Log.AppendAllLines(new string[4] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), e.Message, "-----------------------------------" });

                }
            }
        }
    }
}
