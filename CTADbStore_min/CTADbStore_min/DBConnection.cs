using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Threading;

namespace CTADbStore_min
{
    class DBConnection
    {

        //string connStr = "Server=192.168.2.134;User ID=root;Password=123456;Database=CTATest_deng;CharSet=utf8";
        //string connStr = "Server=192.168.2.180;User ID=root;Password=123456;Database=CTAHisDBSPFT;CharSet=utf8";

        string connStr = "";

        public DBConnection(string connStr)
        {
            this.connStr = connStr;
        }
        internal void executeSqlNoPrm(string excutSql)
        {
            MySqlConnection con = new MySqlConnection(this.connStr);
            if (excutSql == null || excutSql == "") { return; }
            try
            {
                con.Open();
                MySqlCommand cmd = new MySqlCommand(excutSql, con);
                cmd.ExecuteNonQuery();
                con.Close();
            }
            catch(TimeoutException e)
            {
                Console.WriteLine("There is a exception." + e);
                Console.WriteLine("出错的SQL语句" + excutSql);
                Log.AppendAllLines(new string[5] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), e.Message, "出错的SQL语句" + excutSql, "-----------------------------------" });

                con.Close();
                Thread.Sleep(3000);
                executeSqlNoPrm(excutSql);

            }
            catch (Exception e)
            {
                Console.WriteLine("There is a exception." + e);
                Console.WriteLine("出错的SQL语句" + excutSql);
                Log.AppendAllLines(new string[5] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), e.Message, "出错的SQL语句" + excutSql, "-----------------------------------" });

                con.Close();
            }

        }

        internal Object ExecuteScalar(string excutSql)
        {
            MySqlConnection con = new MySqlConnection(this.connStr);
            try
            {
                con.Open();
                MySqlCommand cmd = new MySqlCommand(excutSql, con);
                var column = cmd.ExecuteScalar();
                con.Close();
                return column;
            }
            catch (TimeoutException e)
            {
                Console.WriteLine("There is a exception." + e);
                Console.WriteLine("出错的SQL语句" + excutSql);
                Log.AppendAllLines(new string[5] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), e.Message, "出错的SQL语句" + excutSql, "-----------------------------------" });

                con.Close();
                Thread.Sleep(3000);
                executeSqlNoPrm(excutSql);
                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine("There is a exception." + e);
                Console.WriteLine("出错的SQL语句" + excutSql);
                Log.AppendAllLines(new string[5] { "-----------------------------------", "Time: " + DateTime.Now.ToString(), e.Message, "出错的SQL语句" + excutSql, "-----------------------------------" });
                con.Close();
                return 0;
            }

        }

    }
}
