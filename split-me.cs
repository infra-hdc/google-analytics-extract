using System;
using System.IO;
using System.Text.RegularExpressions; // https://metanit.com/sharp/tutorial/7.4.php
using System.Collections.Generic; // https://docs.microsoft.com/ru-ru/dotnet/api/system.collections.generic.list-1?view=netcore-3.0 -- только не список, а стек

namespace split_me
{
    class MainClass
    {

        public static int Main(string[] args)
        {
            string fn_argv; // имя файла csv
            try
            {
                fn_argv = args[0]; // читаем входную дату, из первого аргумента командной строки
            }
            catch (IndexOutOfRangeException)
            {
                Console.WriteLine("Пропущен аргумент командной строки");
                return(1);
            }
            string fn_pattern  = @"^(?<fname>[^\.]+)\.(csv|CSV)$";
            Regex fn_regex = new Regex(fn_pattern); // делаем объект для работы регулярного выражения с нашим шаблоном
            MatchCollection fn_matches = fn_regex.Matches(fn_argv); // натравливаем регулярку на нашу текущую строку файла
            if (fn_matches.Count != 1) {
                Console.WriteLine("Аргумент командной строки должен быть вида *.csv");
                return(1);
            }
            GroupCollection fn_groups = fn_matches[0].Groups;
            string base_fname = fn_groups["fname"].Value.ToString();
            //Console.WriteLine(base_fname);

            StreamReader sr;
            try
            {
                sr = new StreamReader(fn_argv, System.Text.Encoding.Default);
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("Входной файл не найден");
                return(1);               
            }

            // для парсинга входного файла
            string s_avtorskimi_pattern = @"^\/Bookreader\/Viewer\?bookID=(?<fund>\w+)_(?<pin>\d+)[^\,]+\,(?<num>\d+)$";
            Regex s_avtorskimi_regex = new Regex(s_avtorskimi_pattern);
            string bez_avtorskikh_pattern = @"^\/Bookreader\/Viewer\?OrderId=(?<orderid>\d+)[^\,]+\,(?<num>\d+)$";
            Regex bez_avtorskikh_regex = new Regex(bez_avtorskikh_pattern);

            // для вывода в файлы:
            string s_avtorskimi_out_fname = base_fname+" - с авторскими -.csv";
            string s_avtorskimi_head = "ORDERID,NUM";
            string bez_avtorskikh_out_fname = base_fname+" - без авторских -.csv";
            string bez_avtorskikh_head = "FUND,PIN,NUM";
            
            // для вывода в файлы
            StreamWriter s_avtorskimi_sw = new StreamWriter(s_avtorskimi_out_fname);
            s_avtorskimi_sw.WriteLine(s_avtorskimi_head);
            StreamWriter bez_avtorskikh_sw = new StreamWriter(bez_avtorskikh_out_fname);
            bez_avtorskikh_sw.WriteLine(bez_avtorskikh_head);
            
            // для общей суммы выдачи, для проверки
            int s_avtorskimi_sum=0, bez_avtorskikh_sum=0;
            
            string line; // текущая строка
            while ((line = sr.ReadLine()) != null) // цикл по всем строкам, пока не EOF
                {
                    MatchCollection matches;
                    GroupCollection groups;
                    matches = s_avtorskimi_regex.Matches(line); // натравливаем регулярку на нашу текущую строку файла
                    if (matches.Count == 1) // если совпадение нашлось
                    {
                        groups = matches[0].Groups;
                        bez_avtorskikh_sw.WriteLine("{0},{1},{2}",groups["fund"].Value.ToString(),groups["pin"].Value.ToString(),groups["num"].Value.ToString());
                        bez_avtorskikh_sum += Int32.Parse(groups["num"].Value);
                    } else
                    {
                        matches = bez_avtorskikh_regex.Matches(line);
                        if (matches.Count == 1) // если совпадение нашлось
                        {
                            groups = matches[0].Groups;
                            s_avtorskimi_sw.WriteLine("{0},{1}",groups["orderid"].Value.ToString(),groups["num"].Value.ToString());
                            s_avtorskimi_sum += Int32.Parse(groups["num"].Value);
                        }
                    }
                    //Console.WriteLine(line);
                }
            sr.Close();
            bez_avtorskikh_sw.Close();
            s_avtorskimi_sw.Close();
            Console.WriteLine("Общая сумма: {0}",s_avtorskimi_sum+bez_avtorskikh_sum);
            return(0);
        }
    }
}