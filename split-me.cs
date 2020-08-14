using System;
using System.IO;
using System.Text.RegularExpressions; // https://metanit.com/sharp/tutorial/7.4.php
using System.Collections.Generic; // https://docs.microsoft.com/ru-ru/dotnet/api/system.collections.generic.list-1?view=netcore-3.0 -- только не список, а стек
using System.Linq;

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
                Console.WriteLine("ОШИБКА, пропущен аргумент командной строки");
                Console.WriteLine("Формат команды: split-me.exe <что-то-там>.csv");
                Console.WriteLine("Где <что-то-там>.csv -- входной файл");
                Console.WriteLine("На выходе -- два файла:");
                Console.WriteLine("Выходной файл с авторскими: <что-то-там> - с авторскими -.csv");
                Console.WriteLine("Выходной файл без авторских: <что-то-там> - без авторских -.csv");
                return(1);
            }
            string fn_pattern  = @"^(?<fname>[^\.]+)\.(?:csv|CSV)$";
            Regex fn_regex = new Regex(fn_pattern); // делаем объект для работы регулярного выражения с нашим шаблоном
            MatchCollection fn_matches = fn_regex.Matches(fn_argv); // натравливаем регулярку на нашу текущую строку файла
            if (fn_matches.Count != 1) {
                Console.WriteLine("ОШИБКА, аргумент командной строки должен быть вида *.csv");
                return(1);
            }
            GroupCollection fn_groups = fn_matches[0].Groups;
            string base_fname = fn_groups["fname"].Value.ToString();

            StreamReader sr;
            try
            {
                sr = new StreamReader(fn_argv, System.Text.Encoding.Default);
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("ОШИБКА, входной файл не найден");
                return(1);               
            }

            // для парсинга входного файла
            string s_avtorskimi_pattern = @"^\/Bookreader\/Viewer\?OrderId=(?<orderid>\d+)[^\,]+\,(?<num>\d+)$";
            Regex s_avtorskimi_regex = new Regex(s_avtorskimi_pattern);
            string bez_avtorskikh_pattern = @"^\/Bookreader\/Viewer\?bookID=(?<fund_pin>\w+_\d+)[^\,]+\,(?<num>\d+)$";
            Regex bez_avtorskikh_regex = new Regex(bez_avtorskikh_pattern);
            string total_read_sum_pattern = @"^\,(?<num>\d+)$";
            Regex total_read_sum_regex = new Regex(total_read_sum_pattern);
            
            Dictionary<string, int> s_avtorskimi_aggr = new Dictionary<string, int>(); // количество обращений c авторскими, сагрегированных по ID заказа
            Dictionary<string, int> bez_avtorskikh_aggr = new Dictionary<string, int>(); // количество обращений без авторских, сагрегированных по парам <фонд>_<пин>

            // для вывода в файлы:
            string s_avtorskimi_out_fname = base_fname+" - с авторскими -.csv";                
            string bez_avtorskikh_out_fname = base_fname+" - без авторских -.csv";
            // шапки файлов для вывода
            string s_avtorskimi_head = "ORDERID,NUM";
            string bez_avtorskikh_head = "FUND,PIN,NUM";
            
            // если выходные файлы есть, то выходим по ошибке
            bool l_error=false;
            if (File.Exists(s_avtorskimi_out_fname))
            {
                Console.WriteLine("ОШИБКА, выходной файл"+Environment.NewLine+"{0}"+Environment.NewLine+"существует. Переименуйте или удалите его",s_avtorskimi_out_fname);
                l_error=true;
            }
            if (File.Exists(bez_avtorskikh_out_fname))
            {
                Console.WriteLine("ОШИБКА, выходной файл"+Environment.NewLine+"{0}"+Environment.NewLine+"существует. Переименуйте или удалите его",bez_avtorskikh_out_fname);
                l_error=true;
            }
            if (l_error) return(1);
            
            // начало вывода в файлы
            StreamWriter s_avtorskimi_sw = new StreamWriter(s_avtorskimi_out_fname);
            s_avtorskimi_sw.WriteLine(s_avtorskimi_head);
            StreamWriter bez_avtorskikh_sw = new StreamWriter(bez_avtorskikh_out_fname);
            bez_avtorskikh_sw.WriteLine(bez_avtorskikh_head);
            
            // для общей суммы выдачи, для проверки
            int s_avtorskimi_sum=0, bez_avtorskikh_sum=0, total_sum=0, total_read_sum=0;
            
            // извлечение данных - begin
            string line; // текущая строка
            while ((line = sr.ReadLine()) != null) // цикл по всем строкам, пока не EOF
                {
                    MatchCollection matches;
                    GroupCollection groups;
                    matches = bez_avtorskikh_regex.Matches(line); // натравливаем регулярку на нашу текущую строку файла
                    if (matches.Count == 1) // если совпадение нашлось
                    {
                        groups = matches[0].Groups;
                        if (!bez_avtorskikh_aggr.ContainsKey(groups["fund_pin"].Value.ToString())) // если в массиве для результатов извлечения данных еще нет этой пары
                            {
                                // добавляем объект для пары
                                bez_avtorskikh_aggr.Add(groups["fund_pin"].Value.ToString(), Int32.Parse(groups["num"].Value));
                            } else
                            {
                                bez_avtorskikh_aggr[groups["fund_pin"].Value.ToString()] += Int32.Parse(groups["num"].Value);
                            }
                        bez_avtorskikh_sum += Int32.Parse(groups["num"].Value);
                    } else
                    {
                        matches = s_avtorskimi_regex.Matches(line);
                        if (matches.Count == 1) // если совпадение нашлось
                        {
                            groups = matches[0].Groups;
                            if (!s_avtorskimi_aggr.ContainsKey(groups["orderid"].Value.ToString())) // если в массиве для результатов извлечения данных еще нет этой пары
                                {
                                    // добавляем объект для пары
                                    s_avtorskimi_aggr.Add(groups["orderid"].Value.ToString(), Int32.Parse(groups["num"].Value));
                                } else
                                {
                                    s_avtorskimi_aggr[groups["orderid"].Value.ToString()] += Int32.Parse(groups["num"].Value);
                                }
                            s_avtorskimi_sum += Int32.Parse(groups["num"].Value);
                        } else
                        {
                            matches = total_read_sum_regex.Matches(line);
                            if (matches.Count == 1) // если совпадение нашлось
                            {
                                groups = matches[0].Groups;
                                total_read_sum = Int32.Parse(groups["num"].Value);
                                break;
                            }
                        }
                    }
                }
            // извлечение данных - end
            
            // вывод данных - begin
            foreach (KeyValuePair<string, int> kvp in bez_avtorskikh_aggr.OrderByDescending(key => key.Value))
            {
                bez_avtorskikh_sw.WriteLine("{0},{1}", kvp.Key.ToString().Replace("_", ","), kvp.Value.ToString());
            }
            foreach (KeyValuePair<string, int> kvp in s_avtorskimi_aggr.OrderByDescending(key => key.Value))
            {
                s_avtorskimi_sw.WriteLine("{0},{1}", kvp.Key.ToString(), kvp.Value.ToString());
            }
            // вывод данных - end
            
            // закрываем все файлы
            sr.Close();
            bez_avtorskikh_sw.Close();
            s_avtorskimi_sw.Close();
            
            // под конец -- различная метаинформация
            total_sum = s_avtorskimi_sum + bez_avtorskikh_sum;
            Console.WriteLine("Подсчитанная сумма с авторскими: {0}",s_avtorskimi_sum);
            Console.WriteLine("Подсчитанная сумма без авторских: {0}",bez_avtorskikh_sum);
            Console.WriteLine("Общая подсчитанная сумма: {0}",total_sum);
            Console.WriteLine("Общая прочитанная из входного файла сумма: {0}",total_read_sum);
            if (total_sum == total_read_sum)
            {
                Console.WriteLine("ВСЁ ОК, суммы совпадают");
                return(0);
            } else
            {
                Console.WriteLine("ОШИБКА, суммы не совпадают");
                return(1);
            }
        }
    }
}
