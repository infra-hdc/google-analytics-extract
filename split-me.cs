using System;
using System.IO;
using System.Text.RegularExpressions; // https://metanit.com/sharp/tutorial/7.4.php
using System.Collections.Generic; // https://docs.microsoft.com/ru-ru/dotnet/api/system.collections.generic.list-1?view=netcore-3.0 -- только не список, а стек
using System.Linq;

namespace split_me
{

    public static class UriExtensions
    {
        private static readonly Regex queryStringRegex;
        static UriExtensions()
        {
            queryStringRegex = new Regex(@"[\?&](?<name>[^&=]+)=(?<value>[^&=]+)");
        }

        public static IEnumerable<KeyValuePair<string, string>> ParseQueryString(this string uri)
        {
            if (uri == null)
                throw new ArgumentException("uri");

            var matches = queryStringRegex.Matches(uri);
            for (int i = 0; i < matches.Count; i++)
            {
                var match = matches[i];
                yield return new KeyValuePair<string, string>(match.Groups["name"].Value, match.Groups["value"].Value);
            }
        }
    }

    class MainClass
    {
        public static int Main(string[] args)
        {
            string fn_argv; // имя файла csv
            try
            {
                fn_argv = args[0]; // читаем имя входного файла, из первого аргумента командной строки
            }
            catch (IndexOutOfRangeException) // если аргумента нет
            {
                // то вывод помощи
                Console.WriteLine("ОШИБКА, пропущен аргумент командной строки");
                Console.WriteLine("Формат команды: split-me.exe <что-то-там>.csv");
                Console.WriteLine("Где <что-то-там>.csv -- входной файл");
                Console.WriteLine("На выходе -- два файла:");
                Console.WriteLine("Выходной файл с авторскими: <что-то-там> - с авторскими -.csv");
                Console.WriteLine("Выходной файл без авторских: <что-то-там> - без авторских -.csv");
                return(1); // и выход из программы
            }
            string fn_pattern  = @"^(?<fname>[^\.]+)\.(?:csv|CSV)$"; // имя файла должно оканчиваться на .csv
            Regex fn_regex = new Regex(fn_pattern); // делаем объект для работы регулярного выражения с нашим шаблоном
            MatchCollection fn_matches = fn_regex.Matches(fn_argv); // натравливаем регулярку на нашу текущую строку с именем файла
            if (fn_matches.Count != 1) {
                Console.WriteLine("ОШИБКА, аргумент командной строки должен быть вида *.csv");
                return(1);
            }
            GroupCollection fn_groups = fn_matches[0].Groups;
            string base_fname = fn_groups["fname"].Value.ToString(); // сохраняем имя файла "без расширения"

            StreamReader sr;
            try
            {
                sr = new StreamReader(fn_argv, System.Text.Encoding.Default);
            }
            catch (FileNotFoundException) // если файле не найден
            {
                Console.WriteLine("ОШИБКА, входной файл не найден");
                return(1); // то выход из программы               
            }
            
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
            
            // начало вывода в файлы: открываем на запись (старое содержимое удаляется) и выводим шапку
            StreamWriter s_avtorskimi_sw = new StreamWriter(s_avtorskimi_out_fname);
            s_avtorskimi_sw.WriteLine(s_avtorskimi_head);
            StreamWriter bez_avtorskikh_sw = new StreamWriter(bez_avtorskikh_out_fname);
            bez_avtorskikh_sw.WriteLine(bez_avtorskikh_head);
          
            Dictionary<string, int> s_avtorskimi_aggr = new Dictionary<string, int>(); // количество обращений c авторскими, сагрегированных по <ID-заказа>
            Dictionary<string, int> bez_avtorskikh_aggr = new Dictionary<string, int>(); // количество обращений без авторских, сагрегированных по парам <фонд>_<пин>

            
            // для общей суммы выдачи, для проверки
            int s_avtorskimi_read_sum=0, bez_avtorskikh_read_sum=0, total_sum=0;
            
            //для поиска подстроки
            string stroka_flag = @"/Bookreader/Viewer?";
            
            // ввод данных - begin
            string line; // текущая строка
            while ((line = sr.ReadLine()) != null) // цикл по всем строкам входного файла, пока не EOF
                {
                    if (line.Length > 1) if (line[0] == ',') // если совпадение нашлось
                    {
                        total_sum = Int32.Parse(line.Substring(1));  // считываем сумму
                        break; // и выходим из цикла чтения входного файла
                    }
                    string[] splitted_line = line.Split(',');
                    if (splitted_line.Length < 2) continue;
                    if (!splitted_line[0].Contains(stroka_flag)) { continue; }
                    var url_1 = splitted_line[0].ParseQueryString().ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                    int a = Int32.Parse(splitted_line[1]);
                    string b = url_1.ContainsKey("bookID") ? url_1["bookID"] : null;
                    string c = null;
                    //string c = url_1["OrderId"];
                    if (b != null) // если совпадение нашлось
                    {
                        if (!bez_avtorskikh_aggr.ContainsKey(b)) // если в массиве для результатов извлечения данных еще нет этой пары <fund>_<pin>
                            {
                                // добавляем объект для новой пары <fund>_<pin>
                                bez_avtorskikh_aggr.Add(b, a);
                            } else
                            {   // иначе прибавляем к объекту пары <fund>_<pin>
                                bez_avtorskikh_aggr[b] += a;
                            }
                        bez_avtorskikh_read_sum += a; // прибавляем к общей сумме без авторских
                    } else
                    {
                        c = url_1.ContainsKey("OrderId") ? url_1["OrderId"] : null;
                        if (c != null) // если совпадение нашлось
                        {
                            if (!s_avtorskimi_aggr.ContainsKey(c)) // если в массиве для результатов извлечения данных еще нету <orderid>
                                {
                                    // добавляем объект для нового <orderid>
                                    s_avtorskimi_aggr.Add(c, a);
                                } else
                                {   // иначе прибавляем к объекту <orderid>
                                    s_avtorskimi_aggr[c] += a;
                                }
                            s_avtorskimi_read_sum += a; // прибавляем к общей сумме с авторскими
                        } else {
                            Console.WriteLine("Ошибочный URL: {0}, его NUM: {1}", splitted_line[0], a);
                        }
                    }
                }
            // ввод данных - end
            
            // вывод данных - begin
            int s_avtorskimi_write_sum=0, bez_avtorskikh_write_sum=0;
            foreach (KeyValuePair<string, int> kvp in bez_avtorskikh_aggr.OrderByDescending(key => key.Value))
            {
                bez_avtorskikh_write_sum += kvp.Value;
                bez_avtorskikh_sw.WriteLine("{0},{1}", kvp.Key.ToString().Replace("_", ","), kvp.Value.ToString());
            }
            foreach (KeyValuePair<string, int> kvp in s_avtorskimi_aggr.OrderByDescending(key => key.Value))
            {
                s_avtorskimi_write_sum += kvp.Value;
                s_avtorskimi_sw.WriteLine("{0},{1}", kvp.Key.ToString(), kvp.Value.ToString());
            }
            // вывод данных - end
            
            // закрываем все файлы
            sr.Close();
            bez_avtorskikh_sw.Close();
            s_avtorskimi_sw.Close();
            
            // под конец -- различная метаинформация
            int total_read_sum = s_avtorskimi_read_sum + bez_avtorskikh_read_sum;
            int total_write_sum = s_avtorskimi_write_sum + bez_avtorskikh_write_sum;
            Console.WriteLine("Подсчитанная сумма на этапе ввода, с авторскими: {0}",s_avtorskimi_read_sum);
            Console.WriteLine("Подсчитанная сумма на этапе ввода, без авторских: {0}",bez_avtorskikh_read_sum);
            Console.WriteLine("Общая подсчитанная сумма, на этапе ввода: {0}",total_read_sum);
            Console.WriteLine("Подсчитанная сумма на этапе вывода, с авторскими: {0}",s_avtorskimi_write_sum);
            Console.WriteLine("Подсчитанная сумма на этапе вывода, без авторских: {0}",bez_avtorskikh_write_sum);
            Console.WriteLine("Общая подсчитанная сумма, на этапе вывода: {0}",total_write_sum);
            Console.WriteLine("Общая прочитанная из входного файла сумма: {0}",total_sum);
            if ((total_sum == total_read_sum) && (total_sum == total_write_sum))
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
