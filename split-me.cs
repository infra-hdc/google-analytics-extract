using System;
using System.IO;
using System.Text.RegularExpressions; // https://metanit.com/sharp/tutorial/7.4.php
using System.Collections.Generic; // https://docs.microsoft.com/ru-ru/dotnet/api/system.collections.generic.list-1?view=netcore-3.0 -- только не список, а стек
using System.Linq;
using NReco.Csv;

// External CSV Library -- BEGIN

/*
 * NReco CSV library (https://github.com/nreco/csv/)
 * Copyright 2017-2018 Vitaliy Fedorchenko
 * Distributed under the MIT license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//using System;
//using System.Collections.Generic;
//using System.Linq;
using System.Text;
//using System.IO;

namespace NReco.Csv {

	/// <summary>
	/// Fast and memory efficient implementation of CSV reader (3x times faster than CsvHelper).
	/// </summary>
	/// <remarks>API is similar to CSVHelper CsvReader.</remarks>
	public class CsvReader {

		public string Delimiter { get; private set; }
		int delimLength;

		/// <summary>
		/// Size of the circular buffer. Buffer size limits max length of the CSV line that can be processed. 
		/// </summary>
		/// <remarks>Default buffer size is 32kb.</remarks>
		public int BufferSize { get; set; } = 32768;

		/// <summary>
		/// If true start/end spaces are excluded from field values (except values in quotes). True by default.
		/// </summary>
		public bool TrimFields { get; set; } = true;

		TextReader rdr;

		public CsvReader(TextReader rdr) : this(rdr, ",") {
		}

		public CsvReader(TextReader rdr, string delimiter) {
			this.rdr = rdr;
			Delimiter = delimiter;
			delimLength = delimiter.Length;

			if (delimLength == 0)
				throw new ArgumentException("Delimiter cannot be empty.");
		}

		char[] buffer = null;
		int bufferLength;
		int bufferLoadThreshold;
		int lineStartPos = 0;
		int actualBufferLen = 0;
		List<Field> fields = null;
		int fieldsCount = 0;
		int linesRead = 0;

		private int ReadBlockAndCheckEof(char[] buffer, int start, int len, ref bool eof) {
			if (len == 0)
				return 0;
			var read = rdr.ReadBlock(buffer, start, len);
			if (read < len)
				eof = true;
			return read;
		}

		private bool FillBuffer() {
			var eof = false;
			var toRead = bufferLength - actualBufferLen;
			if (toRead>=bufferLoadThreshold) {
				int freeStart = (lineStartPos + actualBufferLen) % buffer.Length;
				if (freeStart>=lineStartPos) {
					actualBufferLen += ReadBlockAndCheckEof(buffer, freeStart, buffer.Length - freeStart, ref eof);
					if (lineStartPos>0)
						actualBufferLen += ReadBlockAndCheckEof(buffer, 0, lineStartPos, ref eof);
				} else {
					actualBufferLen += ReadBlockAndCheckEof(buffer, freeStart, toRead, ref eof);
				}
			}
			return eof;
		}

		private string GetLineTooLongMsg() {
			return String.Format("CSV line #{1} length exceedes buffer size ({0})", BufferSize, linesRead);
		}

		private int ReadQuotedFieldToEnd(int start, int maxPos, bool eof, ref int escapedQuotesCount) {
			int pos = start;
			int chIdx;
			char ch;
			for (; pos<maxPos; pos++) {
				chIdx = pos < bufferLength ? pos : pos % bufferLength;
				ch = buffer[chIdx];
				if (ch=='\"') {
					bool hasNextCh = (pos + 1) < maxPos;
					if (hasNextCh && buffer[(pos + 1) % bufferLength] == '\"') {
						// double quote inside quote = just a content
						pos++;
						escapedQuotesCount++;
					} else {
						return pos;
					}
				}
			}
			if (eof) {
				// this is incorrect CSV as quote is not closed
				// but in case of EOF lets ignore that
				return pos-1;
			}
			throw new InvalidDataException(GetLineTooLongMsg());
		}

		private bool ReadDelimTail(int start, int maxPos, ref int end) {
			int pos;
			int idx;
			int offset = 1;
			for (; offset<delimLength; offset++) {
				pos = start + offset;
				idx = pos < bufferLength ? pos : pos % bufferLength;
				if (pos >= maxPos || buffer[idx] != Delimiter[offset])
					return false;
			}
			end = start + offset -1;
			return true;
		}

		private Field GetOrAddField(int startIdx) {
			fieldsCount++;
			while (fieldsCount > fields.Count)
				fields.Add(new Field());
			var f = fields[fieldsCount-1];
			f.Reset(startIdx);
			return f;
		}

		public int FieldsCount {
			get {
				return fieldsCount;
			}
		}

		public string this[int idx] {
			get {
				if (idx < fieldsCount) {
					var f = fields[idx];
					return fields[idx].GetValue(buffer);
				}
				return null;
			}
		}

		public int GetValueLength(int idx) {
			if (idx < fieldsCount) {
				var f = fields[idx];
				return f.Quoted ? f.Length-f.EscapedQuotesCount : f.Length;
			}
			return -1;
		}

		public void ProcessValueInBuffer(int idx, Action<char[],int,int> handler) {
			if (idx < fieldsCount) {
				var f = fields[idx];
				if ((f.Quoted && f.EscapedQuotesCount > 0) || f.End>=bufferLength) {
					var chArr = f.GetValue(buffer).ToCharArray();
					handler(chArr, 0, chArr.Length);
				} else if (f.Quoted) {
					handler(buffer, f.Start + 1, f.Length - 2);
				} else { 
					handler(buffer, f.Start, f.Length);
				}
			}
		}

		public bool Read() {
			if (fields == null) {
				fields = new List<Field>();
				fieldsCount = 0;
			}
			if (buffer==null) {
				bufferLoadThreshold = Math.Min(BufferSize, 8192);
				bufferLength = BufferSize + bufferLoadThreshold;
				buffer = new char[bufferLength];
				lineStartPos = 0;
				actualBufferLen = 0;
			}

			var eof = FillBuffer();

			fieldsCount = 0;
			if (actualBufferLen <= 0) {
				return false; // no more data
			}
			linesRead++;

			int maxPos = lineStartPos + actualBufferLen;
			int charPos = lineStartPos;

			var currentField = GetOrAddField(charPos);
			bool ignoreQuote = false;
			char delimFirstChar = Delimiter[0];
			bool trimFields = TrimFields;

			int charBufIdx;
			char ch;
			for (; charPos < maxPos; charPos++) {
				charBufIdx = charPos<bufferLength ? charPos : charPos % bufferLength;
				ch = buffer[charBufIdx];
				switch (ch) {
					case '\"':
						if (ignoreQuote) {
							currentField.End = charPos;
						} else if (currentField.Quoted || currentField.Length>0) {
							// current field already is quoted = lets treat quotes as usual chars
							currentField.End = charPos;
							currentField.Quoted = false;
							ignoreQuote = true;
						} else { 
							var endQuotePos = ReadQuotedFieldToEnd(charPos + 1, maxPos, eof, ref currentField.EscapedQuotesCount);
							currentField.Start = charPos;
							currentField.End = endQuotePos;
							currentField.Quoted = true;
							charPos = endQuotePos;
						}
						break;
					case '\r':
						if ((charPos + 1) < maxPos && buffer[(charPos + 1) % bufferLength] == '\n') {
							// \r\n handling
							charPos++;
						}
						// in some files only \r used as line separator - lets allow that
						charPos++;
						goto LineEnded;
					case '\n':
						charPos++;
						goto LineEnded;
					default:
						if (ch == delimFirstChar && (delimLength == 1 || ReadDelimTail(charPos, maxPos, ref charPos))) {
							currentField = GetOrAddField(charPos+1);
							ignoreQuote = false;
							continue;
						}
						// space
						if (ch==' ' && trimFields) {
							continue; // do nothing
						}

						// content char
						if (currentField.Length==0) {
							currentField.Start = charPos;
						}

						if (currentField.Quoted) {
							// non-space content after quote = treat quotes as part of content
							currentField.Quoted = false;
							ignoreQuote = true;
						}
						currentField.End = charPos;
						break;
				}

			}
			if (!eof) {
				// line is not finished, but whole buffer was processed and not EOF
				throw new InvalidDataException(GetLineTooLongMsg());
			}
		LineEnded:
			actualBufferLen -= charPos - lineStartPos;
			lineStartPos = charPos%bufferLength;

			if (fieldsCount==1 && fields[0].Length==0) {
				// skip empty lines
				return Read();
			}

			return true;
		}


		internal sealed class Field {
			internal int Start;
			internal int End;
			internal int Length {
				get { return End - Start +1; }
			}
			internal bool Quoted;
			internal int EscapedQuotesCount;
			string cachedValue = null;

			internal Field() {
			}

			internal Field Reset(int start) {
				Start = start;
				End = start-1;
				Quoted = false;
				EscapedQuotesCount = 0;
				cachedValue = null;
				return this;
			}
			
			internal string GetValue(char[] buf) {
				if (cachedValue==null) {
					cachedValue = GetValueInternal(buf);
				}
				return cachedValue;
			}

			string GetValueInternal(char[] buf) {
				if (Quoted) {
					var s = Start + 1;
					var lenWithoutQuotes = Length - 2;
					var val = lenWithoutQuotes > 0 ? GetString(buf, s, lenWithoutQuotes) : String.Empty;
					if (EscapedQuotesCount>0)
						val = val.Replace("\"\"", "\"");
					return val;
				}
				var len = Length;
				return len>0 ? GetString(buf, Start, len) : String.Empty;
			}

			private string GetString(char[] buf, int start, int len) {
				var bufLen = buf.Length;
				start = start<bufLen ? start : start % bufLen;
				var endIdx = start + len -1;
				if (endIdx>= bufLen) {
					var prefixLen = buf.Length - start;
					var prefix = new string(buf, start, prefixLen);
					var suffix = new string(buf, 0, len - prefixLen);
					return prefix + suffix;
				}
				return new string(buf, start, len);
			}

		}

	}


}

// External CSV Library -- END



namespace split_me
{
    
    // http://markimarta.ru/dev/asp-net-c/parsing-url-i-vytaskivanie-parametrov-get-zaprosa-cherez-regulyarnye-vyrazheniya-na-yazyke-c.html
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
            
            // начало вывода в файлы: открываем на запись и выводим шапку; ничего из старого вывода не должно быть перезаписано, см. выше проверку на существование файлов
            StreamWriter s_avtorskimi_sw = new StreamWriter(s_avtorskimi_out_fname);
            s_avtorskimi_sw.WriteLine(s_avtorskimi_head);
            StreamWriter bez_avtorskikh_sw = new StreamWriter(bez_avtorskikh_out_fname);
            bez_avtorskikh_sw.WriteLine(bez_avtorskikh_head);
          
            Dictionary<string, int> s_avtorskimi_aggr = new Dictionary<string, int>(); // количество обращений c авторскими, сагрегированных по <ID-заказа>
            Dictionary<string, int> bez_avtorskikh_aggr = new Dictionary<string, int>(); // количество обращений без авторских, сагрегированных по парам <фонд>_<пин>

            // для общей суммы выдачи, для проверки
            int s_avtorskimi_read_sum=0, bez_avtorskikh_read_sum=0, error_read_sum=0, total_sum=0;
            
            //для поиска подстроки
            const string stroka_flag = @"/Bookreader/Viewer?";
            
            // ввод данных - begin
            using (var streamRdr = sr) {
                var csvReader = new CsvReader(streamRdr, ",");
                while (csvReader.Read()) {
                    //for (int i=0; i<csvReader.FieldsCount; i++) {
                    //    string val = csvReader[i];
                    //}
                    if (csvReader.FieldsCount != 2) continue;
                    if (csvReader[0].Length == 0) {
                        total_sum = Int32.Parse(csvReader[1]);  // считываем сумму
                        break; // и выходим из цикла чтения входного файла
                    }
                    if (!csvReader[0].Contains(stroka_flag)) { continue; }
                    var url_1 = csvReader[0].ParseQueryString().ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                    int a = Int32.Parse(csvReader[1]);
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
                            error_read_sum += a;
                            Console.WriteLine("Ошибочный URL: {0}, его NUM: {1}", csvReader[0], a);
                        }
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
            int total_read_sum = s_avtorskimi_read_sum + bez_avtorskikh_read_sum + error_read_sum;
            int total_write_sum = s_avtorskimi_write_sum + bez_avtorskikh_write_sum;
            l_error=false;
            Console.WriteLine("Общая прочитанная из входного файла сумма: {0}",total_sum);
            Console.WriteLine("Подсчитанная сумма на этапе ввода, с авторскими: {0}",s_avtorskimi_read_sum);
            Console.WriteLine("Подсчитанная сумма на этапе ввода, без авторских: {0}",bez_avtorskikh_read_sum);
            Console.WriteLine("Подсчитанная сумма на этапе ввода, ошибочных выдач: {0}",error_read_sum);
            Console.WriteLine("Общая подсчитанная сумма, на этапе ввода: {0}",total_read_sum);
            if (total_read_sum == total_sum)
            {
                Console.WriteLine("ОК, суммы при вводе совпадают с суммой, указанной во входном файле");
            } else
            {
                Console.WriteLine("ОШИБКА, суммы при вводе не совпадают с суммой, указанной во входном файле");
                l_error=true;
            }
            Console.WriteLine("Подсчитанная сумма на этапе вывода, с авторскими: {0}",s_avtorskimi_write_sum);
            Console.WriteLine("Подсчитанная сумма на этапе вывода, без авторских: {0}",bez_avtorskikh_write_sum);
            Console.WriteLine("Общая подсчитанная сумма, на этапе вывода: {0}",total_write_sum);
            if (total_write_sum == total_sum)
            {
                Console.WriteLine("ОК, суммы при выводе совпадают с суммой, указанной во входном файле");
            } else
            {
                Console.WriteLine("ОШИБКА, суммы при выводе не совпадают с суммой, указанной во входном файле");
                l_error=true;
            }
            if (l_error)
            {
                Console.WriteLine("Обработка завершена с ошибками");
                return(1);
            } else {
                Console.WriteLine("Обработка завершена без ошибок");
                return(0);
            }
        }
    }
}
