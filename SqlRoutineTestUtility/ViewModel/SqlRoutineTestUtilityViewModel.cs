using SqlRoutineTestUtility.Command;
using SqlRoutineTestUtility.Helpers;
using StoredProcedureResultComparer.Helpers;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Data;
using System.Windows.Input;

namespace SqlRoutineTestUtility.ViewModel
{
    public enum SqlRoutineType
    {
        StoredProcedure,
        TableValuedFunction,
        ScalarValuedFunction
    }

    public class SqlRoutineTestUtilityViewModel : INotifyPropertyChanged
    {
        private readonly string _defaultSchema = ConfigurationManager.AppSettings["DefaultSchema"];
        private readonly string _connectionString = ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;
        private readonly int _commandTimeoutForGettingInputParameters = int.Parse(ConfigurationManager.AppSettings["CommandTimeoutForGettingInputParameters"]);
        private readonly int _commandTimeoutFor1stSqlRoutine = int.Parse(ConfigurationManager.AppSettings["CommandTimeoutFor1stSqlRoutine"]);
        private readonly int _commandTimeoutFor2ndSqlRoutine = int.Parse(ConfigurationManager.AppSettings["CommandTimeoutFor2ndSqlRoutine"]);

        private CancellationTokenSource _cancellationTokenSource;
        private static Dictionary<string, SqlDbType> _sqlDbTypeEnumByDatabaseEngineType;
        private readonly string _processingMessage = $"Processing...{string.Empty}";

        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        private SqlRoutineType _selectedSqlRoutineType;
        private string _queryToGetInputParameters;
        private string _firstSchemaName;
        private string _firstSqlRoutineName;
        private string _secondSchemaName;
        private string _secondSqlRoutineName;
        private bool _isComparingOutputParameters;
        private bool _isBusy;
        private string _busyContent;

        public SqlRoutineType SelectedSqlRoutineType
        {
            get => _selectedSqlRoutineType;
            set
            {
                _selectedSqlRoutineType = value;
                OnPropertyChanged("SelectedSqlRoutineType");
            }
        }

        public string QueryToGetInputParameters
        {
            get => _queryToGetInputParameters;
            set
            {
                _queryToGetInputParameters = value.Trim();
                OnPropertyChanged("QueryToGetInputParameters");
            }
        }

        public string FirstSchemaName
        {
            get => _firstSchemaName;
            set
            {
                _firstSchemaName = value.Trim();
                OnPropertyChanged("FirstSchemaName");
            }
        }

        public string FirstSqlRoutineName
        {
            get => _firstSqlRoutineName;
            set
            {
                _firstSqlRoutineName = value.Trim();
                OnPropertyChanged("FirstSqlRoutineName");
            }
        }

        public string SecondSchemaName
        {
            get => _secondSchemaName;
            set
            {
                _secondSchemaName = value.Trim();
                OnPropertyChanged("SecondSchemaName");
            }
        }

        public string SecondSqlRoutineName
        {
            get => _secondSqlRoutineName;
            set
            {
                _secondSqlRoutineName = value.Trim();
                OnPropertyChanged("SecondSqlRoutineName");
            }
        }

        public bool IsComparingOutputParameters
        {
            get => _isComparingOutputParameters;
            set
            {
                _isComparingOutputParameters = value;
                OnPropertyChanged("IsComparingOutputParameters");
            }
        }

        public bool IsBusy
        {
            get => _isBusy;
            set
            {
                _isBusy = value;
                OnPropertyChanged("IsBusy");
            }
        }

        public string BusyContent
        {
            get => _busyContent;
            set
            {
                _busyContent = value;
                OnPropertyChanged("BusyContent");
            }
        }

        public SqlRoutineTestUtilityViewModel()
        {
            var dataTypeMappingCsvFilePath = ConfigurationManager.AppSettings["DataTypeMappingCsvFilePath"];
            var linesToSkipInDataTypeMappingCsvFile = int.Parse(ConfigurationManager.AppSettings["LinesToSkipInDataTypeMappingCsvFile"]);
            _sqlDbTypeEnumByDatabaseEngineType = SqlServerDataTypeMapper.GetSqlDbTypeEnumByDatabaseEngineType(dataTypeMappingCsvFilePath, ',', linesToSkipInDataTypeMappingCsvFile);

            IsBusy = false;

            FirstSchemaName
                = SecondSchemaName
                = _defaultSchema;
        }

        public ICommand ResetInputsCommand => new RelayCommand(ExecuteResetInputsCommand, (_ => true));

        private void ExecuteResetInputsCommand(object parameter)
        {
            QueryToGetInputParameters
                = FirstSqlRoutineName
                = SecondSqlRoutineName
                = string.Empty;

            FirstSchemaName
                = SecondSchemaName
                = _defaultSchema;
        }

        public ICommand CancelCommand => new RelayCommand(ExecuteCancelCommand, (_ => true));

        private void ExecuteCancelCommand(object parameter)
        {
            _cancellationTokenSource.Cancel();
        }

        public ICommand CompareCommand => new RelayCommand(ExecuteCompareCommand, (_ => true));

        private async void ExecuteCompareCommand(object parameter)
        {
            try
            {
                if (string.IsNullOrEmpty(QueryToGetInputParameters)
                    || string.IsNullOrEmpty(FirstSchemaName)
                    || string.IsNullOrEmpty(SecondSchemaName)
                    || string.IsNullOrEmpty(FirstSqlRoutineName)
                    || string.IsNullOrEmpty(SecondSqlRoutineName))
                {
                    MessageBox.Show("Input fields require values.", string.Empty, MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                IsBusy = true;
                BusyContent = _processingMessage;

                _cancellationTokenSource = new CancellationTokenSource();
                var token = _cancellationTokenSource.Token;

                var message = await Task.Factory.StartNew(() => ValidateResults(token), token);

                MessageBox.Show(message, string.Empty, MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                while (ex.InnerException != null)
                {
                    ex = ex.InnerException;
                }

                MessageBox.Show(ex.Message, string.Empty, MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                IsBusy = false;
            }
        }

        private string ValidateResults(CancellationToken token)
        {
            var isTrackingOutputParameters = IsComparingOutputParameters && SelectedSqlRoutineType == SqlRoutineType.StoredProcedure;

            var commandTextToGetParameterNames = ObtainCommandTextToGetParameterNames(isTrackingOutputParameters);

            using (var connectionToGetInputParameters = new SqlConnection(_connectionString))
            {
                connectionToGetInputParameters.Open();

                var parametersInSqlRoutine = GetParametersFromSqlRoutine(connectionToGetInputParameters, FirstSchemaName, FirstSqlRoutineName, commandTextToGetParameterNames);
                var anotherSetOfParametersInSqlRoutine = GetParametersFromSqlRoutine(connectionToGetInputParameters, SecondSchemaName, SecondSqlRoutineName, commandTextToGetParameterNames);
                if (!AreTablesTheSame(parametersInSqlRoutine, anotherSetOfParametersInSqlRoutine))
                {
                    return "Failed to proceed: the 2 SQL routines have different parameters !";
                }

                var rowCount = 0;

                using (var commandToGetInputParameters = new SqlCommand(QueryToGetInputParameters, connectionToGetInputParameters)
                {
                    CommandTimeout = _commandTimeoutForGettingInputParameters
                })
                using (var inputParameterReader = commandToGetInputParameters.ExecuteReader())
                using (var connectionToGetResultSets = new SqlConnection(_connectionString))
                using (var dataAdapter = new SqlDataAdapter())
                {
                    BusyContent = $"{_processingMessage}\n\nGetting input parameters for the SQL routines...";

                    var inputParameterTableColumnNames = GetInputParameterTableColumnNames(inputParameterReader);

                    BusyContent = _processingMessage;

                    connectionToGetResultSets.Open();

                    while (inputParameterReader.Read())
                    {
                        if (token.IsCancellationRequested)
                        {
                            IsBusy = false;

                            return $"Terminated by user !";
                        }

                        rowCount++;

                        using (var transactionToGetResultSets = connectionToGetResultSets.BeginTransaction())
                        using (SqlCommand cmd1 = new SqlCommand()
                        {
                            Connection = connectionToGetResultSets,
                            CommandTimeout = _commandTimeoutFor1stSqlRoutine,
                            Transaction = transactionToGetResultSets
                        }, cmd2 = new SqlCommand()
                        {
                            Connection = connectionToGetResultSets,
                            CommandTimeout = _commandTimeoutFor2ndSqlRoutine,
                            Transaction = transactionToGetResultSets
                        })
                        {
                            try
                            {
                                var outputParametersFrom1stSqlCommand = new List<SqlParameter>();
                                var outputParametersFrom2ndSqlCommand = new List<SqlParameter>();

                                ConfigureSqlCommand(cmd1, SelectedSqlRoutineType, $"{FirstSchemaName}.{FirstSqlRoutineName}", parametersInSqlRoutine);
                                ConfigureSqlCommand(cmd2, SelectedSqlRoutineType, $"{SecondSchemaName}.{SecondSqlRoutineName}", parametersInSqlRoutine);

                                foreach (DataRow dataRow in parametersInSqlRoutine.Rows)
                                {
                                    var parameterName = dataRow.Field<string>(Placeholder.ParameterName);

                                    if (inputParameterTableColumnNames.Contains(parameterName))
                                    {
                                        var parameterValue = inputParameterReader[parameterName];
                                        var sqlDbType = _sqlDbTypeEnumByDatabaseEngineType[dataRow.Field<string>(Placeholder.DatabaseEngineType)];
                                        var precision = dataRow.Field<byte>(Placeholder.Precision);
                                        var scale = dataRow.Field<byte>(Placeholder.Scale);

                                        var sqlParameter1 = new SqlParameter(parameterName, parameterValue)
                                        {
                                            SqlDbType = sqlDbType,
                                            Precision = precision,
                                            Scale = scale
                                        };

                                        var sqlParameter2 = new SqlParameter(parameterName, parameterValue)
                                        {
                                            SqlDbType = sqlDbType,
                                            Precision = precision,
                                            Scale = scale
                                        };

                                        if (isTrackingOutputParameters && dataRow.Field<bool>(Placeholder.IsOutputParameter))
                                        {
                                            sqlParameter1.Direction
                                                = sqlParameter2.Direction
                                                = ParameterDirection.Output;

                                            sqlParameter1.Size
                                                = sqlParameter2.Size
                                                = -1;

                                            outputParametersFrom1stSqlCommand.Add(sqlParameter1);
                                            outputParametersFrom2ndSqlCommand.Add(sqlParameter2);
                                        }

                                        cmd1.Parameters.Add(sqlParameter1);
                                        cmd2.Parameters.Add(sqlParameter2);
                                    }
                                }

                                var dataSet1 = GetDataSet(dataAdapter, cmd1);
                                var dataSet2 = GetDataSet(dataAdapter, cmd2);

                                if (dataSet1.Tables.Count != dataSet2.Tables.Count)
                                {
                                    return $"The 2 DataSets returned by the SQL routines contain different DataTable count " +
                                        $"with the following input parameters : {GetFormattedParameters(cmd1.Parameters)}";
                                }

                                if (isTrackingOutputParameters
                                    && !outputParametersFrom1stSqlCommand.Select(p => p.SqlValue)
                                        .SequenceEqual(outputParametersFrom2ndSqlCommand.Select(p => p.SqlValue)))
                                {
                                    return $"The 2 SQL routines generate different output parameter values : {GetFormattedParameters(cmd1.Parameters)}";
                                }

                                for (var i = 0; i < dataSet1.Tables.Count; i++)
                                {
                                    if (!AreTablesTheSame(dataSet1.Tables[i], dataSet2.Tables[i]))
                                    {
                                        return $"The 2 SQL routines generate different results " +
                                            $"with the following input parameters : {GetFormattedParameters(cmd1.Parameters)}";
                                    }
                                }
                            }
                            finally
                            {
                                transactionToGetResultSets.Rollback();

                                BusyContent = $"{_processingMessage}\n\nFinished {rowCount} comparisons";
                            }
                        }
                    }

                    BusyContent = $"Done.\n\nFinished {rowCount} comparisons";

                    if (rowCount == 0)
                    {
                        return "Unable to perform any comparison based on the given inputs.";
                    }

                    return "The 2 SQL routines generate the same results based on the given inputs.";
                }
            }
        }

        private string ObtainCommandTextToGetParameterNames(bool isFetchingOutputParameters)
        {
            return $@"
                SELECT sp.name AS {Placeholder.ParameterName}
                        ,type_name(sp.user_type_id) AS {Placeholder.DatabaseEngineType}
                        ,sp.precision AS {Placeholder.Precision}
                        ,sp.scale AS {Placeholder.Scale}"
                + (isFetchingOutputParameters ? Environment.NewLine + $@",sp.is_output AS {Placeholder.IsOutputParameter}" : string.Empty)
                + $@"
                FROM sys.objects so
                    INNER JOIN sys.parameters sp
                        ON sp.object_id = so.object_id
                    INNER JOIN sys.types ty
                        ON ty.system_type_id = sp.system_type_id AND ty.user_type_id = sp.user_type_id
                    INNER JOIN sys.schemas smas
                        ON smas.schema_id = so.schema_id
                WHERE smas.name = {Placeholder.SchemaName}
                    AND so.name = {Placeholder.SqlRoutineName}
                    AND
                    (
                        so.type = 'P'
                        OR
                        (so.type IN ('FN', 'IF', 'TF') AND is_output = 0)
                    )
            ";
        }

        private DataTable GetParametersFromSqlRoutine(SqlConnection connection, string schemaName, string sqlRoutineName, string cmdText)
        {
            var dataTable = new DataTable();

            using (var sqlCommand = new SqlCommand(cmdText, connection))
            {
                sqlCommand.Parameters.AddWithValue(Placeholder.SqlRoutineName, sqlRoutineName);
                sqlCommand.Parameters.AddWithValue(Placeholder.SchemaName, schemaName);

                dataTable.Load(sqlCommand.ExecuteReader());
            }

            return dataTable;
        }

        private static List<string> GetInputParameterTableColumnNames(SqlDataReader reader)
        {
            var columnNames = new List<string>();

            for (var i = reader.FieldCount - 1; i >= 0; i--)
            {
                columnNames.Add(reader.GetName(i));
            }

            return columnNames;
        }

        private void ConfigureSqlCommand(SqlCommand sqlCommand, SqlRoutineType sqlRoutineType, string fullRoutineName, DataTable parametersInSqlRoutine)
        {
            switch (sqlRoutineType)
            {
                case SqlRoutineType.StoredProcedure:
                    {
                        sqlCommand.CommandType = CommandType.StoredProcedure;
                        sqlCommand.CommandText = fullRoutineName;
                        break;
                    }
                case SqlRoutineType.TableValuedFunction:
                    {
                        sqlCommand.CommandType = CommandType.Text;
                        sqlCommand.CommandText
                            = $"SELECT * FROM {fullRoutineName} "
                            + $"({string.Join(",", parametersInSqlRoutine.AsEnumerable().Select(r => r.Field<string>(Placeholder.ParameterName)))})";
                        break;
                    }
                case SqlRoutineType.ScalarValuedFunction:
                    {
                        sqlCommand.CommandType = CommandType.Text;
                        sqlCommand.CommandText
                            = $"SELECT {fullRoutineName} "
                            + $"({string.Join(",", parametersInSqlRoutine.AsEnumerable().Select(r => r.Field<string>(Placeholder.ParameterName)))})";
                        break;
                    }
                default:
                    throw new InvalidEnumArgumentException();
            }
        }

        private static DataSet GetDataSet(SqlDataAdapter dataAdapter, SqlCommand sqlCommand)
        {
            var dataSet = new DataSet();

            dataAdapter.SelectCommand = sqlCommand;
            dataAdapter.Fill(dataSet);

            return dataSet;
        }

        private static string GetFormattedParameters(SqlParameterCollection parameters)
        {
            var formattedParameters = new StringBuilder();
            foreach (SqlParameter p in parameters)
            {
                formattedParameters.Append($"\n{p.ParameterName} = {p.SqlValue}");
            }

            return formattedParameters.ToString();
        }

        private static bool AreTablesTheSame(DataTable dt1, DataTable dt2)
        {
            if (dt1 == null && dt2 == null)
            {
                return true;
            }

            if (dt1 == null || dt2 == null)
            {
                return false;
            }

            if (dt1.Rows.Count != dt2.Rows.Count || dt1.Columns.Count != dt2.Columns.Count)
            {
                return false;
            }

            for (var x = 0; x < dt1.Rows.Count; x++)
            {
                for (var y = 0; y < dt1.Columns.Count; y++)
                {
                    if (dt1.Columns[y].ColumnName != dt2.Columns[y].ColumnName
                        || !Equals(dt1.Rows[x][y], dt2.Rows[x][y]))
                    {
                        return false;
                    }
                }
            }

            return true;
        }
    }

    public class EnumToBooleanConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            return value.Equals(parameter);
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            return value.Equals(true) ? (SqlRoutineType)parameter : Binding.DoNothing;
        }
    }
}