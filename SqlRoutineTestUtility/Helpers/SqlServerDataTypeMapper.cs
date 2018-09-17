using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;

namespace SqlRoutineTestUtility.Helpers
{
    public static class SqlServerDataTypeMapper
    {
        public static Dictionary<string, SqlDbType> GetSqlDbTypeEnumByDatabaseEngineType(string path, char delimiter, int linesToSkip)
        {
            return File.ReadLines(path)
                        .Skip(linesToSkip)
                        .Select(p => p.Split(delimiter))
                        .ToDictionary(p => p[0], p => GetSqlDbTypeEnum(p[1]));
        }

        private static SqlDbType GetSqlDbTypeEnum(string sqlDbTypeString)
        {
            if (!Enum.TryParse(sqlDbTypeString, out SqlDbType sqlDbType))
            {
                throw new InvalidEnumArgumentException($@"Failed to convert {sqlDbTypeString} to SqlDbType.");
            }

            return sqlDbType;
        }
    }
}