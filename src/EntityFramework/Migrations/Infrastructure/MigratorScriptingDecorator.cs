// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.txt in the project root for license information.

namespace System.Data.Entity.Migrations.Infrastructure
{
    using System.Collections.Generic;
    using System.Data.Entity.Migrations.Model;
    using System.Data.Entity.Migrations.Sql;
    using System.Data.Entity.Resources;
    using System.Data.Entity.Utilities;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// Decorator to produce a SQL script instead of applying changes to the database.
    /// Using this decorator to wrap <see cref="DbMigrator" /> will prevent <see cref="DbMigrator" />
    /// from applying any changes to the target database.
    /// </summary>
    public class MigratorScriptingDecorator : MigratorBase
    {
        private const string IndividualMigrationHeaderFormat = "-- >>> START Migration '{0}' Script <<< --";
        private const string IndividualMigrationFooterFormat = "-- >>> END Migration {0} Script <<< --";
        private const string FullUpwardMigrationHeaderFormat = "-- >>> START Full Migration Script <<< --";
        private const string FullUpwardMigrationFooterFormat = "-- >>> END Full Migration Script <<< --";
        private const string ScriptFileHeaderFormat =
@"-- Generated Database Migration Script --
/* 
 * ContextType: {0}
 * Generated At: {1:u}
 *
 * ContextKey: {2}
 * Direction: {3}
 * SourceMigrationId: {4}
 * TargetMigrationId: {5}
 * 
 */

-- >>> START Migration Script <<< --";

        private const string ScriptFileFooterFormat =
@"-- >>> END Migration Script <<< --
/* 
 * Script Generation Duration: {0}
 *
 * Total Migrations: {1}
 * Total Statements: {2}
 * 
 */";

        private readonly StringBuilder _sqlBuilder = new StringBuilder();

        private UpdateDatabaseOperation _updateDatabaseOperation;

        private readonly Stopwatch _stopwatch = new Stopwatch();

        private int _totalMigrationsCount = 0;
        private int _totalStatementsCount = 0;

        /// <summary>
        /// Initializes a new instance of the  MigratorScriptingDecorator class.
        /// </summary>
        /// <param name="innerMigrator"> The migrator that this decorator is wrapping. </param>
        public MigratorScriptingDecorator(MigratorBase innerMigrator)
            : base(innerMigrator)
        {
            Check.NotNull(innerMigrator, "innerMigrator");
        }

        private void AppendScriptFileHeader(string directionDescription, string sourceMigrationId, string targetMigrationId)
        {
            var sourceMigrationIdDescription = string.IsNullOrEmpty(sourceMigrationId) 
                ? DbMigrator.InitialDatabase 
                : sourceMigrationId;

            var targetMigrationIdDescription = string.IsNullOrEmpty(targetMigrationId)
                ? "LATEST"
                : targetMigrationId;

            _sqlBuilder.AppendFormat(
                ScriptFileHeaderFormat,
                Configuration.ContextType,
                DateTime.UtcNow,
                Configuration.ContextKey,
                directionDescription,
                sourceMigrationIdDescription,
                targetMigrationIdDescription);
            _sqlBuilder.AppendLine();
            _sqlBuilder.AppendLine();
        }

        private void AppendFullUpwardMigrationHeader()
        {
            _sqlBuilder.AppendFormat(FullUpwardMigrationHeaderFormat);
            _sqlBuilder.AppendLine();
        }

        private void AppendFullUpwardMigrationFooter()
        {
            _sqlBuilder.AppendFormat(FullUpwardMigrationFooterFormat);
            _sqlBuilder.AppendLine();
        }

        private void AppendIndividualMigrationHeader(string migrationId)
        {
            _sqlBuilder.AppendFormat(IndividualMigrationHeaderFormat, migrationId);
            _sqlBuilder.AppendLine();
        }

        private void AppendIndividualMigrationFooter(string migrationId)
        {
            _sqlBuilder.AppendFormat(IndividualMigrationFooterFormat, migrationId);
            _sqlBuilder.AppendLine();
            _sqlBuilder.AppendLine();
            _sqlBuilder.AppendLine();
        }

        private void AppendScriptFileFooter(TimeSpan scriptGenerationDuration, int totalMigrationsCount, int totalStatementsCount)
        {
            _sqlBuilder.AppendFormat(ScriptFileFooterFormat, scriptGenerationDuration, totalMigrationsCount, totalStatementsCount);
        }

        /// <summary>
        /// Produces a script to update the database.
        /// </summary>
        /// <param name="sourceMigration">
        /// The migration to update from. If null is supplied, a script to update the
        /// current database will be produced.
        /// </param>
        /// <param name="targetMigration">
        /// The migration to update to. If null is supplied,
        /// a script to update to the latest migration will be produced.
        /// </param>
        /// <returns> The generated SQL script. </returns>
        public string ScriptUpdate(string sourceMigration, string targetMigration)
        {
            _stopwatch.Restart();

            _sqlBuilder.Clear();
            AppendScriptFileHeader("Upward", sourceMigration, targetMigration);

            if (string.IsNullOrWhiteSpace(sourceMigration))
            {
                Update(targetMigration);
            }
            else
            {
                if (sourceMigration.IsAutomaticMigration())
                {
                    throw Error.AutoNotValidForScriptWindows(sourceMigration);
                }

                var sourceMigrationId = GetMigrationId(sourceMigration);
                var pendingMigrations = GetLocalMigrations().Where(m => string.CompareOrdinal(m, sourceMigrationId) > 0);

                string targetMigrationId = null;

                if (!string.IsNullOrWhiteSpace(targetMigration))
                {
                    if (targetMigration.IsAutomaticMigration())
                    {
                        throw Error.AutoNotValidForScriptWindows(targetMigration);
                    }

                    targetMigrationId = GetMigrationId(targetMigration);

                    if (string.CompareOrdinal(sourceMigrationId, targetMigrationId) > 0)
                    {
                        // BEGIN GALEN MODIFICATIONS
                        //throw Error.DownScriptWindowsNotSupported();
                        return ScriptUpdateDownward(sourceMigration, targetMigration);
                        // END GALEN MODIFICATIONS
                    }

                    pendingMigrations = pendingMigrations.Where(m => string.CompareOrdinal(m, targetMigrationId) <= 0);
                }

                _updateDatabaseOperation
                    = sourceMigration == DbMigrator.InitialDatabase
                          ? new UpdateDatabaseOperation(base.CreateDiscoveryQueryTrees().ToList())
                          : null;

                Upgrade(pendingMigrations, targetMigrationId, sourceMigrationId);

                if (_updateDatabaseOperation != null)
                {
                    _sqlBuilder.Clear();
                    AppendScriptFileHeader("Full Upward", sourceMigration, targetMigration);
                    AppendFullUpwardMigrationHeader();
                    ExecuteStatements(base.GenerateStatements(new[] { _updateDatabaseOperation }, null));
                    AppendFullUpwardMigrationFooter();
                }
            }

            _stopwatch.Stop();

            AppendScriptFileFooter(_stopwatch.Elapsed, _totalMigrationsCount, _totalStatementsCount);

            return _sqlBuilder.ToString();
        }

        private string ScriptUpdateDownward(string sourceMigration, string targetMigration)
        {
            _stopwatch.Restart();

            _sqlBuilder.Clear();
            AppendScriptFileHeader("Downward", sourceMigration, targetMigration);

            var targetMigrationId = GetMigrationId(targetMigration);
            var sourceMigrationId = GetMigrationId(sourceMigration);

            var pendingMigrations = GetLocalMigrations()
                .Where(migrationId => 
                    (string.CompareOrdinal(targetMigrationId, migrationId) <= 0) && 
                    (string.CompareOrdinal(sourceMigrationId, migrationId) >= 0))
                .ToList();

            if (targetMigrationId == DbMigrator.InitialDatabase)
            {
                pendingMigrations.Add(DbMigrator.InitialDatabase);
            }

            pendingMigrations = pendingMigrations.OrderByDescending(migrationId => migrationId).ToList();

            Downgrade(pendingMigrations);

            _stopwatch.Stop();

            AppendScriptFileFooter(_stopwatch.Elapsed, _totalMigrationsCount, _totalStatementsCount);

            return _sqlBuilder.ToString();
        }

        internal override IEnumerable<MigrationStatement> GenerateStatements(
            IList<MigrationOperation> operations, string migrationId)
        {
            DebugCheck.NotEmpty(migrationId);

            if (_updateDatabaseOperation == null)
            {
                return base.GenerateStatements(operations, migrationId);
            }

            _updateDatabaseOperation.AddMigration(migrationId, operations);

            return Enumerable.Empty<MigrationStatement>();
        }

        internal override void EnsureDatabaseExists(Action mustSucceedToKeepDatabase)
        {
            mustSucceedToKeepDatabase();
        }

        internal override void ExecuteStatements(IEnumerable<MigrationStatement> migrationStatements, string migrationId = null)
        {
            var isWriteIndividualMigrationHeaderAndFooter = migrationStatements.Any() && (!string.IsNullOrEmpty(migrationId));
            if (isWriteIndividualMigrationHeaderAndFooter)
            {
                AppendIndividualMigrationHeader(migrationId);
            }

            BuildSqlScript(migrationStatements, _sqlBuilder);

            if (isWriteIndividualMigrationHeaderAndFooter)
            {
                AppendIndividualMigrationFooter(migrationId);
            }

            _totalMigrationsCount++;
            _totalStatementsCount += migrationStatements.Count();
        }

        internal static void BuildSqlScript(IEnumerable<MigrationStatement> migrationStatements, StringBuilder sqlBuilder)
        {
            foreach (var migrationStatement in migrationStatements)
            {
                if (!string.IsNullOrWhiteSpace(migrationStatement.Sql))
                {
                    if (!string.IsNullOrWhiteSpace(migrationStatement.BatchTerminator)
                        && (sqlBuilder.Length > 0))
                    {
                        sqlBuilder.AppendLine(migrationStatement.BatchTerminator);
                        sqlBuilder.AppendLine();
                    }

                    sqlBuilder.AppendLine(migrationStatement.Sql);
                }
            }
        }

        internal override void SeedDatabase()
        {
        }

        internal override bool HistoryExists()
        {
            return false;
        }
    }
}
