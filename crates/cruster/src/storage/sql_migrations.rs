use std::collections::HashMap;

use sqlx::migrate::Migrator;
use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::error::ClusterError;

const CRUSTER_MIGRATION_CHAIN: &str = "cruster";
const CRUSTER_MIGRATIONS_TABLE: &str = "_cruster_migrations";

#[derive(Debug, Clone, Copy)]
struct PostgresMigrationOptions<'a> {
    chain: &'a str,
    tracking_table: &'a str,
    advisory_lock: bool,
}

#[derive(Debug, Clone)]
struct MigrationStatus {
    chain: String,
    tracking_table: String,
    summary: RunSummary,
}

#[derive(Debug, Clone, Copy)]
struct RunSummary {
    applied: usize,
    pending: usize,
    applied_now: usize,
}

pub(crate) async fn run_cruster_migrations(
    pool: &PgPool,
    migrations_table: Option<&str>,
) -> Result<(), ClusterError> {
    let migrations_table = migrations_table.unwrap_or(CRUSTER_MIGRATIONS_TABLE);
    let status = run_sqlx_migrations_with_table(
        pool,
        &sqlx::migrate!(),
        PostgresMigrationOptions {
            chain: CRUSTER_MIGRATION_CHAIN,
            tracking_table: migrations_table,
            advisory_lock: true,
        },
    )
    .await?;

    tracing::debug!(
        chain = %status.chain,
        tracking_table = %status.tracking_table,
        applied = status.summary.applied,
        pending = status.summary.pending,
        applied_now = status.summary.applied_now,
        "migration status"
    );

    Ok(())
}

async fn run_sqlx_migrations_with_table(
    pool: &PgPool,
    migrator: &Migrator,
    options: PostgresMigrationOptions<'_>,
) -> Result<MigrationStatus, ClusterError> {
    let mut lock_conn = if options.advisory_lock {
        Some(
            pool.acquire()
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("failed to acquire migration lock connection: {e}"),
                    source: Some(Box::new(e)),
                })?,
        )
    } else {
        None
    };

    if options.advisory_lock {
        let lock_conn = lock_conn
            .as_mut()
            .expect("lock connection must exist when advisory lock is enabled");
        sqlx::query("SELECT pg_advisory_lock(hashtext($1)::bigint)")
            .bind(options.tracking_table)
            .execute(&mut **lock_conn)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to acquire migration advisory lock: {e}"),
                source: Some(Box::new(e)),
            })?;
    }

    let run_result = run_locked(pool, migrator, options).await;

    if options.advisory_lock {
        let lock_conn = lock_conn
            .as_mut()
            .expect("lock connection must exist when advisory lock is enabled");
        sqlx::query("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
            .bind(options.tracking_table)
            .execute(&mut **lock_conn)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to release migration advisory lock: {e}"),
                source: Some(Box::new(e)),
            })?;
    }

    run_result
}

async fn run_locked(
    pool: &PgPool,
    migrator: &Migrator,
    options: PostgresMigrationOptions<'_>,
) -> Result<MigrationStatus, ClusterError> {
    let tracking_table = quote_ident(options.tracking_table);
    let create_table = format!(
        r#"CREATE TABLE IF NOT EXISTS {tracking_table} (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            checksum BYTEA NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    );
    sqlx::query(AssertSqlSafe(create_table))
        .execute(pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to ensure migration tracking table exists: {e}"),
            source: Some(Box::new(e)),
        })?;

    let applied_query =
        format!(r#"SELECT version, checksum FROM {tracking_table} ORDER BY version"#,);
    let applied_rows = sqlx::query(AssertSqlSafe(applied_query))
        .fetch_all(pool)
        .await
        .map_err(|e| ClusterError::PersistenceError {
            reason: format!("failed to load applied migrations: {e}"),
            source: Some(Box::new(e)),
        })?;
    let applied_map: HashMap<i64, Vec<u8>> = applied_rows
        .iter()
        .map(|row| {
            (
                row.get::<i64, _>("version"),
                row.get::<Vec<u8>, _>("checksum"),
            )
        })
        .collect();

    let all_migrations: Vec<_> = migrator
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .collect();

    let mut applied_now = 0usize;
    for migration in all_migrations.iter().copied() {
        let version = migration.version;
        if let Some(existing_checksum) = applied_map.get(&version) {
            if existing_checksum.as_slice() != migration.checksum.as_ref() {
                return Err(ClusterError::PersistenceError {
                    reason: format!(
                        "migration checksum mismatch for version {version} in {}",
                        options.tracking_table
                    ),
                    source: None,
                });
            }
            continue;
        }

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to start migration transaction: {e}"),
                source: Some(Box::new(e)),
            })?;

        for stmt in split_sql_statements(migration.sql.as_ref()) {
            sqlx::query(AssertSqlSafe(stmt))
                .execute(&mut *tx)
                .await
                .map_err(|e| ClusterError::PersistenceError {
                    reason: format!("failed to execute migration {version}: {e}"),
                    source: Some(Box::new(e)),
                })?;
        }

        let insert = format!(
            r#"INSERT INTO {tracking_table} (version, description, checksum) VALUES ($1, $2, $3)"#,
        );
        sqlx::query(AssertSqlSafe(insert))
            .bind(version)
            .bind(migration.description.as_ref())
            .bind(migration.checksum.as_ref())
            .execute(&mut *tx)
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to record migration {version}: {e}"),
                source: Some(Box::new(e)),
            })?;

        tx.commit()
            .await
            .map_err(|e| ClusterError::PersistenceError {
                reason: format!("failed to commit migration {version}: {e}"),
                source: Some(Box::new(e)),
            })?;
        applied_now += 1;
    }

    Ok(MigrationStatus {
        chain: options.chain.to_string(),
        tracking_table: options.tracking_table.to_string(),
        summary: RunSummary {
            applied: applied_map.len(),
            pending: all_migrations.len().saturating_sub(applied_map.len()),
            applied_now,
        },
    })
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    let mut in_dollar_block = false;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'$') {
            current.push(c);
            current.push(chars.next().unwrap_or('$'));
            in_dollar_block = !in_dollar_block;
        } else if c == ';' && !in_dollar_block {
            let trimmed = current.trim().to_string();
            if !trimmed.is_empty() {
                stmts.push(trimmed);
            }
            current.clear();
        } else {
            current.push(c);
        }
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        stmts.push(trimmed);
    }

    stmts
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    #[test]
    fn split_statements_keeps_do_block_together() {
        let sql = r#"
            DO $$
            BEGIN
                PERFORM 1;
            END $$;
            CREATE TABLE test_table(id BIGINT);
        "#;

        let stmts = super::split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("DO $$"));
        assert!(stmts[0].contains("PERFORM 1;"));
        assert!(stmts[1].starts_with("CREATE TABLE"));
    }

    #[test]
    fn quote_ident_escapes_double_quotes() {
        assert_eq!(super::quote_ident("a\"b"), "\"a\"\"b\"");
    }
}
