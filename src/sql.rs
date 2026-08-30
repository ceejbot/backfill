//! sqlx 0.9 wrappers for runtime-constructed SQL.

/// sqlx 0.9's `query*()` functions take `SqlSafeStr` (`&'static str` only).
/// Schema names are interpolated; values are bound. Do not interpolate
/// untrusted input.
///
/// Source: <https://docs.rs/sqlx/0.9.0/sqlx/struct.AssertSqlSafe.html>
pub(crate) fn audited_sql(sql: impl Into<String>) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(sql.into())
}
