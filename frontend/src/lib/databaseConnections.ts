import type {
  DatabaseConnectionConfig,
  DatabaseSourceConfig,
  DbType,
  OracleConnectionConfig,
  OracleFetchConfig,
  PostgresConnectionConfig,
  SavedDatabaseConnection,
  SavedDatabaseConnectionInput,
} from '../types/pipeline'

const ORACLE_FETCH_DEFAULTS: OracleFetchConfig = {
  mode: 'fetchall',
  arraysize: 100,
  prefetchrows: 2,
}

export function defaultConnectionConfig(dbType: DbType): DatabaseConnectionConfig {
  if (dbType === 'oracle') {
    return { host: '', port: 1521, service_name: '', user: '', password: '' }
  }
  return { host: '', port: 5432, database: '', user: '', password: '' }
}

export function defaultOracleFetchConfig(): OracleFetchConfig {
  return { ...ORACLE_FETCH_DEFAULTS }
}

export function normalizeOracleFetchConfig(fetchConfig?: Partial<OracleFetchConfig> | null): OracleFetchConfig {
  const arraysize = Number.isInteger(fetchConfig?.arraysize) && Number(fetchConfig?.arraysize) >= 1
    ? Number(fetchConfig?.arraysize)
    : ORACLE_FETCH_DEFAULTS.arraysize
  const prefetchrows = Number.isInteger(fetchConfig?.prefetchrows) && Number(fetchConfig?.prefetchrows) >= 0
    ? Number(fetchConfig?.prefetchrows)
    : ORACLE_FETCH_DEFAULTS.prefetchrows

  return {
    mode: fetchConfig?.mode === 'fetchmany' ? 'fetchmany' : ORACLE_FETCH_DEFAULTS.mode,
    arraysize,
    prefetchrows,
  }
}

export function defaultDatabaseSourceConfig(dbType: DbType): DatabaseSourceConfig {
  if (dbType === 'oracle') {
    return {
      db_type: 'oracle',
      connection: defaultConnectionConfig('oracle') as OracleConnectionConfig,
      query: '',
      fetch_config: defaultOracleFetchConfig(),
    }
  }

  return {
    db_type: 'postgres',
    connection: defaultConnectionConfig('postgres') as PostgresConnectionConfig,
    query: '',
  }
}

export function switchDatabaseSourceConfigDbType(
  dbType: DbType,
  currentConfig?: Partial<DatabaseSourceConfig> | null,
): DatabaseSourceConfig {
  const query = typeof currentConfig?.query === 'string' ? currentConfig.query : ''

  if (dbType === 'oracle') {
    return {
      db_type: 'oracle',
      connection: defaultConnectionConfig('oracle') as OracleConnectionConfig,
      query,
      fetch_config: defaultOracleFetchConfig(),
    }
  }

  return {
    db_type: 'postgres',
    connection: defaultConnectionConfig('postgres') as PostgresConnectionConfig,
    query,
  }
}

export function makeSavedConnectionDraft(dbType: DbType = 'postgres'): SavedDatabaseConnectionInput {
  if (dbType === 'oracle') {
    return {
      name: '',
      db_type: 'oracle',
      host: '',
      port: 1521,
      service_name: '',
      user: '',
      password: '',
    }
  }

  return {
    name: '',
    db_type: 'postgres',
    host: '',
    port: 5432,
    database: '',
    user: '',
    password: '',
  }
}

export function getConnectionSummary(
  dbType: DbType,
  connection: DatabaseConnectionConfig | SavedDatabaseConnection
): string {
  const host = connection.host
  const port = connection.port
  if (dbType === 'oracle') {
    return `${host}:${port}/${'service_name' in connection ? connection.service_name : ''}`
  }
  return `${host}:${port}/${'database' in connection ? connection.database : ''}`
}
