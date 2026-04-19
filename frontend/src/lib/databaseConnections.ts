import type {
  ConnectionScope,
  DatabaseConnectionConfig,
  DatabaseSourceConfig,
  DbType,
  GlobalOracleDatabaseSourceConfig,
  GlobalPostgresDatabaseSourceConfig,
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
      connection_mode: 'local',
      db_type: 'oracle',
      connection: defaultConnectionConfig('oracle') as OracleConnectionConfig,
      query: '',
      fetch_config: defaultOracleFetchConfig(),
    }
  }

  return {
    connection_mode: 'local',
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
      connection_mode: 'local',
      db_type: 'oracle',
      connection: defaultConnectionConfig('oracle') as OracleConnectionConfig,
      query,
      fetch_config: defaultOracleFetchConfig(),
    }
  }

  return {
    connection_mode: 'local',
    db_type: 'postgres',
    connection: defaultConnectionConfig('postgres') as PostgresConnectionConfig,
    query,
  }
}

export function makeGlobalDatabaseSourceConfig(connection: SavedDatabaseConnection): DatabaseSourceConfig {
  if (connection.db_type === 'oracle') {
    const config: GlobalOracleDatabaseSourceConfig = {
      connection_mode: 'global',
      connection_source_id: connection.id,
      db_type: 'oracle',
      query: '',
      fetch_config: defaultOracleFetchConfig(),
    }
    return config
  }

  const config: GlobalPostgresDatabaseSourceConfig = {
    connection_mode: 'global',
    connection_source_id: connection.id,
    db_type: 'postgres',
    query: '',
  }
  return config
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

export function getDraftConnectionConfig(draft: SavedDatabaseConnectionInput): DatabaseConnectionConfig {
  if (draft.db_type === 'oracle') {
    return {
      host: draft.host,
      port: draft.port,
      service_name: draft.service_name,
      user: draft.user,
      password: draft.password,
    }
  }

  return {
    host: draft.host,
    port: draft.port,
    database: draft.database,
    user: draft.user,
    password: draft.password,
  }
}

export function savedConnectionToInput(connection: SavedDatabaseConnection): SavedDatabaseConnectionInput {
  if (connection.db_type === 'oracle') {
    return {
      name: connection.name,
      db_type: 'oracle',
      host: connection.host,
      port: connection.port,
      service_name: connection.service_name,
      user: connection.user,
      password: connection.password,
    }
  }

  return {
    name: connection.name,
    db_type: 'postgres',
    host: connection.host,
    port: connection.port,
    database: connection.database,
    user: connection.user,
    password: connection.password,
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

export function getDatabaseSourceConnectionScope(config: Record<string, unknown>): ConnectionScope {
  return config.connection_mode === 'global' ? 'global' : 'local'
}

export function getDatabaseSourceConnectionSourceId(config: Record<string, unknown>): string | null {
  return typeof config.connection_source_id === 'string' && config.connection_source_id
    ? config.connection_source_id
    : null
}

export function findSavedConnectionById(
  connections: SavedDatabaseConnection[],
  connectionId: string | null,
): SavedDatabaseConnection | null {
  if (!connectionId) return null
  return connections.find((connection) => connection.id === connectionId) ?? null
}
