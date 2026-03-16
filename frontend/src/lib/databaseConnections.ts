import type {
  DatabaseConnectionConfig,
  DbType,
  SavedDatabaseConnection,
  SavedDatabaseConnectionInput,
} from '../types/pipeline'

export function defaultConnectionConfig(dbType: DbType): DatabaseConnectionConfig {
  if (dbType === 'oracle') {
    return { host: '', port: 1521, service_name: '', user: '', password: '' }
  }
  return { host: '', port: 5432, database: '', user: '', password: '' }
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
