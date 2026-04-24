/**
 * 数据库连接辅助模块
 * @module db/connection
 */

import { initDatabase } from './init.js';
import { getOrCreateTursoD1Adapter } from './turso-adapter.js';
import { createHttpDbAdapter } from './http-db-adapter.js';

/**
 * 获取数据库连接并验证有效性
 * @param {object} env - 环境变量对象
 * @returns {Promise<object>} 数据库连接对象
 * @throws {Error} 当数据库未配置或连接失败时抛出异常
 */
export async function getDatabaseWithValidation(env) {
  const dbFromContext = env && env.DB ? env.DB : null;
  const dbApiUrl = env && env.DB_API_URL ? String(env.DB_API_URL).trim() : '';
  const dbApiToken = env && env.DB_API_TOKEN ? String(env.DB_API_TOKEN) : '';
  const tursoUrl = env && env.TURSO_DATABASE_URL ? env.TURSO_DATABASE_URL : (env ? env.LIBSQL_URL : null);
  const tursoToken = env && env.TURSO_AUTH_TOKEN ? env.TURSO_AUTH_TOKEN : (env ? env.LIBSQL_AUTH_TOKEN : null);

  const db = dbApiUrl
    ? createHttpDbAdapter(dbApiUrl, dbApiToken)
    : (dbFromContext || (tursoUrl ? getOrCreateTursoD1Adapter(tursoUrl, tursoToken) : null));

  if (!db) {
    throw new Error('数据库未配置，请设置 DB_API_URL（IPv6本机网关）或 TURSO_DATABASE_URL 与 TURSO_AUTH_TOKEN（或通过 env.DB 注入）');
  }

  // 验证数据库连接
  try {
    await db.prepare('SELECT 1').all();
  } catch (error) {
    throw new Error(`数据库连接失败: ${error.message}`);
  }

  return db;
}

/**
 * 获取数据库连接并初始化
 * @param {object} env - 环境变量对象
 * @returns {Promise<object>} 初始化后的数据库连接对象
 */
export async function getInitializedDatabase(env) {
  const db = await getDatabaseWithValidation(env);
  
  // 缓存数据库初始化，避免每次请求重复执行
  if (!globalThis.__DB_INITED__) {
    await initDatabase(db);
    globalThis.__DB_INITED__ = true;
  }
  
  return db;
}
