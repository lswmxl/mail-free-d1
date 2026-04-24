/**
 * Turso/libSQL 适配器（D1 兼容最小接口）
 * 提供 prepare().bind().all()/run()/first()、exec()、batch()
 */

import { createClient } from '@libsql/client/web';

const CLIENT_CACHE = new Map();

function normalizeRows(result) {
  const columns = Array.isArray(result.columns) ? result.columns : [];
  const rows = Array.isArray(result.rows) ? result.rows : [];

  return rows.map((row) => {
    if (!row) return {};

    if (typeof row.toJSON === 'function') {
      return row.toJSON();
    }

    if (Array.isArray(row)) {
      const obj = {};
      for (let i = 0; i < columns.length; i += 1) {
        obj[columns[i]] = row[i];
      }
      return obj;
    }

    return { ...row };
  });
}

function normalizeMeta(result) {
  let lastRowId = result.lastInsertRowid;

  if (typeof lastRowId === 'bigint') {
    lastRowId = Number(lastRowId);
  } else if (lastRowId != null && typeof lastRowId !== 'number') {
    const parsed = Number(String(lastRowId));
    lastRowId = Number.isFinite(parsed) ? parsed : 0;
  }

  return {
    changes: Number(result.rowsAffected || 0),
    last_row_id: Number.isFinite(lastRowId) ? lastRowId : 0,
    rows_read: Array.isArray(result.rows) ? result.rows.length : 0,
    duration: 0,
  };
}

class TursoPreparedStatement {
  constructor(adapter, sql, args = []) {
    this.adapter = adapter;
    this.sql = sql;
    this.args = args;
  }

  bind(...args) {
    return new TursoPreparedStatement(this.adapter, this.sql, args);
  }

  async all() {
    const result = await this.adapter._execute(this.sql, this.args);
    return {
      success: true,
      results: normalizeRows(result),
      meta: normalizeMeta(result),
    };
  }

  async run() {
    const result = await this.adapter._execute(this.sql, this.args);
    return {
      success: true,
      meta: normalizeMeta(result),
    };
  }

  async first() {
    const { results } = await this.all();
    return results.length > 0 ? results[0] : null;
  }
}

function getClient(url, authToken) {
  const key = `${url}::${authToken || ''}`;
  if (!CLIENT_CACHE.has(key)) {
    CLIENT_CACHE.set(key, createClient({ url, authToken }));
  }
  return CLIENT_CACHE.get(key);
}

export function getOrCreateTursoD1Adapter(url, authToken) {
  const client = getClient(url, authToken);

  return {
    __type: 'turso-d1-adapter',

    prepare(sql) {
      return new TursoPreparedStatement(this, sql, []);
    },

    async exec(sql) {
      await client.execute(sql);
      return { success: true };
    },

    async batch(statements) {
      if (!Array.isArray(statements)) {
        throw new Error('batch 参数必须是数组');
      }

      const out = [];
      for (const stmt of statements) {
        if (stmt instanceof TursoPreparedStatement) {
          out.push(await stmt.run());
          continue;
        }

        if (stmt && typeof stmt.sql === 'string') {
          const result = await this._execute(stmt.sql, Array.isArray(stmt.args) ? stmt.args : []);
          out.push({
            success: true,
            results: normalizeRows(result),
            meta: normalizeMeta(result),
          });
          continue;
        }

        throw new Error('batch 元素必须是 prepare() 返回值或 {sql,args} 对象');
      }

      return out;
    },

    async _execute(sql, args = []) {
      return client.execute({
        sql,
        args: Array.isArray(args) ? args : [],
      });
    },
  };
}
