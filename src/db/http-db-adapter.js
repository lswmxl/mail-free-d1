/**
 * HTTP DB 适配器（D1 兼容最小接口）
 * 用于 Worker 通过公网 IPv6 调用自建本机数据库网关
 */

class HttpPreparedStatement {
  constructor(adapter, sql, args) {
    this.adapter = adapter;
    this.sql = sql;
    this.args = Array.isArray(args) ? args : [];
  }

  bind() {
    const args = Array.prototype.slice.call(arguments);
    return new HttpPreparedStatement(this.adapter, this.sql, args);
  }

  async all() {
    return this.adapter._query('all', this.sql, this.args);
  }

  async run() {
    return this.adapter._query('run', this.sql, this.args);
  }

  async first() {
    return this.adapter._query('first', this.sql, this.args);
  }
}

export function createHttpDbAdapter(baseUrl, token) {
  if (!baseUrl) {
    throw new Error('缺少 DB_API_URL');
  }

  function buildHeaders() {
    const headers = {
      'content-type': 'application/json',
    };
    if (token) {
      headers['x-db-token'] = token;
    }
    return headers;
  }

  async function callGateway(path, payload) {
    const url = String(baseUrl).replace(/\/+$/, '') + path;
    const res = await fetch(url, {
      method: 'POST',
      headers: buildHeaders(),
      body: JSON.stringify(payload || {}),
    });

    const text = await res.text();
    let data = null;
    try {
      data = text ? JSON.parse(text) : {};
    } catch (e) {
      throw new Error('DB 网关返回了非 JSON 响应: ' + text.slice(0, 200));
    }

    if (!res.ok || (data && data.success === false)) {
      const msg = data && data.error ? data.error : ('HTTP ' + res.status);
      throw new Error('DB 网关调用失败: ' + msg);
    }

    return data;
  }

  return {
    __type: 'http-db-adapter',

    prepare(sql) {
      return new HttpPreparedStatement(this, sql, []);
    },

    async exec(sql) {
      const data = await callGateway('/query', {
        mode: 'exec',
        sql: sql,
        args: [],
      });
      return data.result || { success: true };
    },

    async batch(statements) {
      if (!Array.isArray(statements)) {
        throw new Error('batch 参数必须是数组');
      }
      const out = [];
      for (let i = 0; i < statements.length; i += 1) {
        const stmt = statements[i];
        if (stmt instanceof HttpPreparedStatement) {
          out.push(await stmt.run());
          continue;
        }
        if (stmt && typeof stmt.sql === 'string') {
          out.push(await this._query('run', stmt.sql, Array.isArray(stmt.args) ? stmt.args : []));
          continue;
        }
        throw new Error('batch 元素必须是 prepare() 返回值或 {sql,args}');
      }
      return out;
    },

    async _query(mode, sql, args) {
      const data = await callGateway('/query', {
        mode: mode,
        sql: sql,
        args: Array.isArray(args) ? args : [],
      });
      return data.result;
    },
  };
}
