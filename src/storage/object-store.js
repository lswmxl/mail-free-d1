import { AwsClient } from 'aws4fetch';

const encoder = new TextEncoder();

function hasMinioConfig(env) {
  return !!(env?.MINIO_ENDPOINT && env?.MINIO_BUCKET && env?.MINIO_ACCESS_KEY && env?.MINIO_SECRET_KEY);
}

function getR2Binding(env) {
  return env?.MAIL_EML_BUCKET || env?.R2_BUCKET || null;
}

function sanitizeMailbox(mailbox = 'unknown') {
  return String(mailbox || 'unknown').trim().toLowerCase().replace(/[^a-z0-9@._-]+/g, '_');
}

function randomHex(len = 16) {
  const bytes = new Uint8Array(len);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
}

function buildObjectKey(mailbox) {
  const now = new Date();
  const yyyy = String(now.getUTCFullYear());
  const mm = String(now.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(now.getUTCDate()).padStart(2, '0');
  const ts = String(Date.now());
  return `${yyyy}/${mm}/${dd}/${sanitizeMailbox(mailbox)}/${ts}-${randomHex(8)}.eml`;
}

function normalizeBytes(input) {
  if (!input) return new Uint8Array();
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (typeof input === 'string') return encoder.encode(input);
  return new Uint8Array(input);
}

function createMinioClient(env) {
  const region = String(env.MINIO_REGION || 'us-east-1');
  return new AwsClient({
    accessKeyId: String(env.MINIO_ACCESS_KEY || '').trim(),
    secretAccessKey: String(env.MINIO_SECRET_KEY || '').trim(),
    region,
    service: 's3'
  });
}

function resolveMinioUrl(env, objectKey) {
  const endpoint = String(env.MINIO_ENDPOINT || '').replace(/\/$/, '');
  const bucket = String(env.MINIO_BUCKET || '').trim();
  const forcePath = String(env.MINIO_FORCE_PATH_STYLE || '1') !== '0';

  if (forcePath) {
    return `${endpoint}/${bucket}/${encodeURI(objectKey)}`;
  }

  const u = new URL(endpoint);
  return `${u.protocol}//${bucket}.${u.host}/${encodeURI(objectKey)}`;
}

export async function storeRawEmail(env, mailbox, rawInput) {
  const objectKey = buildObjectKey(mailbox);
  const bytes = normalizeBytes(rawInput);

  const r2 = getR2Binding(env);
  if (r2 && typeof r2.put === 'function') {
    await r2.put(objectKey, bytes, {
      httpMetadata: { contentType: 'message/rfc822' }
    });
    return {
      bucket: 'MAIL_EML_BUCKET',
      key: objectKey,
      provider: 'r2'
    };
  }

  if (!hasMinioConfig(env)) {
    return null;
  }

  const client = createMinioClient(env);
  const url = resolveMinioUrl(env, objectKey);
  const resp = await client.fetch(url, {
    method: 'PUT',
    headers: {
      'content-type': 'message/rfc822'
    },
    body: bytes
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`MinIO PUT failed: ${resp.status} ${text.slice(0, 200)}`);
  }

  return {
    bucket: String(env.MINIO_BUCKET || '').trim(),
    key: objectKey,
    provider: 'minio'
  };
}

export async function getRawEmail(env, bucket, objectKey) {
  const r2 = getR2Binding(env);
  if (r2 && typeof r2.get === 'function' && bucket === 'MAIL_EML_BUCKET') {
    const obj = await r2.get(objectKey);
    if (!obj) return null;
    const body = await obj.arrayBuffer();
    return {
      body,
      contentType: obj.httpMetadata?.contentType || 'message/rfc822'
    };
  }

  if (!hasMinioConfig(env)) return null;

  const client = createMinioClient(env);
  const url = resolveMinioUrl(env, objectKey);
  const resp = await client.fetch(url, { method: 'GET' });

  if (resp.status === 404) return null;
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`MinIO GET failed: ${resp.status} ${text.slice(0, 200)}`);
  }

  return {
    body: await resp.arrayBuffer(),
    contentType: resp.headers.get('content-type') || 'message/rfc822'
  };
}

export async function deleteRawEmail(env, bucket, objectKey) {
  if (!objectKey) return;

  const r2 = getR2Binding(env);
  if (r2 && typeof r2.delete === 'function' && bucket === 'MAIL_EML_BUCKET') {
    await r2.delete(objectKey);
    return;
  }

  if (!hasMinioConfig(env)) return;

  const client = createMinioClient(env);
  const url = resolveMinioUrl(env, objectKey);
  const resp = await client.fetch(url, { method: 'DELETE' });
  if (!resp.ok && resp.status !== 404 && resp.status !== 204) {
    const text = await resp.text();
    throw new Error(`MinIO DELETE failed: ${resp.status} ${text.slice(0, 200)}`);
  }
}
