/**
 * 邮件 API 模块
 * @module api/emails
 */

import { getJwtPayload, errorResponse } from './helpers.js';
import { buildMockEmails, buildMockEmailDetail } from './mock.js';
import { extractEmail } from '../utils/common.js';
import { getMailboxIdByAddress } from '../db/index.js';
import { getRawEmail, deleteRawEmail } from '../storage/object-store.js';

/**
 * 处理邮件相关 API
 * @param {Request} request - HTTP 请求
 * @param {object} db - 数据库连接
 * @param {URL} url - 请求 URL
 * @param {string} path - 请求路径
 * @param {object} options - 选项
 * @returns {Promise<Response|null>} 响应或 null（未匹配）
 */
export async function handleEmailsApi(request, db, url, path, options) {
  const isMock = !!options.mockOnly;
  const isMailboxOnly = !!options.mailboxOnly;
  const env = options?.env || null;

  // 获取邮件列表
  if (path === '/api/emails' && request.method === 'GET') {
    const mailbox = url.searchParams.get('mailbox');
    if (!mailbox) {
      return errorResponse('缺少 mailbox 参数', 400);
    }
    try {
      if (isMock) {
        return Response.json(buildMockEmails(6));
      }
      const normalized = extractEmail(mailbox).trim().toLowerCase();
      const mailboxId = await getMailboxIdByAddress(db, normalized);
      if (!mailboxId) return Response.json([]);
      
      let timeFilter = '';
      let timeParam = [];
      if (isMailboxOnly) {
        const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        timeFilter = ' AND received_at >= ?';
        timeParam = [twentyFourHoursAgo];
      }
      
      const limit = Math.min(parseInt(url.searchParams.get('limit') || '20', 10), 50);
      
      const { results } = await db.prepare(`
        SELECT id, sender, to_addrs, subject, received_at, is_read, preview, verification_code, r2_bucket, r2_object_key
        FROM messages 
        WHERE mailbox_id = ?${timeFilter}
        ORDER BY received_at DESC 
        LIMIT ?
      `).bind(mailboxId, ...timeParam, limit).all();
      return Response.json(results || []);
    } catch (e) {
      console.error('查询邮件失败:', e);
      return errorResponse('查询邮件失败', 500);
    }
  }

  // 批量查询邮件详情
  if (path === '/api/emails/batch' && request.method === 'GET') {
    try {
      const idsParam = String(url.searchParams.get('ids') || '').trim();
      if (!idsParam) return Response.json([]);
      const ids = idsParam.split(',').map(s => parseInt(s, 10)).filter(n => Number.isInteger(n) && n > 0);
      if (!ids.length) return Response.json([]);
      
      if (ids.length > 50) {
        return errorResponse('单次最多查询50封邮件', 400);
      }
      
      if (isMock) {
        const arr = ids.map(id => buildMockEmailDetail(id));
        return Response.json(arr);
      }
      
      let timeFilter = '';
      let timeParam = [];
      if (isMailboxOnly) {
        const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        timeFilter = ' AND received_at >= ?';
        timeParam = [twentyFourHoursAgo];
      }
      
      const placeholders = ids.map(() => '?').join(',');
      const { results } = await db.prepare(`
        SELECT id, sender, to_addrs, subject, verification_code, preview, r2_bucket, r2_object_key, received_at, is_read
        FROM messages WHERE id IN (${placeholders})${timeFilter}
      `).bind(...ids, ...timeParam).all();
      return Response.json(results || []);
    } catch (e) {
      return errorResponse('批量查询失败', 500);
    }
  }

  // 清空邮箱邮件
  if (request.method === 'DELETE' && path === '/api/emails') {
    if (isMock) return errorResponse('演示模式不可清空', 403);
    const mailbox = url.searchParams.get('mailbox');
    if (!mailbox) {
      return errorResponse('缺少 mailbox 参数', 400);
    }
    try {
      const normalized = extractEmail(mailbox).trim().toLowerCase();
      const mailboxId = await getMailboxIdByAddress(db, normalized);
      if (!mailboxId) {
        return Response.json({ success: true, deletedCount: 0 });
      }
      
      const { results: objectRows } = await db.prepare(`
        SELECT r2_bucket, r2_object_key
        FROM messages
        WHERE mailbox_id = ? AND r2_object_key IS NOT NULL AND r2_object_key != ''
      `).bind(mailboxId).all();

      const result = await db.prepare(`DELETE FROM messages WHERE mailbox_id = ?`).bind(mailboxId).run();
      const deletedCount = result?.meta?.changes || 0;

      if (env && Array.isArray(objectRows) && objectRows.length > 0) {
        await Promise.allSettled(
          objectRows.map(row => deleteRawEmail(env, row.r2_bucket, row.r2_object_key))
        );
      }

      return Response.json({
        success: true,
        deletedCount
      });
    } catch (e) {
      console.error('清空邮件失败:', e);
      return errorResponse('清空邮件失败', 500);
    }
  }

  // 下载原始 EML
  if (request.method === 'GET' && /^\/api\/email\/\d+\/download$/.test(path)) {
    const emailId = path.split('/')[3];
    try {
      const { results } = await db.prepare(`
        SELECT id, sender, to_addrs, subject, content, html_content, r2_bucket, r2_object_key, received_at
        FROM messages WHERE id = ?
      `).bind(emailId).all();

      if (!results || results.length === 0) {
        return errorResponse('未找到邮件', 404);
      }

      const row = results[0];

      if (env && row.r2_object_key) {
        try {
          const obj = await getRawEmail(env, row.r2_bucket, row.r2_object_key);
          if (obj?.body) {
            return new Response(obj.body, {
              status: 200,
              headers: {
                'content-type': obj.contentType || 'message/rfc822',
                'content-disposition': `attachment; filename="email-${emailId}.eml"`
              }
            });
          }
        } catch (e) {
          console.error('对象存储读取失败，回退拼接下载:', e?.message || e);
        }
      }

      const fallbackEml = [
        `From: ${row.sender || ''}`,
        `To: ${row.to_addrs || ''}`,
        `Subject: ${row.subject || '(无主题)'}`,
        `Date: ${row.received_at || new Date().toISOString()}`,
        'MIME-Version: 1.0',
        'Content-Type: text/plain; charset=UTF-8',
        '',
        row.content || row.html_content || ''
      ].join('\r\n');

      return new Response(fallbackEml, {
        status: 200,
        headers: {
          'content-type': 'message/rfc822; charset=UTF-8',
          'content-disposition': `attachment; filename="email-${emailId}.eml"`
        }
      });
    } catch (e) {
      console.error('下载邮件失败:', e);
      return errorResponse('下载邮件失败', 500);
    }
  }

  // 获取单封邮件详情
  if (request.method === 'GET' && path.startsWith('/api/email/')) {
    const emailId = path.split('/')[3];
    if (isMock) {
      return Response.json(buildMockEmailDetail(emailId));
    }
    try {
      let timeFilter = '';
      let timeParam = [];
      if (isMailboxOnly) {
        const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        timeFilter = ' AND received_at >= ?';
        timeParam = [twentyFourHoursAgo];
      }
      
      const { results } = await db.prepare(`
        SELECT id, sender, to_addrs, subject, verification_code, preview, content, html_content, r2_bucket, r2_object_key, received_at, is_read
        FROM messages WHERE id = ?${timeFilter}
      `).bind(emailId, ...timeParam).all();
      if (results.length === 0) {
        if (isMailboxOnly) {
          return errorResponse('邮件不存在或已超过24小时访问期限', 404);
        }
        return errorResponse('未找到邮件', 404);
      }
      await db.prepare(`UPDATE messages SET is_read = 1 WHERE id = ?`).bind(emailId).run();
      const row = results[0];

      return Response.json({
        ...row,
        content: row.content || '',
        html_content: row.html_content || '',
        download: `/api/email/${emailId}/download`
      });
    } catch (e) {
      console.error('获取邮件详情失败:', e);
      return errorResponse('获取邮件详情失败', 500);
    }
  }

  // 删除单封邮件
  if (request.method === 'DELETE' && path.startsWith('/api/email/')) {
    if (isMock) return errorResponse('演示模式不可删除', 403);
    const emailId = path.split('/')[3];

    if (!emailId || !Number.isInteger(parseInt(emailId))) {
      return errorResponse('无效的邮件ID', 400);
    }

    try {
      const { results } = await db.prepare(`
        SELECT r2_bucket, r2_object_key
        FROM messages WHERE id = ?
      `).bind(emailId).all();

      const objectRow = results?.[0] || null;

      const result = await db.prepare(`DELETE FROM messages WHERE id = ?`).bind(emailId).run();
      const deleted = (result?.meta?.changes || 0) > 0;

      if (deleted && env && objectRow?.r2_object_key) {
        await deleteRawEmail(env, objectRow.r2_bucket, objectRow.r2_object_key);
      }

      return Response.json({
        success: true,
        deleted,
        message: deleted ? '邮件已删除' : '邮件不存在或已被删除'
      });
    } catch (e) {
      console.error('删除邮件失败:', e);
      return errorResponse('删除邮件时发生错误: ' + e.message, 500);
    }
  }

  return null;
}
