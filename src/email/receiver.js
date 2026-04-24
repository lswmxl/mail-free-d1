/**
 * 邮件接收处理模块
 * @module email/receiver
 */

import { extractEmail, normalizeEmailAlias } from '../utils/common.js';
import { getOrCreateMailboxId } from '../db/index.js';
import { parseEmailBody, extractVerificationCode } from './parser.js';

/**
 * 处理通过 HTTP 接收的邮件
 * @param {Request} request - HTTP 请求对象
 * @param {object} db - 数据库连接
 * @param {object} env - 环境变量
 * @returns {Promise<Response>} HTTP 响应
 */
export async function handleEmailReceive(request, db, env) {
  try {
    const emailData = await request.json();
    const to = String(emailData?.to || '');
    const from = String(emailData?.from || '');
    const subject = String(emailData?.subject || '(无主题)');
    const text = String(emailData?.text || '');
    const html = String(emailData?.html || '');

    // 提取并规范化邮箱地址（支持别名邮箱，例如 ab.c@qq.ss -> c@qq.ss）
    const rawMailbox = extractEmail(to);
    const mailbox = normalizeEmailAlias(rawMailbox);
    const sender = extractEmail(from);
    const mailboxId = await getOrCreateMailboxId(db, mailbox);

    const previewBase = (text || html.replace(/<[^>]+>/g, ' ')).replace(/\s+/g, ' ').trim();
    const preview = String(previewBase || '').slice(0, 120);
    let verificationCode = '';
    try {
      verificationCode = extractVerificationCode({ subject, text, html });
    } catch (_) { }

    await db.prepare(`
      INSERT INTO messages (mailbox_id, sender, to_addrs, subject, verification_code, preview, content, html_content)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      mailboxId,
      sender,
      String(to || ''),
      subject || '(无主题)',
      verificationCode || null,
      preview || null,
      text || null,
      html || null
    ).run();

    return Response.json({ success: true });
  } catch (error) {
    console.error('处理邮件时出错:', error);
    return new Response('处理邮件失败', { status: 500 });
  }
}
