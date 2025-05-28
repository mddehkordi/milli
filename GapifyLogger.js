require('dotenv').config();
const axios = require('axios');
const mysql = require('mysql2/promise');
const dayjs = require('dayjs');
const pLimit = require('p-limit');
const cron = require('node-cron');

const GAPI_TOKEN = process.env.GAPI_TOKEN;
const BASE_URL = 'https://api.gapify.ai/v1';

const dbPool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'gapify_db',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// گرفتن مکالمات برای بازه زمانی دلخواه (مثلاً امروز)
async function fetchConversations(from, to) {
  try {
    const res = await axios.get(`${BASE_URL}/conversations`, {
      headers: { Authorization: `Bearer ${GAPI_TOKEN}` },
      params: {
        from,
        to,
        limit: 100
      }
    });
    return res.data.payload || [];
  } catch (error) {
    console.error('Error fetching conversations:', error.message);
    return [];
  }
}

async function fetchMessages(conversationId) {
  try {
    const res = await axios.get(`${BASE_URL}/conversations/${conversationId}/messages`, {
      headers: { Authorization: `Bearer ${GAPI_TOKEN}` }
    });
    return res.data.payload || [];
  } catch (error) {
    console.error(`Error fetching messages for conversation ${conversationId}:`, error.message);
    return [];
  }
}

async function saveSender(sender) {
  if (!sender || !sender.id) return;

  const sql = `
    INSERT INTO senders (
      id, name, email, phone_number, identifier, availability_status,
      last_activity_at, created_at, description, created_at_ip,
      thumbnail, additional_attributes, custom_attributes
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      name=VALUES(name),
      email=VALUES(email),
      phone_number=VALUES(phone_number),
      identifier=VALUES(identifier),
      availability_status=VALUES(availability_status),
      last_activity_at=VALUES(last_activity_at),
      created_at=VALUES(created_at),
      description=VALUES(description),
      created_at_ip=VALUES(created_at_ip),
      thumbnail=VALUES(thumbnail),
      additional_attributes=VALUES(additional_attributes),
      custom_attributes=VALUES(custom_attributes)
  `;

  const params = [
    sender.id,
    sender.name || null,
    sender.email || null,
    sender.phone_number || null,
    sender.identifier || null,
    sender.availability_status || null,
    sender.last_activity_at || null,
    sender.created_at || null,
    sender.additional_attributes?.description || null,
    sender.additional_attributes?.created_at_ip || null,
    sender.thumbnail || null,
    JSON.stringify(sender.additional_attributes || {}),
    JSON.stringify(sender.custom_attributes || {})
  ];

  await dbPool.execute(sql, params);
}

async function saveConversation(convo) {
  if (!convo || !convo.id) return;

  const sql = `
    INSERT INTO conversations (
      id, account_id, uuid, channel, assignee_id, assignee_name, assignee_email,
      assignee_role, assignee_availability_status, agent_last_seen_at,
      assignee_last_seen_at, contact_last_seen_at, can_reply, inbox_id, muted,
      snoozed_until, status, created_at, timestamp, first_reply_created_at,
      unread_count, priority, waiting_since, sla_policy_id,
      custom_attributes, labels, additional_attributes, last_non_activity_message_id
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      account_id=VALUES(account_id),
      uuid=VALUES(uuid),
      channel=VALUES(channel),
      assignee_id=VALUES(assignee_id),
      assignee_name=VALUES(assignee_name),
      assignee_email=VALUES(assignee_email),
      assignee_role=VALUES(assignee_role),
      assignee_availability_status=VALUES(assignee_availability_status),
      agent_last_seen_at=VALUES(agent_last_seen_at),
      assignee_last_seen_at=VALUES(assignee_last_seen_at),
      contact_last_seen_at=VALUES(contact_last_seen_at),
      can_reply=VALUES(can_reply),
      inbox_id=VALUES(inbox_id),
      muted=VALUES(muted),
      snoozed_until=VALUES(snoozed_until),
      status=VALUES(status),
      created_at=VALUES(created_at),
      timestamp=VALUES(timestamp),
      first_reply_created_at=VALUES(first_reply_created_at),
      unread_count=VALUES(unread_count),
      priority=VALUES(priority),
      waiting_since=VALUES(waiting_since),
      sla_policy_id=VALUES(sla_policy_id),
      custom_attributes=VALUES(custom_attributes),
      labels=VALUES(labels),
      additional_attributes=VALUES(additional_attributes),
      last_non_activity_message_id=VALUES(last_non_activity_message_id)
  `;

  const params = [
    convo.id,
    convo.account_id,
    convo.uuid,
    convo.meta?.channel || null,
    convo.meta?.assignee?.id || null,
    convo.meta?.assignee?.name || null,
    convo.meta?.assignee?.email || null,
    convo.meta?.assignee?.role || null,
    convo.meta?.assignee?.availability_status || null,
    convo.agent_last_seen_at || null,
    convo.assignee_last_seen_at || null,
    convo.contact_last_seen_at || null,
    convo.can_reply || false,
    convo.inbox_id || null,
    convo.muted || false,
    convo.snoozed_until ? new Date(convo.snoozed_until) : null,
    convo.status || null,
    convo.created_at || null,
    convo.timestamp || null,
    convo.first_reply_created_at || null,
    convo.unread_count || 0,
    convo.priority || null,
    convo.waiting_since || null,
    convo.sla_policy_id || null,
    JSON.stringify(convo.custom_attributes || {}),
    JSON.stringify(convo.labels || []),
    JSON.stringify(convo.additional_attributes || {}),
    convo.last_non_activity_message?.id || null
  ];

  await dbPool.execute(sql, params);
}

async function saveMessage(msg, conversationId) {
  if (!msg || !msg.id) return;

  const sql = `
    INSERT INTO messages (
      id, conversation_id, content, message_type, created_at, updated_at,
      private, status, source_id, content_type, content_attributes, sender_type,
      sender_id, external_source_ids, additional_attributes, processed_message_content,
      sentiment
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      conversation_id=VALUES(conversation_id),
      content=VALUES(content),
      message_type=VALUES(message_type),
      created_at=VALUES(created_at),
      updated_at=VALUES(updated_at),
      private=VALUES(private),
      status=VALUES(status),
      source_id=VALUES(source_id),
      content_type=VALUES(content_type),
      content_attributes=VALUES(content_attributes),
      sender_type=VALUES(sender_type),
      sender_id=VALUES(sender_id),
      external_source_ids=VALUES(external_source_ids),
      additional_attributes=VALUES(additional_attributes),
      processed_message_content=VALUES(processed_message_content),
      sentiment=VALUES(sentiment)
  `;

  const params = [
    msg.id,
    conversationId,
    msg.content || null,
    msg.message_type || null,
    msg.created_at || null,
    msg.updated_at ? new Date(msg.updated_at) : null,
    msg.private || false,
    msg.status || null,
    msg.source_id || null,
    msg.content_type || null,
    JSON.stringify(msg.content_attributes || {}),
    msg.sender_type || null,
    msg.sender_id || null,
    JSON.stringify(msg.external_source_ids || {}),
    JSON.stringify(msg.additional_attributes || {}),
    msg.processed_message_content || null,
    JSON.stringify(msg.sentiment || {})
  ];

  await dbPool.execute(sql, params);
}

async function saveConversationData(convo) {
  try {
    await saveConversation(convo);

    // ذخیره پیام‌ها
    const messages = convo.messages || await fetchMessages(convo.id);
    const limit = pLimit(5);

    const saveMessagesPromises = messages.map(msg =>
      limit(async () => {
        await saveMessage(msg, convo.id);
        if (msg.sender) {
          await saveSender(msg.sender);
        }
      })
    );

    await Promise.all(saveMessagesPromises);

    // ذخیره ارسال کننده مکالمه (assignee)
    if (convo.meta?.assignee) {
      await saveSender(convo.meta.assignee);
    }

  } catch (err) {
    console.error(`Error saving conversation data for ${convo.id}:`, err.message);
  }
}

async function saveToDatabase(conversations) {
  const limit = pLimit(3);
  const promises = conversations.map(convo => limit(() => saveConversationData(convo)));
  await Promise.all(promises);
}

async function main() {
  try {
    const from = dayjs().startOf('day').toISOString();
    const to = dayjs().endOf('day').toISOString();

    console.log(`Fetching conversations from ${from} to ${to} ...`);
    const conversations = await fetchConversations(from, to);

    if (conversations.length === 0) {
      console.log('No conversations found in this period.');
      return;
    }

    console.log(`Saving ${conversations.length} conversations to database...`);
    await saveToDatabase(conversations);

    console.log('Done saving conversations.');

  } catch (error) {
    console.error('Main error:', error.message);
  }
}

// اجرای کرون جاب: هر 15 دقیقه یکبار اجرا شود
cron.schedule('*/15 * * * *', () => {
  console.log('Cron job started at', new Date().toLocaleString());
  main();
});

// اجرای اول هنگام استارت برنامه
main();

