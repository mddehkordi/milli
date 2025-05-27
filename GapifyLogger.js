require('dotenv').config();
const axios = require('axios');
const mysql = require('mysql2/promise');
const dayjs = require('dayjs');
const pLimit = require('p-limit');

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

// گرفتن مکالمات امروز با تاریخ به‌روز در هر اجرا
async function fetchConversations() {
  try {
    const start = dayjs().startOf('day').toISOString();
    const end = dayjs().endOf('day').toISOString();

    const res = await axios.get(`${BASE_URL}/conversations`, {
      headers: { Authorization: `Bearer ${GAPI_TOKEN}` },
      params: {
        from: start,
        to: end,
        limit: 100
      }
    });
    return res.data.data;
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
    return res.data.data;
  } catch (error) {
    console.error(`Error fetching messages for conversation ${conversationId}:`, error.message);
    return [];
  }
}

async function saveConversation(convo) {
  const sql = `
    INSERT INTO conversations (id, customer_id, status, started_at, ended_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      customer_id=VALUES(customer_id),
      status=VALUES(status),
      started_at=VALUES(started_at),
      ended_at=VALUES(ended_at),
      created_at=VALUES(created_at),
      updated_at=VALUES(updated_at)
  `;
  const params = [
    convo.id,
    convo.customer_id,
    convo.status,
    convo.started_at,
    convo.ended_at,
    convo.created_at,
    convo.updated_at
  ];
  await dbPool.execute(sql, params);
}

async function saveMessage(msg, conversationId) {
  const sql = `
    INSERT INTO messages (id, conversation_id, sender_id, sender_type, content, sent_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      conversation_id=VALUES(conversation_id),
      sender_id=VALUES(sender_id),
      sender_type=VALUES(sender_type),
      content=VALUES(content),
      sent_at=VALUES(sent_at)
  `;
  const params = [
    msg.id,
    conversationId,
    msg.sender.id,
    msg.sender.type,
    msg.content,
    msg.created_at
  ];
  await dbPool.execute(sql, params);
}

async function saveUser(user) {
  const sql = `
    INSERT INTO users (id, name, role, email)
    VALUES (?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      name=VALUES(name),
      role=VALUES(role),
      email=VALUES(email)
  `;
  const params = [
    user.id,
    user.name || '',
    user.type || '',
    user.email || null
  ];
  await dbPool.execute(sql, params);
}

async function saveConversationData(convo) {
  try {
    await saveConversation(convo);

    const messages = await fetchMessages(convo.id);
    const limit = pLimit(5);

    const saveMessagesPromises = messages.map(msg => limit(async () => {
      try {
        await saveMessage(msg, convo.id);
        await saveUser(msg.sender);
      } catch (err) {
        console.error(`Error saving message or user for conversation ${convo.id}:`, err.message);
      }
    }));

    await Promise.all(saveMessagesPromises);
  } catch (err) {
    console.error(`Error processing conversation ${convo.id}:`, err.message);
  }
}

async function saveToDatabase(conversations) {
  const limit = pLimit(3); // اجازه میدیم 3 مکالمه همزمان پردازش بشه

  const promises = conversations.map(convo => limit(() => saveConversationData(convo)));
  await Promise.all(promises);
}

async function main() {
  try {
    const conversations = await fetchConversations();
    if (conversations.length === 0) {
      console.log('No conversations found for today.');
      return;
    }
    await saveToDatabase(conversations);
    console.log('Data saved successfully.');
  } catch (error) {
    console.error('Unexpected error:', error);
  } finally {
    await dbPool.end();
  }
}

main();
