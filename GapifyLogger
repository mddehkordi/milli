require('dotenv').config(); // اگر میخوای از فایل .env استفاده کنی
const axios = require('axios');
const mysql = require('mysql2/promise');
const dayjs = require('dayjs');
const pLimit = require('p-limit'); // باید نصب کنی: npm i p-limit

const GAPI_TOKEN = process.env.GAPI_TOKEN;
const BASE_URL = 'https://api.gapify.ai/v1';
const START = dayjs().startOf('day').toISOString();
const END = dayjs().endOf('day').toISOString();

const dbPool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'gapify_db',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

async function fetchConversations() {
  try {
    const res = await axios.get(`${BASE_URL}/conversations`, {
      headers: { Authorization: `Bearer ${GAPI_TOKEN}` },
      params: {
        from: START,
        to: END,
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

async function saveToDatabase(conversations) {
  const limit = pLimit(5); // محدودیت همزمانی ۵ تا

  for (const convo of conversations) {
    await saveConversation(convo);

    // با محدودیت همزمانی پیام‌ها رو ذخیره می‌کنیم
    const messages = await fetchMessages(convo.id);
    const saveMessagesPromises = messages.map(msg => limit(async () => {
      await saveMessage(msg, convo.id);
      await saveUser(msg.sender);
    }));

    await Promise.all(saveMessagesPromises);
  }
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
