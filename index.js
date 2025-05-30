require('dotenv').config({ path: './config.env' });

const axios = require('axios');
const mysql = require('mysql2/promise');

const API_TOKEN = process.env.GAPI_TOKEN;
const DB_CONFIG = {
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'gapify_db',
};
console.log('TOKEN:', process.env.GAPI_TOKEN);
console.log('DB:', process.env.DB_HOST, process.env.DB_USER, process.env.DB_PASSWORD, process.env.DB_NAME);


async function fetchFilteredConversations() {
  try {
    const response = await axios.post(
      'https://app.gapify.ai/api/v1/accounts/65/conversations/filter?page=1',
      {
        payload: [
          {
            values: ['1'],
            attribute_key: 'last_activity_at',
            attribute_model: 'standard',
            filter_operator: 'days_before',
            custom_attribute_type: ''
          }
        ]
      },
      {
        headers: {
          'api-access-token': API_TOKEN,
          'Content-Type': 'application/json'
        }
      }
    );
    return response.data.payload; // اصلاح شدهت
  } catch (error) {
    console.error('API fetch error:', error.response ? error.response.data : error.message);
    return [];
  }
}

async function saveConversation(db, convo) {
  const sql = `
    INSERT INTO conversations (id, last_activity_at, data)
    VALUES (?, ?, ?)
    ON DUPLICATE KEY UPDATE
      last_activity_at = VALUES(last_activity_at),
      data = VALUES(data)
  `;
  const params = [
    convo.id,
    convo.meta?.sender?.last_activity_at
      ? new Date(convo.meta.sender.last_activity_at * 1000)
      : null,
    JSON.stringify(convo)
  ];
  await db.execute(sql, params);
}

async function main() {
  const db = await mysql.createConnection(DB_CONFIG);
  const conversations = await fetchFilteredConversations();
  if (!conversations || conversations.length === 0) {
    console.log('No conversations found.');
    await db.end();
    return;
  }
  for (const convo of conversations) {
    await saveConversation(db, convo);
  }
  await db.end();
  console.log('Conversations saved to database.');
}

main();
