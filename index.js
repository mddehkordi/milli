require('dotenv').config();
const axios = require('axios');

const API_TOKEN = 'ob4e77HBE5sKnRWmQUpQEGgB'; // اینو بذار تو env بهتره

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
          // 'Cookie': '...' اگر نیاز بود اضافه کن
        }
      }
    );
    console.log('Status code:', response.status);
    console.log('Response data:', response.data);
  } catch (error) {
    if (error.response) {
      console.log('Status code:', error.response.status);
      console.log('Response data:', error.response.data);
    } else {
      console.error('Error:', error.message);
    }
  }
}

fetchFilteredConversations();
