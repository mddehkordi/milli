const axios = require('axios');

const URL = 'https://app.gapify.ai/api/v1/accounts/65/conversations/1008272/messages';
const TOKEN = 'توکن_تو_اینجا_قرار_بده';  // اگر نیاز به توکن داری اینجا بزار

async function testApi() {
  try {
    const res = await axios.get(URL, {
      headers: {
        Authorization: `Bearer ${TOKEN}` // اگر توکن نیاز نداره این خط رو حذف کن
      }
    });
    console.log('Status:', res.status);
    console.log('Data:', res.data);
  } catch (error) {
    if (error.response) {
      console.log('Status code:', error.response.status);
      console.log('Response data:', error.response.data);
    } else {
      console.log('Error:', error.message);
    }
  }
}

testApi();
