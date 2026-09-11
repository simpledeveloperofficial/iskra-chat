const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const express = require('express');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: '1d',
  setHeaders: (res, filePath) => {
    // index.html и манифест должны обновляться сразу, картинки — можно кэшировать дольше
    if (filePath.endsWith('.html') || filePath.endsWith('manifest.json')) {
      res.setHeader('Cache-Control', 'no-cache');
    } else if (/\.(png|jpg|jpeg|svg|ico)$/.test(filePath)) {
      res.setHeader('Cache-Control', 'public, max-age=604800, immutable');
    }
  },
}));

// Провайдер выбирается по наличию ключа: есть OPENAI_API_KEY — работаем на нём,
// иначе откатываемся на GEMINI_API_KEY. Так переключение между ними — это
// только смена переменной окружения, без правок кода.
const OPENAI_KEY = process.env.OPENAI_API_KEY;
const GEMINI_KEY = process.env.GEMINI_API_KEY;
const PROVIDER = OPENAI_KEY ? 'openai' : GEMINI_KEY ? 'gemini' : null;

if (!PROVIDER) {
  console.error('Не задан ни OPENAI_API_KEY, ни GEMINI_API_KEY — добавьте один из них в .env');
  console.error('OpenAI: https://platform.openai.com/api-keys | Gemini: https://aistudio.google.com/apikey');
  process.exit(1);
}

const MODEL = PROVIDER === 'openai' ? 'gpt-5-nano' : 'gemini-3.6-flash';
const PROVIDER_NAME = PROVIDER === 'openai' ? 'OpenAI' : 'Google Gemini';

// Оба провайдера отдают SSE в формате "data: {json}", различаются только
// телом запроса и тем, где внутри чанка лежит кусочек текста.
function buildUpstreamRequest(messages, systemText) {
  if (PROVIDER === 'openai') {
    return {
      url: 'https://api.openai.com/v1/chat/completions',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${OPENAI_KEY}` },
      body: {
        model: MODEL,
        stream: true,
        messages: [
          { role: 'system', content: systemText },
          ...messages.map((m) => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content })),
        ],
      },
    };
  }
  return {
    url: `https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:streamGenerateContent?alt=sse&key=${GEMINI_KEY}`,
    headers: { 'Content-Type': 'application/json' },
    body: {
      contents: messages.map((m) => ({
        role: m.role === 'assistant' ? 'model' : 'user',
        parts: [{ text: m.content }],
      })),
      systemInstruction: { parts: [{ text: systemText }] },
    },
  };
}

function extractDelta(chunk) {
  return PROVIDER === 'openai'
    ? chunk?.choices?.[0]?.delta?.content || ''
    : chunk?.candidates?.[0]?.content?.parts?.map((p) => p.text || '').join('') || '';
}

const BASE_INSTRUCTION = 'Тебя зовут Искра. Ты дружелюбный ИИ-помощник. Если спросят, кто тебя создал или на чём ты работаешь — просто скажи, что ты Искра, ассистент этого сайта, без лишних технических деталей. Форматируй ответы markdown: списки, заголовки, ```блоки кода``` там, где уместно. Отвечай на русском языке, если пользователь не пишет на другом.';

// Собирает системную инструкцию из базовой личности + персонализации
// пользователя (профиль хранится и передаётся с клиента, на сервере
// нигде не сохраняется).
function buildSystemInstruction(profile) {
  let text = BASE_INSTRUCTION;
  if (profile && typeof profile === 'object') {
    const lines = [];
    if (profile.name) lines.push(`Имя пользователя: ${String(profile.name).slice(0, 80)}. Обращайся к нему по имени, когда это уместно.`);
    if (profile.age) lines.push(`Возраст пользователя: ${String(profile.age).slice(0, 10)}.`);
    if (profile.occupation) lines.push(`Чем занимается пользователь: ${String(profile.occupation).slice(0, 200)}.`);
    if (profile.about) lines.push(`Что ещё важно знать о пользователе: ${String(profile.about).slice(0, 800)}.`);
    if (profile.style) lines.push(`Как отвечать этому пользователю: ${String(profile.style).slice(0, 800)}.`);
    if (lines.length) {
      text += '\n\nПерсонализация от пользователя (учитывай, но не пересказывай эти пункты вслух без повода):\n' + lines.join('\n');
    }
  }
  return text;
}

// Стриминг: сервер сам читает SSE-поток провайдера и пересобирает его в
// простой формат "data: {text}\n\n" — клиенту не нужно знать, чей это ответ.
app.post('/api/chat', async (req, res) => {
  const { messages, profile } = req.body;
  if (!Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: 'messages обязателен' });
  }

  const request = buildUpstreamRequest(messages, buildSystemInstruction(profile));

  let upstream;
  try {
    upstream = await fetch(request.url, {
      method: 'POST',
      headers: request.headers,
      body: JSON.stringify(request.body),
    });
  } catch (err) {
    return res.status(502).json({ error: `Не удалось связаться с ${PROVIDER_NAME}: ` + err.message });
  }

  if (!upstream.ok) {
    const data = await upstream.json().catch(() => ({}));
    const message = data?.error?.message || `Ошибка ${upstream.status}`;
    return res.status(upstream.status).json({ error: message });
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split('\n');
      buffer = lines.pop(); // хвост — возможно неполная строка

      for (const line of lines) {
        if (!line.startsWith('data:')) continue;
        const payload = line.slice(5).trim();
        if (!payload) continue;
        if (payload === '[DONE]') continue; // отправим свой [DONE] в конце
        try {
          const text = extractDelta(JSON.parse(payload));
          if (text) res.write(`data: ${JSON.stringify({ text })}\n\n`);
        } catch {
          // неполный JSON-чанк — пропускаем, дождёмся следующего куска
        }
      }
    }
  } catch (err) {
    res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
  }

  res.write('data: [DONE]\n\n');
  res.end();
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Искра запущена: http://localhost:${port}`);
  console.log(`Модель: ${MODEL} (${PROVIDER_NAME})`);
});
