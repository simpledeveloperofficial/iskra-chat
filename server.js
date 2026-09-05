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

const apiKey = process.env.OPENAI_API_KEY;
if (!apiKey) {
  console.error('OPENAI_API_KEY не задан. Создайте ключ на https://platform.openai.com/api-keys и добавьте его в .env');
  process.exit(1);
}

const MODEL = 'gpt-5-nano';
const OPENAI_URL = 'https://api.openai.com/v1/chat/completions';

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

// Стриминг: сервер сам читает SSE-поток OpenAI и пересобирает его в
// простой построчный формат "data: {text}\n\n" для клиента — клиенту
// не нужно знать формат ответа OpenAI.
app.post('/api/chat', async (req, res) => {
  const { messages, profile } = req.body;
  if (!Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: 'messages обязателен' });
  }

  const chatMessages = [
    { role: 'system', content: buildSystemInstruction(profile) },
    ...messages.map((m) => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content })),
  ];

  let upstream;
  try {
    upstream = await fetch(OPENAI_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({ model: MODEL, messages: chatMessages, stream: true }),
    });
  } catch (err) {
    return res.status(502).json({ error: 'Не удалось связаться с OpenAI: ' + err.message });
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
          const parsed = JSON.parse(payload);
          const text = parsed?.choices?.[0]?.delta?.content || '';
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
});
