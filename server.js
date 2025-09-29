require('dotenv').config();
const { Pool } = require('pg');
const { parse } = require('pg-connection-string');

function createPool() {
  const url =
    process.env.DATABASE_POOL_URL ||
    process.env.DATABASE_URL_DIRECT ||
    process.env.DATABASE_URL;

  if (!url) throw new Error('Missing DB URL env');

  const cfg = parse(url);
  const host = (cfg.host || '').toLowerCase();
  const isPooler = host.includes('.pooler.supabase.com');
  const isDirect = host.endsWith('.supabase.co');

  let ssl;
  if (isPooler) {
    // GH Runner에서는 체인이 안 맞는 케이스가 있어 검증 임시 우회
    ssl = { rejectUnauthorized: false };
  } else if (isDirect) {
    const ca = process.env.SUPABASE_CA;
    if (!ca) throw new Error('SUPABASE_CA missing for direct');
    ssl = { ca };
  } else {
    ssl = true; // 기타
  }
  return new Pool({ connectionString: url, ssl, keepAlive: true });
}

module.exports = { createPool };

app.get('/api/top', async (req, res) => {
  try {
    const { category, limit = 20 } = req.query;
    const sql = `
      select video_id, title, channel_title, category_id, region,
             round(hotness_score::numeric,2) as score,
             round(comment_per_min::numeric,2) as cpm,
             round(comments_per_1k_views::numeric,3) as cpk
      from public.hotness
      ${category ? 'where category_id = $1' : ''}
      order by hotness_score desc
      limit ${Math.min(Number(limit)||20, 50)}
    `;
    const { rows } = await pool.query(sql, category ? [Number(category)] : []);
    res.json(rows);
  } catch (e) { console.error(e); res.status(500).json({ error: 'server_error' }); }
});

const port = process.env.PORT || 3000;

app.listen(port, () => console.log('API on http://localhost:'+port));





