require('dotenv').config();
const { Pool } = require('pg');
const { parse } = require('pg-connection-string');

const url = process.env.DATABASE_POOL_URL || process.env.DATABASE_URL;
if (!url) { console.error('Missing env: DATABASE_POOL_URL'); process.exit(1); }

const cfg = parse(url);
const host = (cfg.host || '').toLowerCase();
const isPooler = host.includes('.pooler.supabase.com');

let ssl;
if (isPooler) ssl = true;                   // pooler는 시스템 CA 사용
else {
  const ca = process.env.SUPABASE_CA;       // direct일 때만 CA
  ssl = ca ? { ca } : { rejectUnauthorized: false }; // 우회는 진단용
}

cfg.ssl = ssl;
const pool = new Pool(cfg);

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


