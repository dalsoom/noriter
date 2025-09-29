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

(async () => {
  try {
    const { rows } = await pool.query(`
      select video_id, title,
             round(hotness_score::numeric,2) as score,
             round(comment_per_min::numeric,2) as cpm,
             round(comments_per_1k_views::numeric,3) as cpk
      from public.hotness
      order by hotness_score desc
      limit 10
    `);
    console.table(rows);
  } catch (e) {
    console.error(e);
  } finally {
    await pool.end();
  }

})();


