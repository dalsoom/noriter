require('dotenv').config();
const { Pool } = require('pg');
const { parse } = require('pg-connection-string');

const connStr = process.env.DATABASE_POOL_URL || process.env.DATABASE_URL;
if (!connStr) { console.error('Missing env: DATABASE_POOL_URL'); process.exit(1); }

const cfg = parse(connStr);
const host = (cfg.host || connStr.match(/@([^:/?]+)/)?.[1] || '').toLowerCase();
const isPooler = host.includes('.pooler.supabase.com');

// 1) pooler일 땐 시스템 CA 사용(ssl: true). CA 지정하지 않음!
const sslForPooler = true; // 또는 { rejectUnauthorized: true }

// 2) direct(db.<ref>.supabase.co)일 땐 Supabase CA 사용
const ca = process.env.SUPABASE_CA; // PEM 전체(멀티라인)
const sslForDirect = ca ? { ca } : { rejectUnauthorized: false }; // 진단용 우회 포함

cfg.ssl = isPooler ? sslForPooler : sslForDirect;

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

