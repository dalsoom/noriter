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





