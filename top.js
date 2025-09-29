require('dotenv').config();
const { Pool } = require('pg');
const { parse } = require('pg-connection-string');

const url =
  process.env.DATABASE_URL_DIRECT ||     // Actions(Direct 용)
  process.env.DATABASE_POOL_URL ||       // 로컬에서 기존 Pooler 쓰던 경우
  process.env.DATABASE_URL;              // 기타

if (!url) {
  console.error('Missing env: DATABASE_URL_DIRECT / DATABASE_POOL_URL / DATABASE_URL');
  process.exit(1);
}

const cfg = parse(url);
const host = (cfg.host || '').toLowerCase();
const isPooler = host.includes('.pooler.supabase.com');
const isDirect = host.endsWith('.supabase.co');

let ssl;
if (isDirect) {
  // Direct(5432) → Supabase CA로 엄격 검증
  const ca = process.env.SUPABASE_CA;
  if (!ca) {
    console.error('SUPABASE_CA missing for direct connection');
    process.exit(1);
  }
  ssl = { ca };
} else if (isPooler) {
  // Pooler(6543) → 시스템 CA(ssl:true) 또는 임시 우회(원하면)
  ssl = true;
} else {
  // 기타 환경(진단용 우회, 필요시 제거)
  ssl = { rejectUnauthorized: false };
}

cfg.ssl = ssl;
const pool = new Pool(cfg);
// 이후 pool 사용…

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



