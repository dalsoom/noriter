// top.js
require('dotenv').config();
const { Pool } = require('pg');
const { parse } = require('pg-connection-string');
const fs = require('fs');

// (선택) IPv4 우선
try { require('dns').setDefaultResultOrder('ipv4first'); } catch {}

const connStr = process.env.DATABASE_POOL_URL || process.env.DATABASE_URL;
console.log('Using host =', connStr.match(/@([^:/?]+)/)?.[1]);

// poll.js/seed.js에서 쓰던 CA 파일과 동일해야 합니다
const ca = fs.readFileSync('./prod-ca-2021.crt', 'utf8');

// poll.js와 동일: 문자열 → 설정 객체로 파싱 후 ssl.ca 주입
const config = parse(connStr);
config.ssl = { ca };

const pool = new Pool(config);

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