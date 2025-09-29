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

const API_KEY = process.env.YT_API_KEY;

async function fetchStatsBatch(ids) {
  const url =
    'https://www.googleapis.com/youtube/v3/videos?part=statistics&id=' +
    ids.join(',') + '&key=' + API_KEY;
  const res = await fetch(url);
  const json = await res.json();
  if (!json.items) return [];
  return json.items.map(i => ({
    id: i.id,
    views: Number(i.statistics?.viewCount || 0),
    comments: Number(i.statistics?.commentCount || 0),
    likes: i.statistics?.likeCount != null ? Number(i.statistics.likeCount) : null,
  }));
}

function chunk(arr, n) { const r=[]; for (let i=0;i<arr.length;i+=n) r.push(arr.slice(i,i+n)); return r; }

async function runOnce() {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `select video_id from videos order by created_at desc limit 200`
    );
    const ids = rows.map(r => r.video_id);
    if (!ids.length) { console.log('no videos to poll'); return; }

    const snapTs = new Date(Math.floor(Date.now() / 60000) * 60000); // 분 단위 반올림
    let saved = 0;
    for (const group of chunk(ids, 50)) {
      const stats = await fetchStatsBatch(group);
      await client.query('begin');
      for (const s of stats) {
        await client.query(
          `insert into snapshots (video_id, captured_at, view_count, comment_count, like_count)
           values ($1,$2,$3,$4,$5)
           on conflict (video_id, captured_at) do nothing`,
          [s.id, snapTs, s.views, s.comments, s.likes]
        );
        saved++;
      }
      await client.query('commit');
      await new Promise(r => setTimeout(r, 400)); // 쿼터 보호
    }
    console.log('snapshots saved:', saved, 'at', snapTs.toISOString());
  } catch (e) {
    console.error(e);
  } finally {
    client.release();
  }
}

// === 스케줄러(파일을 직접 실행했을 때만) ===
if (require.main === module) {
  const cron = require('node-cron');

  let running = false;
  const job = async () => {
    if (running) return console.log('skip: previous run still running');
    running = true;
    try { await runOnce(); }
    catch (e) { console.error(e); }
    finally { running = false; }
  };

  // 1회 즉시 실행
  job();
  // 15분마다 실행(한국 시간대)
  cron.schedule('*/15 * * * *', job, { timezone: 'Asia/Seoul' });

  // 종료 시 커넥션 깔끔히 닫기
  const shutdown = async () => { try { await pool.end(); } finally { process.exit(0); } };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

// 다른 파일에서 수동 호출할 수 있게 export 유지

module.exports = { runOnce };


