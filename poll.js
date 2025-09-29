require('dotenv').config();
const { Pool } = require('pg');
const { parse } = require('pg-connection-string');
const dns = require('dns').promises;

// IPv4 우선(가능한 환경에서 기본 해석 순서 보정)
try { require('dns').setDefaultResultOrder('ipv4first'); } catch {}

async function createPoolFromEnv() {
  const url =
    process.env.DATABASE_URL_DIRECT ||     // Actions(Direct)
    process.env.DATABASE_POOL_URL ||       // 로컬에서 풀러 테스트할 때만
    process.env.DATABASE_URL;              // 기타

  if (!url) throw new Error('Missing DB URL env');

  const cfg = parse(url);
  const host = (cfg.host || '').toLowerCase();
  const isDirect = host.endsWith('.supabase.co');
  const isPooler = host.includes('.pooler.supabase.com');

  if (isDirect) {
    // 1) IPv4 주소로 강제
    const A = await dns.resolve4(host);
    if (!A || !A.length) throw new Error('No IPv4 A record for ' + host);
    const ipv4 = A[0];

    // 2) Direct는 반드시 CA로 검증
    // 로컬에서만 파일fallback이 필요하면 아래 두 줄을 쓰세요.
    // const fs = require('fs');
    // const ca = process.env.SUPABASE_CA || (fs.existsSync('./prod-ca-2021.crt') ? fs.readFileSync('./prod-ca-2021.crt','utf8') : undefined);
    const ca = process.env.SUPABASE_CA;
    if (!ca) throw new Error('SUPABASE_CA missing for direct');

    return new Pool({
      host: ipv4,
      port: Number(cfg.port || 5432),
      user: cfg.user || 'postgres',
      password: cfg.password,
      database: cfg.database || 'postgres',
      ssl: { ca },
      keepAlive: true,
    });
  }

  // 풀러(필요할 때만)
  return new Pool({
    connectionString: url,
    ssl: true,           // 시스템 CA 신뢰
    keepAlive: true,
  });
}

module.exports = { createPoolFromEnv };

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




