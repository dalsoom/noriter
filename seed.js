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


async function fetchTrendingKR(categoryId = 10) {
  const url = `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode=KR&videoCategoryId=${categoryId}&maxResults=50&key=${process.env.YT_API_KEY}`;
  const res = await fetch(url);
  const json = await res.json();
  return json.items || [];
}

async function upsert(items) {
  const client = await pool.connect();
  const snapTs = new Date(Math.floor(Date.now() / 60000) * 60000); // 분 단위
  try {
    await client.query('begin');
    for (const it of items) {
      const s = it.statistics || {};
      const sn = it.snippet || {};
      await client.query(
        `insert into videos (video_id, title, channel_id, channel_title, category_id, region, published_at)
         values ($1,$2,$3,$4,$5,'KR',$6)
         on conflict (video_id) do update set
           title=excluded.title,
           channel_id=excluded.channel_id,
           channel_title=excluded.channel_title,
           category_id=excluded.category_id,
           published_at=excluded.published_at;`,
        [it.id, sn.title || '', sn.channelId || '', sn.channelTitle || '', Number(sn.categoryId || 0), sn.publishedAt || null]
      );
      await client.query(
        `insert into snapshots (video_id, captured_at, view_count, comment_count, like_count)
         values ($1,$2,$3,$4,$5)
         on conflict (video_id, captured_at) do nothing;`,
        [it.id, snapTs, Number(s.viewCount || 0), Number(s.commentCount || 0), s.likeCount != null ? Number(s.likeCount) : null]
      );
    }
    await client.query('commit');
  } catch (e) {
    await client.query('rollback');
    console.error(e);
  } finally {
    client.release();
  }
}

(async () => {
  const items = await fetchTrendingKR(10); // 음악 카테고리
  console.log('Fetched:', items.length);
  await upsert(items);
  await pool.end();
  console.log('Done');

})();




