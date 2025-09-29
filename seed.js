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

