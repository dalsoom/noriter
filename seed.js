// CommonJS 버전(쉽게 실행)
// Node 18+ 전제: fetch 내장

require('dotenv').config();
const { Pool } = require('pg');
const { parse } = require('pg-connection-string');
const fs = require('fs');

const connStr = process.env.DATABASE_POOL_URL || process.env.DATABASE_URL;
const config = parse(connStr);

// Supabase에서 다운로드한 CA 인증서 경로
const ca = fs.readFileSync('./prod-ca-2021.crt').toString();

// TLS 검증을 CA로 하도록 설정(권장)
config.ssl = { ca };

const pool = new Pool(config);


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