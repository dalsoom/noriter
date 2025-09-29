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
app.get('/api/top', async (req, res) => {
  try {
    const { category, limit = 20 } = req.query;
    const sql = `
      select video_id, title, channel_title, category_id, region,
             round(hotness_score::numeric,2) as score,
             round(comment_per_min::numeric,2) as cpm,
             round(comments_per_1k_views::numeric,3) as cpk
      from public.hotness
      ${category ? 'where category_id = $1' : ''}
      order by hotness_score desc
      limit ${Math.min(Number(limit)||20, 50)}
    `;
    const { rows } = await pool.query(sql, category ? [Number(category)] : []);
    res.json(rows);
  } catch (e) { console.error(e); res.status(500).json({ error: 'server_error' }); }
});

const port = process.env.PORT || 3000;

app.listen(port, () => console.log('API on http://localhost:'+port));




