require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const { parse } = require('pg-connection-string');
const fs = require('fs');

const app = express();
const connStr = process.env.DATABASE_POOL_URL || process.env.DATABASE_URL;
const ca = fs.readFileSync('./prod-ca-2021.crt','utf8');
const cfg = parse(connStr); cfg.ssl = { ca };
const pool = new Pool(cfg);

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