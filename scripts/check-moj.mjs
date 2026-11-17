#!/usr/bin/env node
/*
 * 以全國法規資料庫的公開資料，核對本選輯收錄的法規在出版後是否已修正或廢止。
 *
 * 能回答什麼、不能回答什麼（重要）：
 *   本頁條文取自教育部《技術及職業教育法規選輯》PDF，官方公開資料是 XML，
 *   同一條文在兩邊的分行、全形空白與款項編號樣式都不一致，逐條文字比對
 *   會產生大量假差異。因此本核對只做法規層級：比對官方的最新異動日期與
 *   廢止註記，告訴你「這一部法規在選輯出版後官方又修正過」，不判斷是哪
 *   一條變了。
 *
 * 來源（取自 openlawtw 於 2026-11-10 實際抓取時記錄的端點）：
 *   法律 https://law.moj.gov.tw/api/ch/law/xml    封存檔 ChLaw.xml
 *   命令 https://law.moj.gov.tw/api/ch/order/xml  封存檔 ChOrder.xml
 *   授權 政府資料開放授權條款第 1 版
 *
 * 產出：
 *   moj-report.md   報告
 *   index.html      更新 meta.mojCheck（核對日與已修正清單），供頁面顯示
 *   GITHUB_OUTPUT   changed、n_stale、routine 等
 *
 * 無外部套件，Node 20 以上可直接執行。需要 unzip 或 gunzip 處理封存檔。
 */

import { readFile, writeFile, appendFile, mkdtemp, rm } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createInterface } from 'node:readline';
import { createReadStream } from 'node:fs';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import path from 'node:path';
import process from 'node:process';

const SOURCES = [
  { kind: '法律', url: 'https://law.moj.gov.tw/api/ch/law/xml', member: 'ChLaw.xml' },
  { kind: '命令', url: 'https://law.moj.gov.tw/api/ch/order/xml', member: 'ChOrder.xml' },
];

const LOCAL_HTML = new URL('../index.html', import.meta.url);
const REPORT = new URL('../moj-report.md', import.meta.url);
const TODAY = new Date().toISOString().slice(0, 10);

/* 例行回報月份。檢查每月執行：一來 GitHub 在公開 repo 連續 60 天沒有
   repository 活動時會自動停用排程且不發通知，半年的 cron 會在觸發前失效；
   二來法規一旦修正，愈早知道愈好。 */
const REPORT_MONTHS = (process.env.REPORT_MONTHS || '1,7')
  .split(',')
  .map((n) => parseInt(n.trim(), 10))
  .filter((n) => n >= 1 && n <= 12);
const THIS_MONTH = new Date().getUTCMonth() + 1;
const IS_REPORT_MONTH = REPORT_MONTHS.includes(THIS_MONTH);

/* ---------------------------------------------------------------- 日期 */

/* 「中華民國 103 年 6 月 18 日」→ 2014-06-18 */
function rocToIso(s) {
  const m = String(s ?? '').match(/民國\s*(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日/);
  if (!m) return '';
  const y = parseInt(m[1], 10) + 1911;
  return `${y}-${String(m[2]).padStart(2, '0')}-${String(m[3]).padStart(2, '0')}`;
}

/* 去掉法規名稱結尾的括號註記，例如「…辦法(令釋)」→「…辦法」 */
const baseName = (s) => String(s ?? '').replace(/[（(][^）)]*[）)]\s*$/, '').trim();

/* 官方最新異動日期可能是 20241126、2024-11-26 或民國表示，一律化為 ISO */
function officialToIso(s) {
  const t = String(s ?? '').trim();
  let m = t.match(/^(\d{4})-?(\d{2})-?(\d{2})$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  m = t.match(/^(\d{3})(\d{2})(\d{2})$/); // 民國 1131126
  if (m) return `${parseInt(m[1], 10) + 1911}-${m[2]}-${m[3]}`;
  const roc = rocToIso(t);
  if (roc) return roc;
  return '';
}

/* ---------------------------------------------------------------- 下載 */

async function download(url, dest) {
  const res = await fetch(url, {
    headers: { 'user-agent': 'tvet-law moj check', accept: '*/*' },
    redirect: 'follow',
  });
  if (!res.ok) throw new Error(`下載 ${url} 失敗，HTTP ${res.status}`);
  await pipeline(Readable.fromWeb(res.body), createWriteStream(dest));
  return dest;
}

/* 依檔頭判斷封存格式，回傳一個可讀取 XML 文字的 stream */
async function openXmlStream(file, member) {
  const fh = await readFile(file, { encoding: null });
  const magic = fh.subarray(0, 4);
  const isZip = magic[0] === 0x50 && magic[1] === 0x4b;
  const isGzip = magic[0] === 0x1f && magic[1] === 0x8b;

  if (isZip) {
    const list = await run('unzip', ['-Z1', file]);
    const names = list.split('\n').map((s) => s.trim()).filter(Boolean);
    const pick =
      names.find((n) => n.toLowerCase() === member.toLowerCase()) ||
      names.find((n) => n.toLowerCase().endsWith('.xml')) ||
      names[0];
    if (!pick) throw new Error(`封存檔 ${file} 內沒有檔案`);
    const child = spawn('unzip', ['-p', file, pick], { stdio: ['ignore', 'pipe', 'inherit'] });
    return { stream: child.stdout, member: pick, format: 'zip' };
  }
  if (isGzip) {
    const child = spawn('gunzip', ['-c', file], { stdio: ['ignore', 'pipe', 'inherit'] });
    return { stream: child.stdout, member: `${member}（gzip）`, format: 'gzip' };
  }
  return { stream: createReadStream(file), member: `${member}（未封存）`, format: 'xml' };
}

function run(cmd, args) {
  return new Promise((resolve, reject) => {
    const c = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'inherit'] });
    let out = '';
    c.stdout.on('data', (d) => (out += d));
    c.on('error', reject);
    c.on('close', (code) => (code === 0 ? resolve(out) : reject(new Error(`${cmd} 退出碼 ${code}`))));
  });
}

/* ---------------------------------------------------------------- 解析 */

const TAG = {
  name: ['法規名稱'],
  url: ['法規網址'],
  changed: ['最新異動日期'],
  abolished: ['廢止註記'],
  nature: ['法規性質'],
};

function pick(xml, keys) {
  for (const k of keys) {
    const m = xml.match(new RegExp(`<${k}>([\\s\\S]*?)</${k}>`));
    if (m) return m[1].replace(/<!\[CDATA\[|\]\]>/g, '').trim();
  }
  return '';
}

/* 逐行讀取，累積 <法規> … </法規> 區塊後取出需要的欄位。
   全文不留在記憶體，只保留 pcode 對應的小物件。 */
async function parseArchive(src, want, diag) {
  const dir = await mkdtemp(path.join(tmpdir(), 'moj-'));
  const file = path.join(dir, 'archive.bin');
  try {
    /* MOJ_FIXTURE_DIR 指向本機樣本時不連網，供離線測試與日後除錯使用。
       樣本檔名為封存檔名，例如 ChLaw.xml.zip、ChOrder.xml.zip 或 ChLaw.xml。 */
    if (process.env.MOJ_FIXTURE_DIR) {
      const base = path.join(process.env.MOJ_FIXTURE_DIR, src.member);
      const cand = [`${base}.zip`, `${base}.gz`, base];
      let used = '';
      for (const c of cand) {
        try {
          await writeFile(file, await readFile(c));
          used = c;
          break;
        } catch {
          /* 換下一個候選 */
        }
      }
      if (!used) throw new Error(`MOJ_FIXTURE_DIR 中找不到 ${src.member}（含 .zip 與 .gz）`);
      diag.push(`${src.kind}：使用本機樣本 ${used}`);
    } else {
      await download(src.url, file);
    }
    const opened = await openXmlStream(file, src.member);
    diag.push(`${src.kind}：${src.url} → ${opened.member}（${opened.format}）`);

    const rl = createInterface({ input: opened.stream, crlfDelay: Infinity });
    const found = new Map();
    let buf = null;
    let records = 0;
    let head = '';

    for await (const line of rl) {
      if (head.length < 1500) head += `${line}\n`;
      if (buf === null) {
        if (line.includes('<法規>')) buf = [line];
        continue;
      }
      buf.push(line);
      if (!line.includes('</法規>')) continue;

      const xml = buf.join('\n');
      buf = null;
      records += 1;
      const url = pick(xml, TAG.url);
      const pc = (url.match(/[Pp][Cc]ode=([A-Z]\d{7})/) || [])[1];
      if (!pc || !want.has(pc)) continue;
      found.set(pc, {
        pcode: pc,
        name: pick(xml, TAG.name),
        changed: pick(xml, TAG.changed),
        abolished: pick(xml, TAG.abolished),
        nature: pick(xml, TAG.nature),
        url,
      });
    }

    diag.push(`${src.kind}：共讀到 ${records} 部法規，命中本選輯 ${found.size} 部`);
    if (records === 0) {
      throw new Error(
        `${src.kind} 封存檔解析不到任何 <法規> 區塊，XML 結構可能與預期不同。開頭 1500 字如下，請據此修正 scripts/check-moj.mjs：\n${head}`,
      );
    }
    return found;
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

/* ---------------------------------------------------------------- 本頁 */

const DATA_RE = /var DATA = (\{[\s\S]*?\});\r?\n/;

async function readLocal() {
  const html = await readFile(LOCAL_HTML, 'utf8');
  const m = html.match(DATA_RE);
  if (!m) throw new Error('index.html 找不到 var DATA 宣告，資料結構可能已改，請檢查本腳本');
  const data = JSON.parse(m[1]);
  return { html, meta: data.meta || {}, laws: data.laws || [] };
}

/* ---------------------------------------------------------------- 主程序 */

async function main() {
  const local = await readLocal();
  const withPcode = local.laws.filter((l) => l.pcode);
  const want = new Set(withPcode.map((l) => l.pcode));

  const diag = [];
  const official = new Map();
  for (const src of SOURCES) {
    const found = await parseArchive(src, want, diag);
    for (const [k, v] of found) if (!official.has(k)) official.set(k, { ...v, kind: src.kind });
  }

  const stale = [];     // 官方最新異動日期晚於選輯所載
  const abolished = []; // 官方已標示廢止
  const renamed = [];   // 名稱不一致
  const missing = [];   // 官方兩包都查不到
  const ok = [];

  for (const l of withPcode) {
    const o = official.get(l.pcode);
    const mine = rocToIso(l.hist);
    if (!o) {
      missing.push({ pcode: l.pcode, name: l.name, mine });
      continue;
    }
    const theirs = officialToIso(o.changed);
    const row = {
      pcode: l.pcode,
      name: l.name,
      officialName: o.name,
      mine,
      theirs,
      kind: o.kind,
      abolished: o.abolished,
    };
    if (/[^無否N]/.test(String(o.abolished || '').trim()) && String(o.abolished || '').trim() !== '') {
      abolished.push(row);
      continue;
    }
    /* 選輯中有同一 pcode 收兩筆的情形（本法與其「(令釋)」共用代碼），
       比對名稱時先去掉結尾的括號註記，否則每次都會誤報更名。 */
    if (o.name && l.name && baseName(o.name) !== baseName(l.name)) renamed.push(row);
    if (mine && theirs && theirs > mine) stale.push(row);
    else ok.push(row);
  }

  stale.sort((a, b) => (a.theirs < b.theirs ? 1 : -1));
  const needsWork = stale.length > 0 || abolished.length > 0;

  /* ------------------------------------------------------------ 報告 */

  const L = [];
  L.push('## 官方法規異動核對');
  L.push('');
  L.push(`核對日：${TODAY}　來源：全國法規資料庫公開資料（政府資料開放授權條款第 1 版）`);
  L.push('');
  L.push(`本選輯收錄 ${local.laws.length} 部法規，其中 ${withPcode.length} 部有全國法規資料庫代碼可供核對，另 ${local.laws.length - withPcode.length} 部為行政規則、要點或基準，官方資料庫查不到，不在核對範圍。`);
  L.push('');
  L.push('| 檢查項目 | 部數 |');
  L.push('| --- | --- |');
  L.push(`| 官方已修正（選輯內容可能過期） | ${stale.length} |`);
  L.push(`| 官方標示廢止 | ${abolished.length} |`);
  L.push(`| 名稱與官方不一致 | ${renamed.length} |`);
  L.push(`| 官方查無此代碼 | ${missing.length} |`);
  L.push(`| 與選輯所載日期相同或更舊 | ${ok.length} |`);
  L.push('');

  if (abolished.length) {
    L.push(`### 官方標示廢止（${abolished.length} 部，優先處理）`);
    L.push('');
    L.push('| 法規 | 代碼 | 廢止註記 |');
    L.push('| --- | --- | --- |');
    for (const r of abolished) L.push(`| ${r.name} | ${r.pcode} | ${r.abolished} |`);
    L.push('');
    L.push('選輯中仍收錄已廢止的法規時，應在該部法規的卡片上標示，避免教師誤用。');
    L.push('');
  }

  if (stale.length) {
    L.push(`### 官方已修正（${stale.length} 部）`);
    L.push('');
    L.push('| 法規 | 代碼 | 位階 | 選輯所載 | 官方最新異動 |');
    L.push('| --- | --- | --- | --- | --- |');
    for (const r of stale) {
      L.push(`| ${r.name} | ${r.pcode} | ${r.kind} | ${r.mine} | ${r.theirs} |`);
    }
    L.push('');
    L.push('這份清單代表該部法規在選輯出版後官方又修正過，**不代表本頁哪一條文字有變**。要確認差異須開啟該部法規的官方頁面逐條核對。');
    L.push('');
  }

  if (renamed.length) {
    L.push(`### 名稱與官方不一致（${renamed.length} 部）`);
    L.push('');
    L.push('| 選輯名稱 | 官方名稱 | 代碼 |');
    L.push('| --- | --- | --- |');
    for (const r of renamed) L.push(`| ${r.name} | ${r.officialName} | ${r.pcode} |`);
    L.push('');
    L.push('可能是法規更名，也可能是代碼比對有誤，須人工判斷。');
    L.push('');
  }

  if (missing.length) {
    L.push(`### 官方查無此代碼（${missing.length} 部）`);
    L.push('');
    for (const r of missing) L.push(`- ${r.pcode} ${r.name}`);
    L.push('');
    L.push('可能是代碼有誤、法規已移除，或該法規不在法律與命令這兩包公開資料中。');
    L.push('');
  }

  L.push('### 執行診斷');
  L.push('');
  for (const d of diag) L.push(`- ${d}`);
  L.push('');

  if (!needsWork) {
    L.push('**結論：有代碼的法規在官方均無更新或廢止紀錄，選輯內容仍可使用。**');
  } else {
    L.push('**結論：有需要人工處理的項目，見上方清單。**');
  }

  L.push('');
  L.push('> 本核對只做法規層級的日期與廢止比對，不逐條比對文字（來源格式不同，逐字比對會產生大量假差異）。正式引用前仍應以官方現行條文為準。');
  L.push('');
  L.push(
    IS_REPORT_MONTH
      ? `<sub>由 \`scripts/check-moj.mjs\` 於 ${new Date().toISOString()} 產生。本月（${THIS_MONTH} 月）為例行回報月份，設定為 ${REPORT_MONTHS.join('、')} 月；檢查每月執行。</sub>`
      : `<sub>由 \`scripts/check-moj.mjs\` 於 ${new Date().toISOString()} 產生。例行回報月份為 ${REPORT_MONTHS.join('、')} 月，本月僅在有異動時通知。</sub>`,
  );

  const report = L.join('\n');
  await writeFile(REPORT, `${report}\n`, 'utf8');
  console.log(report);

  /* ------------------------------------------ 把結果寫回頁面的 meta */

  const payload = {
    checked: TODAY,
    nChecked: withPcode.length,
    stale: stale.map((r) => ({ pcode: r.pcode, name: r.name, mine: r.mine, official: r.theirs })),
    abolished: abolished.map((r) => ({ pcode: r.pcode, name: r.name, note: r.abolished })),
  };
  /* mojCheck 固定是 meta 的最後一個欄位，meta 之後緊接 "laws"，
     所以用 }," laws": 當右界可以唯一定位，不會被 payload 內的巢狀括號干擾。 */
  const token = JSON.stringify(payload);
  const FIELD_RE = /"mojCheck":[\s\S]*?(?=\},"laws":)/;
  const hits = local.html.match(/"mojCheck":/g) || [];
  let bumped = false;
  if (hits.length === 1 && FIELD_RE.test(local.html)) {
    const next = local.html.replace(FIELD_RE, `"mojCheck":${token}`);
    if (next !== local.html) {
      await writeFile(LOCAL_HTML, next, 'utf8');
      bumped = true;
      console.log(`\n已更新 meta.mojCheck：核對日 ${TODAY}，已修正 ${stale.length} 部，廢止 ${abolished.length} 部`);
    }
  } else {
    console.log(`\nmeta.mojCheck 欄位出現 ${hits.length} 次，為避免誤改未更新，請人工檢查`);
  }

  if (process.env.GITHUB_OUTPUT) {
    await appendFile(
      process.env.GITHUB_OUTPUT,
      [
        `changed=${needsWork}`,
        `n_stale=${stale.length}`,
        `n_abolished=${abolished.length}`,
        `routine=${!needsWork && IS_REPORT_MONTH}`,
        `page_bumped=${bumped}`,
        `checked_date=${TODAY}`,
      ].join('\n') + '\n',
      'utf8',
    );
  }
}

main().catch((err) => {
  console.error(`核對失敗：${err.message}`);
  process.exit(1);
});
