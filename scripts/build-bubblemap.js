import axios from 'axios';
import fs from 'fs';
import { setTimeout as sleep } from 'timers/promises';
import open from 'open';

const BITQUERY_ENDPOINT = 'https://asia.graphql.bitquery.io';
const OAuth_token = "ory_at_"; //generate one from here https://account.bitquery.io/user/api_v2/access_tokens

if (!OAuth_token) {
  console.error('Missing BITQUERY_OAuth_token environment variable.');
  process.exit(1);
}

function buildGQL(receiverAddress) {
  return `
query TransfersForBubbleMap($since: ISO8601DateTime!, $currency: String, $limit: Int = 1000, $offset: Int = 0) {
  solana {
    transfers(
      date: { is: $since }
      options: { limit: $limit, offset: $offset, desc: ["date.date", "block.height"] }
      currency: { is: $currency }
      receiverAddress: { is: "${receiverAddress}" }
    ) {
      amount (in:USD)
      currency { symbol address decimals }
      sender { address }
      receiver { address }
      transaction { signature transactionIndex }
      block { height timestamp { iso8601 } }
      date { date }
    }
  }
}`;
}

async function fetchPage(gql, params, { timeoutMs, retries, backoffMs }) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log(`[fetchPage] attempt=${attempt} limit=${params.limit} offset=${params.offset} since=${params.since} currency=${params.currency}`);
      const res = await axios.request({
        method: 'post',
        url: BITQUERY_ENDPOINT,
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer ' + OAuth_token,
        },
        data: JSON.stringify({ query: gql, variables: params }),
        timeout: timeoutMs,
        maxBodyLength: Infinity,
        validateStatus: s => s >= 200 && s < 300,
      });
      console.log(`[fetchPage] HTTP status=${res.status}`);
      const json = res.data;
      console.log(`[fetchPage] parsed JSON, errors=${json.errors ? 'yes' : 'no'}`);
      if (json.errors) throw new Error(JSON.stringify(json.errors));
      const items = json.data.solana.transfers;
      console.log(`[fetchPage] items=${Array.isArray(items) ? items.length : 0}`);
      return items;
    } catch (e) {
      const status = e.response?.status;
      const body = e.response?.data;
      console.warn(`[fetchPage] error on attempt ${attempt}: ${e.message || e} status=${status ?? 'n/a'} body=${body ? JSON.stringify(body).slice(0, 300) : 'n/a'}`);
      if (attempt === retries) throw e;
      const delay = backoffMs * attempt;
      console.log(`[fetchPage] retrying after ${delay}ms`);
      await sleep(delay);
    } finally {
      // axios handles cancellation via timeout
    }
  }
}

function add(map, key, delta) {
  map.set(key, (map.get(key) || 0) + delta);
}

async function main() {
  // Parse command-line arguments
  const args = process.argv.slice(2);
  if (args.length < 2) {
    console.error('Usage: node build-bubblemap.js <currency_address> <receiver_address> [since_date]');
    console.error('Example: node build-bubblemap.js Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB CapuXNQoDviLvU1PxFiizLgPNQCxrsag1uMeyk6zLVps 2025-09-24');
    process.exit(1);
  }
  
  const currency = args[0];
  const receiverAddress = args[1];
  const since = args[2] || '2025-09-24'; // default date if not provided
  
  const pageSize = 100; // reduce if queries are heavy
  const timeoutMs = 60000; // 30s per request
  const retries = 5; // retry attempts
  const backoffMs = 800; // base backoff between retries (linear)
  const interPageSleepMs = 200; // ms delay between pages
  const maxPages = 4; // hard cap on number of pages to fetch

  console.log('[main] starting with configuration:', { since, currency, receiverAddress, pageSize, timeoutMs, retries, backoffMs, interPageSleepMs });

  // Build the GraphQL query with the receiver address
  const GQL = buildGQL(receiverAddress);

  const nodeValueUSD = new Map();
  const linkValueUSD = new Map();
  const linkCount = new Map();

  console.log('[main] beginning paging loop');
  let pageCount = 0;
  for (let offset = 0; ; offset += pageSize) {
    console.log(`[main] fetching page offset=${offset} limit=${pageSize}`);
    const page = await fetchPage(
      GQL,
      { since, currency, limit: pageSize, offset },
      { timeoutMs, retries, backoffMs }
    );
    console.log(`[main] received page length=${page ? page.length : 0}`);
    if (!page || page.length === 0) {
      console.log('[main] empty page received, stopping');
      break;
    }
    pageCount++;

    for (const t of page) {
      const usd = typeof t.amount === 'number' ? t.amount : 0;
      const sender = t.sender?.address || 'UNKNOWN_SENDER';
      const receiver = t.receiver?.address || 'UNKNOWN_RECEIVER';
      add(nodeValueUSD, sender, Math.abs(usd));
      add(nodeValueUSD, receiver, Math.abs(usd));
      const key = `${sender}->${receiver}`;
      add(linkValueUSD, key, Math.abs(usd));
      add(linkCount, key, 1);
    }
    console.log('[main] aggregated page into maps', {
      nodeCount: nodeValueUSD.size,
      linkPairs: linkValueUSD.size
    });
    if (pageCount >= maxPages) {
      console.log('[main] maxPages reached, stopping');
      break;
    }

    if (page.length < pageSize) {
      console.log('[main] last page detected (shorter than pageSize), stopping');
      break;
    }
    if (interPageSleepMs > 0) {
      console.log(`[main] sleeping ${interPageSleepMs}ms before next page`);
      await sleep(interPageSleepMs);
    }
  }

  const nodes = Array.from(nodeValueUSD.entries())
    .map(([id, value]) => ({ id, label: id, value }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 300);
  console.log('[main] built nodes', { totalNodes: nodeValueUSD.size, kept: nodes.length });

  const allowed = new Set(nodes.map(n => n.id));
  const links = [];
  for (const [key, value] of linkValueUSD.entries()) {
    const [source, target] = key.split('->');
    if (!allowed.has(source) || !allowed.has(target)) continue;
    links.push({ source, target, value, count: linkCount.get(key) || 0 });
  }
  console.log('[main] built links (pre-prune)', { eligible: links.length });

  links.sort((a, b) => b.value - a.value);
  const prunedLinks = links.slice(0, 1000);
  console.log('[main] pruned links', { kept: prunedLinks.length });

  const out = { nodes, links: prunedLinks };
  
  console.log('[main] reading index.html template');
  let html = await fs.promises.readFile('index.html', 'utf-8');
  
  console.log('[main] embedding data into HTML');
  const jsonData = JSON.stringify(out, null, 2);
  html = html.replace(
    'const data = await fetch(\'./bubblemap.json\').then(r => r.json());',
    `const data = ${jsonData};`
  );
  
  console.log('[main] writing output.html');
  await fs.promises.writeFile('output.html', html);
  console.log('Wrote output.html with', nodes.length, 'nodes and', prunedLinks.length, 'links');
  
  console.log('[main] opening output.html in browser');
  await open('output.html');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});


