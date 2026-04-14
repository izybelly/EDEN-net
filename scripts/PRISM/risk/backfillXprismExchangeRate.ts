/**
 * One-time backfill: fetches xPRISM → PRISM exchange rate for every day
 * from contract deployment to yesterday, then upserts into MongoDB.
 *
 * Collection: prod.xprism_exchange_rate_history
 * Schema:     { dateKey, blockNumber, exchangeRate, createdAt }
 *
 * Run:
 *   npm run build && npm run backfill-xprism-rate
 *
 * Safe to re-run — uses upserts keyed on dateKey. Already-present days
 * are overwritten only if the exchange rate changed (shouldn't happen).
 *
 * Alchemy Archive Node required (ETH_RPC_URL must point to an archive endpoint).
 */

import "dotenv/config";
import { ethers } from "ethers";
import { MongoClient } from "mongodb";

// ── Config ────────────────────────────────────────────────────────────────────

const ETH_RPC_URL  = process.env.ETH_RPC_URL!;
const MONGODB_URI  = process.env.MONGODB_CONNECTION_STRING!;

const XPRISM_ADDRESS = "0x12E04c932D682a2999b4582F7c9B86171B73220D";
const PRISM_ADDRESS  = "0x06Bb4ab600b7D22eB2c312f9bAbC22Be6a619046";

const MONGODB_DB   = "prod";
const COLLECTION   = "xprism_exchange_rate_history";

// Ethereum mainnet: post-Merge block time is very stable at 12 seconds
const AVG_BLOCK_TIME_SECS = 12;

// Lower bound for deployment binary search: block 18_900_000 ≈ Jan 1 2024
const SEARCH_LO = 18_900_000;

// Max concurrent eth_call requests per batch (respect Alchemy compute units)
const BATCH_SIZE = 15;

// ── ABIs ──────────────────────────────────────────────────────────────────────

const ERC4626_ABI = [
  "function decimals() view returns (uint8)",
  "function convertToAssets(uint256 shares) view returns (uint256)",
];
const ERC20_ABI = [
  "function decimals() view returns (uint8)",
];

// ── Helpers ───────────────────────────────────────────────────────────────────

function dateKeyOf(d: Date): string {
  return d.toISOString().slice(0, 10);
}

/** Returns midnight UTC for the given date. */
function midnightUTC(d: Date): Date {
  const out = new Date(d);
  out.setUTCHours(0, 0, 0, 0);
  return out;
}

/**
 * Binary search: finds the first block where xPRISM is deployed.
 * Makes ~log2(hi - lo) ≈ 18 RPC calls.
 */
async function findDeploymentBlock(
  provider: ethers.JsonRpcProvider,
  hi: number,
): Promise<{ blockNumber: number; blockTimestamp: number }> {
  console.log(`Searching for xPRISM deployment block in [${SEARCH_LO.toLocaleString()}, ${hi.toLocaleString()}]...`);
  let lo = SEARCH_LO;

  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    const code = await provider.getCode(XPRISM_ADDRESS, mid);
    if (code === "0x") {
      lo = mid;
    } else {
      hi = mid;
    }
  }

  // hi is the first block with deployed bytecode
  const block = await provider.getBlock(hi);
  const ts = block!.timestamp;
  console.log(`  → Deployment block: ${hi.toLocaleString()} (${new Date(ts * 1000).toISOString()})`);
  return { blockNumber: hi, blockTimestamp: ts };
}

/**
 * Estimates the block closest to targetTimestampSec using linear interpolation
 * from the latest known block. Accurate to ±few minutes which is fine for
 * daily granularity.
 */
function estimateBlock(
  targetTimestampSec: number,
  latestBlock: number,
  latestTimestampSec: number,
  deployBlock: number,
): number {
  const blocksBack = (latestTimestampSec - targetTimestampSec) / AVG_BLOCK_TIME_SECS;
  return Math.max(deployBlock, Math.round(latestBlock - blocksBack));
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  const provider = new ethers.JsonRpcProvider(ETH_RPC_URL);

  // Read decimals once — used for all historical calls
  const prism  = new ethers.Contract(PRISM_ADDRESS,  ERC20_ABI,   provider);
  const xprism = new ethers.Contract(XPRISM_ADDRESS, ERC4626_ABI, provider);

  const [prismDecimals, xprismDecimals] = await Promise.all([
    prism.decimals()  as Promise<number>,
    xprism.decimals() as Promise<number>,
  ]);
  const oneShare = 10n ** BigInt(xprismDecimals);

  console.log(`PRISM decimals: ${prismDecimals}  xPRISM decimals: ${xprismDecimals}`);

  // Anchor: latest block (used for block estimation)
  const latestBlockNumber = await provider.getBlockNumber();
  const latestBlockData   = await provider.getBlock(latestBlockNumber);
  const latestTimestampSec = latestBlockData!.timestamp;

  console.log(`Latest block: ${latestBlockNumber.toLocaleString()} (${new Date(latestTimestampSec * 1000).toISOString()})`);

  // Find deployment block
  const { blockNumber: deployBlock, blockTimestamp: deployTs } =
    await findDeploymentBlock(provider, latestBlockNumber);

  // Build list of midnight-UTC dates from day after deployment to yesterday
  const deployDay = midnightUTC(new Date(deployTs * 1000));
  // Start from day after deployment (first full day of operation)
  const startDay  = new Date(deployDay.getTime() + 86_400_000);
  const yesterday = midnightUTC(new Date(Date.now() - 86_400_000));

  const dates: Date[] = [];
  const cursor = new Date(startDay);
  while (cursor <= yesterday) {
    dates.push(new Date(cursor));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  console.log(
    `\nDates to fetch: ${dates.length}` +
    `  (${dateKeyOf(startDay)} → ${dateKeyOf(yesterday)})`
  );

  // ── Fetch exchange rate for each day ───────────────────────────────────────

  interface RateRecord {
    dateKey: string;
    blockNumber: number;
    exchangeRate: number;
    createdAt: Date;
  }

  const records: RateRecord[] = [];
  let failed = 0;

  for (let i = 0; i < dates.length; i += BATCH_SIZE) {
    const batch = dates.slice(i, i + BATCH_SIZE);

    const settled = await Promise.allSettled(
      batch.map(async (date) => {
        const targetTs = date.getTime() / 1000;
        const blockNum = estimateBlock(targetTs, latestBlockNumber, latestTimestampSec, deployBlock);

        const raw = await xprism.convertToAssets(oneShare, { blockTag: blockNum }) as bigint;
        const rate = Number(ethers.formatUnits(raw, prismDecimals));

        return {
          dateKey: dateKeyOf(date),
          blockNumber: blockNum,
          exchangeRate: rate,
          createdAt: new Date(),
        };
      })
    );

    for (const result of settled) {
      if (result.status === "fulfilled") {
        records.push(result.value);
      } else {
        failed++;
        console.warn(`  ✗ Batch item failed: ${(result.reason as Error).message}`);
      }
    }

    const done = Math.min(i + BATCH_SIZE, dates.length);
    const lastRecord = records[records.length - 1];
    console.log(
      `  [${done}/${dates.length}] last: ${lastRecord?.dateKey ?? "-"} ` +
      `rate=${lastRecord?.exchangeRate?.toFixed(6) ?? "-"}`
    );

    // Brief pause between batches to avoid Alchemy rate limits
    if (i + BATCH_SIZE < dates.length) {
      await new Promise((r) => setTimeout(r, 300));
    }
  }

  console.log(`\nFetched ${records.length} records, ${failed} failed`);

  if (records.length === 0) {
    console.error("No records to insert — exiting.");
    return;
  }

  // ── Upsert into MongoDB ────────────────────────────────────────────────────

  const client = new MongoClient(MONGODB_URI);
  try {
    await client.connect();
    const col = client.db(MONGODB_DB).collection<RateRecord>(COLLECTION);

    // Ensure index on dateKey for fast lookups in Metabase
    await col.createIndex({ dateKey: 1 }, { unique: true });

    const ops = records.map((r) => ({
      updateOne: {
        filter: { dateKey: r.dateKey },
        update: { $set: r },
        upsert: true,
      },
    }));

    const bulkResult = await col.bulkWrite(ops, { ordered: false });
    console.log(
      `MongoDB: ${bulkResult.upsertedCount} inserted, ` +
      `${bulkResult.modifiedCount} updated, ` +
      `${bulkResult.matchedCount} unchanged`
    );
  } finally {
    await client.close();
  }

  console.log("\nDone.");

  // ── Print sample ──────────────────────────────────────────────────────────

  console.log("\nSample (first 5 + last 5):");
  const sample = [
    ...records.slice(0, 5),
    ...(records.length > 10 ? records.slice(-5) : []),
  ];
  for (const r of sample) {
    console.log(`  ${r.dateKey}  block=${r.blockNumber.toLocaleString()}  rate=${r.exchangeRate.toFixed(8)}`);
  }
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
