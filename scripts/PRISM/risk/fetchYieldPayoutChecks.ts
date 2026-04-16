import "dotenv/config";
import { MongoClient } from "mongodb";
import { ethers } from "ethers";

// ═══════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════

const ETH_RPC_URL  = process.env.ETH_RPC_URL!;
const MONGODB_URI  = process.env.MONGODB_CONNECTION_STRING!;
const MONGODB_DB   = "prod";
const COLLECTION   = "prism_yield_payout_checks";

const MONARQ_SENDER   = "0xb81a777A96603E69f990954b29ecF07F20669FB8";
const OE_WALLET       = "0x889e9C6d484201394Afd6Bce17996a16a8BbDa92";
const XPRISM_CONTRACT = "0x12E04c932D682a2999b4582F7c9B86171B73220D";
const PRISM_TOKEN     = "0x06Bb4ab600b7D22eB2c312f9bAbC22Be6a619046";

// Lookback: 48h to ensure no gaps between daily CI runs
const LOOKBACK_BLOCKS = 7200 * 2; // ~48h

// Alert thresholds (used for console summary only — Metabase derives these from the collection)
const TOPUP_EXPECTED_PER_24H  = 2;
const WEEKLY_YIELD_AMBER_DAYS = 7;
const WEEKLY_YIELD_RED_DAYS   = 8;

// ═══════════════════════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════════════════════

type RecordType = "weekly_yield" | "daily_topup";

interface PayoutRecord {
  hash: string;
  type: RecordType;
  timestamp: Date;
  dateKey: string;
  fromAddress: string;
  toAddress: string;
  asset: string;
  amountToken: number;
  blockNumber: number;
}

// ═══════════════════════════════════════════════════════════════════
// ALCHEMY FETCH (single page — 48h window fits well within 1000 limit)
// ═══════════════════════════════════════════════════════════════════

async function fetchTransfers(
  provider: ethers.JsonRpcProvider,
  params: {
    fromBlock: string;
    toBlock: string;
    fromAddress?: string;
    toAddress?: string;
    contractAddresses?: string[];
    category: string[];
  }
): Promise<any[]> {
  const result = await provider.send("alchemy_getAssetTransfers", [
    { ...params, withMetadata: true, excludeZeroValue: true, maxCount: "0x3e8" },
  ]);
  return result.transfers ?? [];
}

// ═══════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════

async function main() {
  const provider = new ethers.JsonRpcProvider(ETH_RPC_URL);
  const currentBlock = await provider.getBlockNumber();
  const fromBlock = "0x" + Math.max(0, currentBlock - LOOKBACK_BLOCKS).toString(16);

  console.log(`Fetching last 48h — from block ${fromBlock} to latest (current: ${currentBlock})`);

  const [yieldTransfers, topupTransfers] = await Promise.all([
    fetchTransfers(provider, {
      fromBlock,
      toBlock: "latest",
      fromAddress: MONARQ_SENDER,
      toAddress: OE_WALLET,
      category: ["erc20"],
    }),
    fetchTransfers(provider, {
      fromBlock,
      toBlock: "latest",
      fromAddress: OE_WALLET,
      toAddress: XPRISM_CONTRACT,
      contractAddresses: [PRISM_TOKEN],
      category: ["erc20"],
    }),
  ]);

  // Build records
  const records: PayoutRecord[] = [];

  for (const t of yieldTransfers) {
    const ts = new Date(t.metadata.blockTimestamp);
    records.push({
      hash: t.hash,
      type: "weekly_yield",
      timestamp: ts,
      dateKey: ts.toISOString().slice(0, 10),
      fromAddress: (t.from ?? "").toLowerCase(),
      toAddress: (t.to ?? "").toLowerCase(),
      asset: t.asset ?? "UNKNOWN",
      amountToken: t.value ?? 0,
      blockNumber: parseInt(t.blockNum, 16),
    });
  }

  for (const t of topupTransfers) {
    const ts = new Date(t.metadata.blockTimestamp);
    records.push({
      hash: t.hash,
      type: "daily_topup",
      timestamp: ts,
      dateKey: ts.toISOString().slice(0, 10),
      fromAddress: (t.from ?? "").toLowerCase(),
      toAddress: (t.to ?? "").toLowerCase(),
      asset: t.asset ?? "PRISM",
      amountToken: t.value ?? 0,
      blockNumber: parseInt(t.blockNum, 16),
    });
  }

  // Upsert — hash uniqueness prevents duplicates across overlapping runs
  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  try {
    const col = client.db(MONGODB_DB).collection(COLLECTION);
    await col.createIndex({ hash: 1 }, { unique: true });
    await col.createIndex({ type: 1, dateKey: 1 });
    await col.createIndex({ timestamp: 1 });

    let inserted = 0;
    for (const rec of records) {
      const result = await col.updateOne(
        { hash: rec.hash },
        { $setOnInsert: rec },
        { upsert: true }
      );
      if (result.upsertedCount > 0) inserted++;
    }
    console.log(`Upserted ${inserted} new records (${records.length - inserted} already existed)`);

    // ── Console summary (alert state derived from collection — same logic Metabase will use) ──
    const now = new Date();

    const lastYield = await col
      .find({ type: "weekly_yield" })
      .sort({ timestamp: -1 })
      .limit(1)
      .toArray();

    const lastTopup = await col
      .find({ type: "daily_topup" })
      .sort({ timestamp: -1 })
      .limit(1)
      .toArray();

    const daysSinceYield = lastYield.length > 0
      ? (now.getTime() - lastYield[0].timestamp.getTime()) / (1000 * 60 * 60 * 24)
      : 999;

    const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const topupsLast24h = await col.countDocuments({ type: "daily_topup", timestamp: { $gte: oneDayAgo } });
    const topupsLast7d  = await col.countDocuments({ type: "daily_topup", timestamp: { $gte: new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000) } });

    const yieldStatus  = daysSinceYield >= WEEKLY_YIELD_RED_DAYS ? "RED" : daysSinceYield >= WEEKLY_YIELD_AMBER_DAYS ? "AMBER" : "GREEN";
    const topupStatus  = topupsLast24h === 0 ? "RED" : topupsLast24h < TOPUP_EXPECTED_PER_24H ? "AMBER" : "GREEN";
    const overallStatus = yieldStatus === "RED" || topupStatus === "RED" ? "RED" : yieldStatus === "AMBER" || topupStatus === "AMBER" ? "AMBER" : "GREEN";

    const divider = "─".repeat(60);
    const dateKey = now.toISOString().slice(0, 10);
    console.log("\n" + divider);
    console.log("  xPRISM Yield Payout Checks — " + dateKey);
    console.log(divider);
    console.log(`\n  Overall:  ${overallStatus}`);

    console.log(`\n  CHECK 1 — Weekly Yield (Monarq → OE Wallet)`);
    console.log(`  Status:   ${yieldStatus}`);
    if (lastYield.length > 0) {
      console.log(`  Last:     ${lastYield[0].timestamp.toISOString()}  (${Math.round(daysSinceYield * 10) / 10}d ago)`);
      console.log(`  Amount:   ${lastYield[0].amountToken} ${lastYield[0].asset}`);
      console.log(`  Txn:      ${lastYield[0].hash}`);
    } else {
      console.log(`  Last:     NONE on record`);
    }

    console.log(`\n  CHECK 2 — Twice-Daily PRISM Drip (OE Wallet → xPRISM)`);
    console.log(`  Status:   ${topupStatus}`);
    console.log(`  Last 24h: ${topupsLast24h} / ${TOPUP_EXPECTED_PER_24H} expected`);
    console.log(`  Last 7d:  ${topupsLast7d} / ${TOPUP_EXPECTED_PER_24H * 7} expected`);
    if (lastTopup.length > 0) {
      const hAgo = Math.round(((now.getTime() - lastTopup[0].timestamp.getTime()) / (1000 * 60 * 60)) * 10) / 10;
      console.log(`  Last:     ${lastTopup[0].timestamp.toISOString()}  (${hAgo}h ago)`);
      console.log(`  Amount:   ${lastTopup[0].amountToken} PRISM`);
      console.log(`  Txn:      ${lastTopup[0].hash}`);
    } else {
      console.log(`  Last:     NONE on record`);
    }

    console.log("\n" + divider + "\n");
  } finally {
    await client.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
