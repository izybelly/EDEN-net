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

// Mar-26-2026 06:14:35 AM UTC — ~24,739,366
// Derived: block 24,679,894 (2026-03-18) + 8d * 7200 + 6.24h * 300
const START_BLOCK = "0x179C206"; // 24,739,846 hex

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
// ALCHEMY PAGINATED FETCH
// ═══════════════════════════════════════════════════════════════════

async function fetchAllTransfers(
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
  const all: any[] = [];
  let pageKey: string | undefined;

  do {
    const reqParams: any = {
      ...params,
      withMetadata: true,
      excludeZeroValue: true,
      maxCount: "0x3e8", // 1000 per page
    };
    if (pageKey) reqParams.pageKey = pageKey;

    const result = await provider.send("alchemy_getAssetTransfers", [reqParams]);
    all.push(...(result.transfers ?? []));
    pageKey = result.pageKey;
    if (pageKey) console.log(`  Paginating... ${all.length} transfers so far`);
  } while (pageKey);

  return all;
}

// ═══════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════

async function main() {
  const provider = new ethers.JsonRpcProvider(ETH_RPC_URL);

  console.log(`Backfilling from block ${START_BLOCK} (Mar-26-2026 06:14:35 UTC) to latest...`);

  // Fetch both in parallel
  const [yieldTransfers, topupTransfers] = await Promise.all([
    // Check 1: USDO/USDC weekly yield — Monarq → OE wallet
    fetchAllTransfers(provider, {
      fromBlock: START_BLOCK,
      toBlock: "latest",
      fromAddress: MONARQ_SENDER,
      toAddress: OE_WALLET,
      category: ["erc20"],
    }),
    // Check 2: PRISM daily drip — OE wallet → xPRISM contract
    fetchAllTransfers(provider, {
      fromBlock: START_BLOCK,
      toBlock: "latest",
      fromAddress: OE_WALLET,
      toAddress: XPRISM_CONTRACT,
      contractAddresses: [PRISM_TOKEN],
      category: ["erc20"],
    }),
  ]);

  console.log(`Found ${yieldTransfers.length} weekly yield transfers`);
  console.log(`Found ${topupTransfers.length} daily topup transfers`);

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

  if (records.length === 0) {
    console.log("No records to insert.");
    return;
  }

  // Upsert into MongoDB (hash is the unique key — safe to re-run)
  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  try {
    const col = client.db(MONGODB_DB).collection(COLLECTION);

    // Indexes
    await col.createIndex({ hash: 1 }, { unique: true });
    await col.createIndex({ type: 1, dateKey: 1 });
    await col.createIndex({ timestamp: 1 });

    let inserted = 0;
    let skipped = 0;

    for (const rec of records) {
      const result = await col.updateOne(
        { hash: rec.hash },
        { $setOnInsert: rec },
        { upsert: true }
      );
      if (result.upsertedCount > 0) inserted++;
      else skipped++;
    }

    console.log(`\nDone. Inserted: ${inserted}  |  Already existed (skipped): ${skipped}`);

    // Summary
    const topups = records.filter((r) => r.type === "daily_topup");
    const yields = records.filter((r) => r.type === "weekly_yield");

    if (yields.length > 0) {
      console.log(`\nWeekly yield receipts (${yields.length}):`);
      for (const y of yields.sort((a, b) => b.blockNumber - a.blockNumber)) {
        console.log(`  ${y.dateKey}  ${y.amountToken} ${y.asset}  ${y.hash}`);
      }
    }

    if (topups.length > 0) {
      const totalPRISM = topups.reduce((s, t) => s + t.amountToken, 0);
      console.log(`\nTopup drips (${topups.length}) — total: ${totalPRISM.toFixed(6)} PRISM`);
      for (const t of topups.sort((a, b) => b.blockNumber - a.blockNumber).slice(0, 20)) {
        console.log(`  ${t.dateKey}  ${t.amountToken} PRISM  ${t.hash}`);
      }
      if (topups.length > 20) console.log(`  ... and ${topups.length - 20} more`);
    }
  } finally {
    await client.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
