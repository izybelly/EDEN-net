import "dotenv/config";
import { MongoClient } from "mongodb";

// ═══════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════

const DUNE_API_KEY = process.env.DUNE_API_KEY!;
const DUNE_QUERY_ID = 6953622; // PRISMExpress - Daily Redemption Requests (4pm SGT cutoff)

const MONGODB_URI = process.env.MONGODB_CONNECTION_STRING!;
const MONGODB_DB = "prod";
const MONGODB_COLLECTION = "prism_redemption_metrics";

// ─── Helpers ─────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** Returns YYYY-MM-DD for today in UTC. */
function todayDateKey(): string {
  return new Date().toISOString().slice(0, 10);
}

// ─── Dune polling ────────────────────────────────────────────────

interface DuneRow {
  settlement_date: string; // "2026-04-05"
  num_requests: number;
  total_redemption_usdo: number;
  tx_hashes: string[];
}

async function executeDuneQuery(): Promise<string> {
  const res = await fetch(
    `https://api.dune.com/api/v1/query/${DUNE_QUERY_ID}/execute`,
    {
      method: "POST",
      headers: {
        "X-Dune-API-Key": DUNE_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ performance: "medium" }),
    }
  );
  const json = (await res.json()) as { execution_id?: string };
  if (!json.execution_id)
    throw new Error(`Dune execute failed: ${JSON.stringify(json)}`);
  return json.execution_id;
}

async function pollDuneResults(executionId: string): Promise<DuneRow[]> {
  for (let i = 0; i < 60; i++) {
    await sleep(5000);

    const res = await fetch(
      `https://api.dune.com/api/v1/execution/${executionId}/results`,
      { headers: { "X-Dune-API-Key": DUNE_API_KEY } }
    );
    const json = (await res.json()) as any;

    if (json.state === "QUERY_STATE_COMPLETED") {
      return json.result.rows as DuneRow[];
    }
    if (json.state === "QUERY_STATE_FAILED") {
      throw new Error(`Dune query failed: ${JSON.stringify(json)}`);
    }

    console.log(`[dune] Waiting… (${i + 1}/60) state=${json.state}`);
  }
  throw new Error("Timed out waiting for Dune results");
}

// ─── Main ────────────────────────────────────────────────────────

async function main() {
  const targetDate = todayDateKey(); // e.g. "2026-04-06"
  console.log(`[prism-redemptions] Fetching redemption requests for settlement_date=${targetDate}`);

  const executionId = await executeDuneQuery();
  console.log(`[dune] execution_id=${executionId}`);

  const rows = await pollDuneResults(executionId);
  console.log(`[dune] Query returned ${rows.length} rows`);

  // Filter to yesterday's settlement date
  const row = rows.find((r) => {
    // Dune may return dates as "2026-04-05 00:00:00.000 UTC" or "2026-04-05"
    return r.settlement_date.slice(0, 10) === targetDate;
  });

  if (!row) {
    console.warn(`[prism-redemptions] No redemption data found for ${targetDate} — inserting zero-row.`);
  }

  const snapshotId = new Date().toISOString();

  const doc = {
    snapshotId,
    dateKey: targetDate,
    settlementDate: targetDate,
    numRequests: row ? Number(row.num_requests) : 0,
    totalRedemptionUSDO: row ? Number(row.total_redemption_usdo) : 0,
    txHashes: row?.tx_hashes ?? [],
    fetchedAt: new Date(),
  };

  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  try {
    const col = client.db(MONGODB_DB).collection(MONGODB_COLLECTION);

    // Upsert: one doc per dateKey (idempotent re-runs)
    const result = await col.updateOne(
      { dateKey: targetDate },
      { $set: doc },
      { upsert: true }
    );

    await col.createIndex({ dateKey: 1 }, { unique: true });

    const action = result.upsertedCount > 0 ? "Inserted" : "Updated";
    console.log(
      `[${MONGODB_COLLECTION}] ${action} for ${targetDate}: ` +
        `numRequests=${doc.numRequests}, totalRedemptionUSDO=${doc.totalRedemptionUSDO}`
    );
  } finally {
    await client.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
