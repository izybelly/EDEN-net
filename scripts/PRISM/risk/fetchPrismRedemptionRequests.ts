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

/**
 * Computes the settlement window timestamps for the current day's batch.
 *
 * 4pm SGT = 08:00 UTC
 *   window opens:  yesterday at 08:01 UTC
 *   window closes: today     at 08:00 UTC
 *
 * Returns strings in "YYYY-MM-DD HH:MM:SS" format expected by Dune date params.
 */
function settlementWindow(): { startTime: string; endTime: string } {
  const now = new Date();

  // today 08:00 UTC
  const end = new Date(now);
  end.setUTCHours(8, 0, 0, 0);

  // yesterday 08:01 UTC
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - 1);
  start.setUTCMinutes(1);

  const fmt = (d: Date) => d.toISOString().replace("T", " ").slice(0, 19);

  return { startTime: fmt(start), endTime: fmt(end) };
}

// ─── Dune ────────────────────────────────────────────────────────

interface DuneRow {
  num_requests: number;
  total_redemption_prism: number;
  tx_hashes: string[];
}

async function executeDuneQuery(startTime: string, endTime: string): Promise<string> {
  const res = await fetch(
    `https://api.dune.com/api/v1/query/${DUNE_QUERY_ID}/execute`,
    {
      method: "POST",
      headers: {
        "X-Dune-API-Key": DUNE_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query_parameters: {
          start_time: startTime,
          end_time: endTime,
        },
        performance: "medium",
      }),
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
  const dateKey = todayDateKey();
  const { startTime, endTime } = settlementWindow();

  console.log(`[prism-redemptions] Settlement window: ${startTime} → ${endTime} UTC`);
  console.log(`[prism-redemptions] dateKey=${dateKey}`);

  const executionId = await executeDuneQuery(startTime, endTime);
  console.log(`[dune] execution_id=${executionId}`);

  const rows = await pollDuneResults(executionId);
  console.log(`[dune] Query returned ${rows.length} row(s)`);

  // Query returns a single aggregate row; if no events it returns an empty result
  const row = rows[0] ?? null;

  if (!row || row.num_requests === 0) {
    console.warn(`[prism-redemptions] No redemption events in window — storing zero-row.`);
  }

  const snapshotId = new Date().toISOString();

  const doc = {
    snapshotId,
    dateKey,
    windowStart: startTime,
    windowEnd: endTime,
    numRequests: row ? Number(row.num_requests) : 0,
    totalRedemptionPRISM: row ? Number(row.total_redemption_prism) : 0,
    txHashes: row?.tx_hashes ?? [],
    fetchedAt: new Date(),
  };

  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  try {
    const col = client.db(MONGODB_DB).collection(MONGODB_COLLECTION);

    // Upsert: one doc per dateKey (idempotent re-runs)
    const result = await col.updateOne(
      { dateKey },
      { $set: doc },
      { upsert: true }
    );

    await col.createIndex({ dateKey: 1 }, { unique: true });

    const action = result.upsertedCount > 0 ? "Inserted" : "Updated";
    console.log(
      `[${MONGODB_COLLECTION}] ${action} for ${dateKey}: ` +
        `numRequests=${doc.numRequests}, totalRedemptionPRISM=${doc.totalRedemptionPRISM}`
    );
  } finally {
    await client.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
