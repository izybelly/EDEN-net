import "dotenv/config";
import { MongoClient } from "mongodb";

const DUNE_API_KEY = process.env.DUNE_API_KEY!;
const DUNE_QUERY_ID = 6493568;
const XRPL_RPC_URL = process.env.XRPL_RPC_URL!;
const MONGODB_URI = process.env.MONGODB_CONNECTION_STRING!;

const MONGODB_DB = "prod";
const MONGODB_COLLECTION = "tbill_tvl";

function isoNow(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function parseDuneTimestampUtc(s: string): Date {
  return new Date(s.replace(" ", "T") + "Z");
}

async function fetchXrplTvl(): Promise<number> {
  const payload = {
    jsonrpc: "2.0",
    id: 1,
    method: "gateway_balances",
    params: [
      {
        account: "rJNE2NNz83GJYtWVLwMvchDWEon3huWnFn",
        strict: true,
        hotwallet: ["rB56JZWRKvpWNeyqM3QYfZwW4fS9YEyPWM"],
        ledger_index: "validated",
      },
    ],
  };

  const res = await fetch(XRPL_RPC_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const j = (await res.json()) as any;
  const tvl = Number(j?.result?.obligations?.TBL ?? 0);
  return tvl;
}

async function fetchDuneRows(timestamp: string) {
  const execRes = await fetch(
    `https://api.dune.com/api/v1/query/${DUNE_QUERY_ID}/execute`,
    {
      method: "POST",
      headers: {
        "X-Dune-API-Key": DUNE_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query_parameters: { timestamp },
        performance: "medium",
      }),
    }
  );

  const execJson = (await execRes.json()) as { execution_id?: string };
  const executionId = execJson.execution_id;
  if (!executionId)
    throw new Error(`No execution_id: ${JSON.stringify(execJson)}`);

  for (let i = 0; i < 60; i++) {
    await sleep(5000);

    const r = await fetch(
      `https://api.dune.com/api/v1/execution/${executionId}/results`,
      {
        headers: { "X-Dune-API-Key": DUNE_API_KEY },
      }
    );

    const j = (await r.json()) as any;

    if (j.state === "QUERY_STATE_COMPLETED") {
      return j.result.rows as Array<{
        chain: string;
        query_timestamp: string;
        tvl: number;
      }>;
    }
    if (j.state === "QUERY_STATE_FAILED") {
      throw new Error(`Dune failed: ${JSON.stringify(j)}`);
    }
  }

  throw new Error("Timed out waiting for Dune results");
}

async function main() {
  const timestamp = isoNow();

  const xrplTvl = await fetchXrplTvl();

  const duneRows = await fetchDuneRows(timestamp);

  const duneDocs = duneRows.map((row) => ({
    network: row.chain,
    tvl: Number(row.tvl),
    datetime: parseDuneTimestampUtc(row.query_timestamp), // BSON Date
  }));

  const xrplDoc = {
    network: "xrpl",
    tvl: xrplTvl,
    datetime: new Date(timestamp), // BSON Date (UTC)
  };

  const docs = [...duneDocs, xrplDoc];

  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  try {
    const col = client.db(MONGODB_DB).collection(MONGODB_COLLECTION);
    await col.insertMany(docs);
    console.log(
      `Inserted ${docs.length} docs into ${MONGODB_DB}.${MONGODB_COLLECTION}`
    );
  } finally {
    await client.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
