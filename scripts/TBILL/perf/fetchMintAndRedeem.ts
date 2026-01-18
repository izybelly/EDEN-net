import "dotenv/config";
import { MongoClient } from "mongodb";

const DUNE_API_KEY = process.env.DUNE_API_KEY!;
const DUNE_QUERY_ID = 6517050;
const MONGODB_URI = process.env.MONGODB_CONNECTION_STRING!;

const MONGODB_DB = "prod";
const MONGODB_COLLECTION = "tbill_mint_redeem";

function isoNow(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchDuneRows(endDate: string, startDate: string) {
  const execRes = await fetch(
    `https://api.dune.com/api/v1/query/${DUNE_QUERY_ID}/execute`,
    {
      method: "POST",
      headers: {
        "X-Dune-API-Key": DUNE_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query_parameters: {
          start_date: startDate,
          end_date: endDate,
        },
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
        day: string;
        minted: number;
        net: number;
        redeemed: number;
      }>;
    }
    if (j.state === "QUERY_STATE_FAILED") {
      throw new Error(`Dune failed: ${JSON.stringify(j)}`);
    }
  }

  throw new Error("Timed out waiting for Dune results");
}

async function main() {
  const nowIso = isoNow();
  const nowDate = new Date(nowIso);
  const oneDayAgoDate = new Date(nowDate.getTime() - 86400000);

  const endDate = nowIso;
  const startDate = oneDayAgoDate.toISOString().replace(/\.\d{3}Z$/, "Z");

  const duneRows = await fetchDuneRows(endDate, startDate);

  const duneDocs = duneRows.map((row) => ({
    network: row.chain,
    date: row.day, 
    minted: Number(row.minted || 0),
    redeemed: Number(row.redeemed || 0),
    net: Number(row.net || 0),
  }));

  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  try {
    const col = client.db(MONGODB_DB).collection(MONGODB_COLLECTION);
    await col.insertMany(duneDocs);
    console.log(
      `Inserted ${duneDocs.length} docs into ${MONGODB_DB}.${MONGODB_COLLECTION}`
    );
  } finally {
    await client.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
