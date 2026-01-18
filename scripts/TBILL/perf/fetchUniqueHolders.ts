import "dotenv/config";
import { MongoClient } from "mongodb";
import { time } from "node:console";

const DUNE_API_KEY = process.env.DUNE_API_KEY!;
const DUNE_QUERY_ID = 6517120;
const MONGODB_URI = process.env.MONGODB_CONNECTION_STRING!;

const MONGODB_DB = "prod";
const MONGODB_COLLECTION = "tbill_unique_holders";

function isoNow(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchDuneRows(snapshot_date: string) {
  const execRes = await fetch(
    `https://api.dune.com/api/v1/query/${DUNE_QUERY_ID}/execute`,
    {
      method: "POST",
      headers: {
        "X-Dune-API-Key": DUNE_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query_parameters: { snapshot_date },
        performance: "medium",
      }),
    }
  );

  const execJson = (await execRes.json()) as { execution_id?: string };
  const executionId = execJson.execution_id;
  if (!executionId)
    throw new Error(`No execution_id: ${JSON.stringify(execJson)}`);

  for (let i = 0; i < 360; i++) {
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
        datetime: string;
        unique_holders: number;
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

  const duneRows = await fetchDuneRows(timestamp);


  const duneDocs = duneRows.map((row) => ({
    network: row.chain,
    date: row.datetime,
    unique_holders: Number(row.unique_holders || 0),
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
