import "dotenv/config";
import { MongoClient } from "mongodb";

const MONGODB_URI = process.env.MONGODB_CONNECTION_STRING!;
const MONGODB_DB = "prod";
const MONGODB_COLLECTION = "usdo_collateral_asset_allocation";

interface AssetAllocation {
  chain: string;
  asset: string;
  usdValue: number;
  percentage: number;
}

interface Wallet {
  usdcAmount: number;
  tbillAmountInUsd: number;
  buidlAmount: number;
  vbillAmount: number;
  usycAmount: number;
  usycAmountInUsd: number;
  benjiAmount: number;
}

async function fetchAssetAllocation() {
  const execRes = await fetch(
    "https://prod-gw.openeden.com/sys/reserve-composition-live",
    { method: "GET" }
  );
  if (!execRes.ok) throw new Error(`Failed: ${execRes.status}`);

  const data = await execRes.json();

  const totalAssetsUsd = data.reserveAssetsInUsd;
  const collateralRatio = data.ratio;

  const allocations: AssetAllocation[] = [];

  for (const chainInfo of data.chainReserveInfo) {
    const chain = chainInfo.chainType;

    const assets: Record<string, number> = {
      TBILL: chainInfo.totalTbillAmountInUsd ?? 0,
      USDC: chainInfo.usdcAmount ?? 0,
      BUIDL: chainInfo.buidlAmount ?? 0,
      VBILL: chainInfo.vbillAmount ?? 0,
      USYC: chainInfo.usycAmountInUsd ?? 0,
      BENJI: chainInfo.benjiAmount ?? 0,
    };

    Object.entries(assets).forEach(([asset, usdValue]) => {
      if (usdValue > 1) {
        allocations.push({
          chain,
          asset,
          usdValue,
          percentage: (usdValue / totalAssetsUsd) * 100,
        });
      }
    });
  }

  allocations.sort((a, b) => b.usdValue - a.usdValue);

  return {
    date: data.date,
    totalTbillUsd: data.totalTbillAmountInUsd,
    totalReserveUsd: totalAssetsUsd,
    collateralRatio,
    allocations,
  };
}

async function main() {
  const assetAllocation = await fetchAssetAllocation();

  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  try {
    const col = client.db(MONGODB_DB).collection(MONGODB_COLLECTION);

    const doc = {
      date: assetAllocation.date,
      totalReserveUsd: assetAllocation.totalReserveUsd,
      allocations: assetAllocation.allocations,
      collateralRatio: assetAllocation.collateralRatio,
    };

    const result = await col.replaceOne(
      { date: assetAllocation.date }, 
      doc,                            
      { upsert: true }                
    );
    console.log(`Upserted: ${result.upsertedCount} / Matched: ${result.matchedCount}`);
    
  } finally {
    await client.close();
  }
}

main().catch(console.error);
