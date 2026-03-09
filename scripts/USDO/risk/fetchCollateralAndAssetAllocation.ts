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

interface CirculatingSupply {
  chain: string;
  usdoAmount: number;
  percentage: number;
}

interface SettlementRatios {
  tPlus1: number;
  tPlus2: number;
  tPlus3: number;
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
  const totalUsdoAmount = data.usdoAmount;

  const allocations: AssetAllocation[] = [];
  const circulatingSupplyByNetwork: CirculatingSupply[] = [];

  // Aggregate total amounts across all chains
  let totalTbill = 0;
  let totalUsdc = 0;
  let totalBuidl = 0;
  let totalVbill = 0;
  let totalUsyc = 0;
  let totalBenji = 0;

  for (const chainInfo of data.chainReserveInfo) {
    const chain = chainInfo.chainType;

    // Collect circulating supply per network
    if (chainInfo.usdoAmount > 0) {
      circulatingSupplyByNetwork.push({
        chain,
        usdoAmount: chainInfo.usdoAmount,
        percentage: (chainInfo.usdoAmount / totalUsdoAmount) * 100,
      });
    }

    // Aggregate asset totals
    totalTbill += chainInfo.totalTbillAmountInUsd ?? 0;
    totalUsdc += chainInfo.usdcAmount ?? 0;
    totalBuidl += chainInfo.buidlAmount ?? 0;
    totalVbill += chainInfo.vbillAmount ?? 0;
    totalUsyc += chainInfo.usycAmountInUsd ?? 0;
    totalBenji += chainInfo.benjiAmount ?? 0;

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
  circulatingSupplyByNetwork.sort((a, b) => b.usdoAmount - a.usdoAmount);

  // Calculate settlement ratios
  // Note: Assuming USD (BitGo) is included in totalUsdc or should be fetched separately
  // If BitGo USD is separate, you'll need to add it to the API response
  const tPlus1Assets = totalTbill + totalBuidl + totalVbill + totalUsyc + totalUsdc;
  const tPlus2Assets = tPlus1Assets + totalBenji;
  const tPlus3Assets = tPlus2Assets; // Same as T+2

  const settlementRatios: SettlementRatios = {
    tPlus1: (tPlus1Assets / totalAssetsUsd) * 100,
    tPlus2: (tPlus2Assets / totalAssetsUsd) * 100,
    tPlus3: (tPlus3Assets / totalAssetsUsd) * 100,
  };

  return {
    date: data.date,
    totalUsdoAmount,
    totalTbillUsd: data.totalTbillAmountInUsd,
    totalReserveUsd: totalAssetsUsd,
    collateralRatio,
    allocations,
    circulatingSupplyByNetwork,
    settlementRatios,
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
      totalUsdoAmount: assetAllocation.totalUsdoAmount,
      totalReserveUsd: assetAllocation.totalReserveUsd,
      collateralRatio: assetAllocation.collateralRatio,
      allocations: assetAllocation.allocations,
      circulatingSupplyByNetwork: assetAllocation.circulatingSupplyByNetwork,
      settlementRatios: assetAllocation.settlementRatios,
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
