import "dotenv/config";
import { MongoClient } from "mongodb";
import { ethers } from "ethers";

// ═══════════════════════════════════════════════════════════════════
// CONFIGURATION — edit these when strategies/venues/assets change
// ═══════════════════════════════════════════════════════════════════

const HARUKO_BASE_URL = "https://sgp10.haruko.io/cefi";
const HARUKO_BEARER_TOKEN = process.env.HARUKO_BEARER_TOKEN!;

// PRISM reserve composition API (authoritative source for AUM, supply, ratio, APY, strategies)
const PRISM_RESERVE_API = "https://beta-gw.openeden.com/prism/sys/reserve-composition-live";

const MONGODB_URI = process.env.MONGODB_CONNECTION_STRING!;
const MONGODB_DB = "prod";
const MONGODB_COLLECTION = "prism_risk_metrics";

// On-chain contracts (Ethereum mainnet) — used as fallback if PRISM API is unavailable
const ETH_RPC_URL = process.env.ETH_RPC_URL!;
const PRISM_ADDRESS = "0x06Bb4ab600b7D22eB2c312f9bAbC22Be6a619046";
const XPRISM_ADDRESS = "0x12E04c932D682a2999b4582F7c9B86171B73220D";

// Maple Finance — Secured Institutional Lending pool (Ethereum mainnet)
const MAPLE_GRAPHQL_URL  = "https://api.maple.finance/v2/graphql";
const MAPLE_POOL_ADDRESS = "0xc39a5a616f0ad1ff45077fa2de3f79ab8eb8b8b9"; // lowercase for GraphQL id

// Morpho Blue — FalconX Pareto position (Ethereum mainnet)
const MORPHO_BLUE_ADDRESS  = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb";
const MORPHO_PARETO_MARKET = "0xe83d72fa5b00dcd46d9e0e860d95aa540d5ec106da5833108a9f826f21f36f52";
const PRISM_PARETO_WALLET  = "0x8a602f71cb72663fb0e4019b3b2d59d2944a4981";

// Chainlink price feeds (Ethereum mainnet) — used for Maple collateral pricing
const CHAINLINK_ETH_USD = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419";
const CHAINLINK_BTC_USD = "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88b";

// Minimal ABIs for on-chain reads
const ERC20_ABI = [
  "function totalSupply() view returns (uint256)",
  "function decimals() view returns (uint8)",
];

const ERC4626_ABI = [
  "function totalSupply() view returns (uint256)",
  "function decimals() view returns (uint8)",
  "function convertToAssets(uint256 shares) view returns (uint256)",
];

const MORPHO_BLUE_ABI = [
  "function market(bytes32 id) view returns (uint128 totalSupplyAssets, uint128 totalSupplyShares, uint128 totalBorrowAssets, uint128 totalBorrowShares, uint128 lastUpdate, uint128 fee)",
  "function position(bytes32 id, address user) view returns (uint256 supplyShares, uint128 borrowShares, uint128 collateral)",
];

const CHAINLINK_ABI = [
  "function latestRoundData() view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)",
  "function decimals() view returns (uint8)",
];

// Haruko group that contains ALL venue accounts for PRISM (used for total AUM)
const HARUKO_VAULT_GROUP = "Open Eden Vault";

// Strategy groups — each maps to a Haruko "group" inside the vault
const STRATEGY_GROUPS = [
  { groupId: "Prism Cash & Carry", name: "Prism Cash & Carry" },
  { groupId: "Prism Monarq Operations", name: "Prism Monarq Operations" },
  { groupId: "Prism DEFI", name: "Prism DEFI" },
  { groupId: "Prism Overcollaterized Lending", name: "Prism Overcollateralized Lending" },
];

// Strategy bucket → venue account names (for mapping venue IDs dynamically)
const STRATEGY_BUCKET_ACCOUNTS: Record<string, string[]> = {
  "Prism Cash & Carry": [
    "open_eden_deribit_falconx",
    "open_eden_bybit_falconx",
    "open_eden_okx_falconx",
    "open_eden_binance_falconx",
    "Prism Lighter",
    "BitGo Prism Lighter",
    "open_eden_falconx",
    "Prism Lighter Staked LIT",
  ],
  "Prism Monarq Operations": [
    "Prism Yield wallet",
    "BitGo PRISM Funding Wallet",
    "BitGo Monarq Ops Wallet",
    "BitGo Tokenized US Treasuries",
  ],
  "Prism DEFI": [
    "PRISM Monarq Operations (Centrifuge) ETHEREUM",
    "PRISM Monarq Sentora PYUSD Vault ETHEREUM",
    "Open_Eden_August_Monarq_1 ETHEREUM",
    "PRISM Monarq Operations (Centrifuge) JAAA Position",
    "PRISM Monarq Operations (Neutrl) Position",
    "PRISM Monarq Operations (Neutrl) ETHEREUM",
    "PRISM Monarq Steakhouse AUSD Position",
    "PRISM Monarq Steakhouse AUSD ETHEREUM",
  ],
  "Prism Overcollaterized Lending": [
    "Maple Secured Inst Lending pool ETHEREUM",
    "Falconx Pareto Position",
    "FalconX Pareto ETHEREUM",
  ],
};

// DeFi wallet addresses (from Haruko venue accounts — used for /api/defi/wallet_deployments)
const DEFI_WALLET_ADDRESSES: string[] = [
  "0x9f78d300b9b8804107930a40b09f73e7b0f85dcc",  // Maple Secured Inst Lending pool (Overcollaterized Lending)
  "0x8AC314d779E892c0F6818456F9E569b43d151Ed4",  // PRISM Monarq Operations (Centrifuge) (DEFI)
  "0x7d2E7131ba6885dc8064cE56977Ce618262F88A5",  // PRISM Monarq Sentora PYUSD Vault (DEFI)
  "0xD8F22956fE60bBc0BBF7B2d5194DEab7D921734A",  // Open_Eden_August_Monarq_1 (DEFI)
];

// Asset classification — "core" includes major crypto + high-quality stablecoins + treasuries
const CORE_ASSETS = [
  "BTC", "ETH", "SOL", "BNB", "XRP", "AVAX",          // blue-chip crypto
  "USDC", "USDT", "DAI", "BUSD", "TUSD", "USDP",      // established stablecoins
  "FDUSD", "PYUSD", "GUSD",                             // regulated stablecoins
  "LIT",                                                 // Lighter (staked position in C&C)
];

const STABLECOINS = [
  "USDC", "USDT", "DAI", "BUSD", "TUSD", "USDP", "FRAX", "LUSD",
  "GUSD", "FDUSD", "PYUSD", "USDD", "CRVUSD",
];

const NOVEL_STABLECOINS = [
  "USDE", "EUSD", "GHO", "MKUSD", "ULTRA",
  // TODO: Add novel/newer stablecoins as they appear
];

const LIQUID_ASSETS = [
  "BTC", "ETH", "SOL", "USDC", "USDT", "DAI", "BUSD", "FDUSD",
  "PYUSD", "BNB", "XRP", "AVAX", "DOGE", "ADA", "LINK", "DOT",
];

// Yield transfer filtering
const YIELD_TRANSFER_COUNTERPARTY = ""; // TODO: set counterparty name
const YIELD_TRANSFER_ASSET = "USDC";

// Funding rate adjustment types
const FUNDING_TYPES = ["FUNDING", "FUNDING_FEE"];

// ═══════════════════════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════════════════════

interface CapitalAllocationEntry {
  groupId: string;
  groupName: string;
  totalEquityUSD: number;
  percentage: number;
}

interface GrossLeverageStrategyEntry {
  strategy: string;
  spotUSD: number;
  futuresAbsNotionalUSD: number;
  grossExposureUSD: number;
  strategyEquityUSD: number;
  grossLeverage: number;
}

interface GrossLeverageDeploymentEntry {
  venue: string;
  strategy: string;
  spotEquityUSD: number;
  futuresAbsNotionalUSD: number;
  grossExposureUSD: number;
  pctOfAUM: number;
}

interface CounterpartyExposureEntry {
  venue: string;
  totalExposureUSD: number;
  percentage: number;
}

interface StablecoinHolding {
  asset: string;
  balanceUSD: number;
  percentage: number;
}

interface NonCoreAllocationDetail {
  asset: string;
  balanceUSD: number;
  isCore: boolean;
}

interface CexMarginEntry {
  venue: string;
  asset: string;
  symbol: string;
  side: string;
  notionalUSD: number;
  marginMode: string;
  isCross: boolean;
}

interface BasisEntry {
  asset: string;
  venue: string;
  spotPrice: number;
  futuresPrice: number;
  maturityTs: number;
  notionalUSD: number;
  annualizedBasisPct: number;
}

interface LoanEntry {
  borrowValue: number;
  collateralValue: number;
  ltv: number;
  maturityTs: number;
}

interface MapleLTVData {
  totalBorrowUSD: number;    // sum of outstanding principal across all pool loans
  totalCollateralUSD: number; // sum of BTC/ETH collateral USD value across all pool loans
  loanCount: number;
}

interface ParetoMorphoData {
  borrowUSD: number;     // USDC borrowed by PRISM on Morpho against AA tokens
  collateralUSD: number; // value of AA_FALCONXUSDC tokens deposited as Morpho collateral
}

interface DefiDeploymentEntry {
  protocol: string;
  suppliedUSD: number;
  borrowedUSD: number;
  ltv: number;
}

interface StrategyPnlEntry {
  strategyId: string;
  strategyName: string;
  pnl: number;
  timestamp: number;
}

interface YieldTransferEntry {
  counterparty: string;
  asset: string;
  amount: number;
  timestamp: number;
  type: string;
}

interface FundingRateEntry {
  asset: string;
  totalFunding: number;
  count: number;
}

// ═══════════════════════════════════════════════════════════════════
// API UTILITIES
// ═══════════════════════════════════════════════════════════════════

async function harukoFetch(path: string, params?: Record<string, string>): Promise<any> {
  const url = new URL(`${HARUKO_BASE_URL}${path}`);
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  }

  const res = await fetch(url.toString(), {
    method: "GET",
    headers: { Authorization: `Bearer ${HARUKO_BEARER_TOKEN}` },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Haruko ${path} failed: ${res.status} ${text}`);
  }

  const json = await res.json();
  return json.result ?? json;
}

async function safeFetch<T>(
  label: string,
  fn: () => Promise<T>
): Promise<{ data: T | null; error: string | null }> {
  try {
    const data = await fn();
    return { data, error: null };
  } catch (e: any) {
    console.error(`[${label}] ${e.message}`);
    return { data: null, error: `${label}: ${e.message}` };
  }
}

// ═══════════════════════════════════════════════════════════════════
// DATA FETCHING (one function per Haruko endpoint)
// ═══════════════════════════════════════════════════════════════════

// Fetch venue accounts from /api/venues and build name→id map
interface VenueAccount {
  id: number;
  name: string;
  venue: string;
  venueType: string;
  groupName: string;
}

async function fetchVenueAccounts(): Promise<Record<string, VenueAccount>> {
  const data = await harukoFetch("/api/venues");
  const result: Record<string, VenueAccount> = {};
  for (const group of data.groups ?? []) {
    for (const acct of group.venues ?? []) {
      result[acct.name] = {
        id: acct.id,
        name: acct.name,
        venue: acct.venue,
        venueType: acct.venueType,
        groupName: group.group,
      };
    }
  }
  return result;
}

// Get venue account IDs for Cash & Carry from dynamic lookup
function getCashCarryAccountIds(venueAccounts: Record<string, VenueAccount>): number[] {
  const ccAccountNames = STRATEGY_BUCKET_ACCOUNTS["Prism Cash & Carry"] ?? [];
  const ids: number[] = [];
  for (const name of ccAccountNames) {
    if (venueAccounts[name]) {
      ids.push(venueAccounts[name].id);
    }
  }
  return ids;
}

// Get all CeFi venue account IDs across all strategies (for transfers / balance_adjustments)
function getAllCefiAccountIds(venueAccounts: Record<string, VenueAccount>): number[] {
  const ids: number[] = [];
  for (const acct of Object.values(venueAccounts)) {
    // Only include CeFi and Hybrid accounts (not DeFi — those have IDs >= 1000000000)
    if (acct.venueType !== "DeFi" && acct.id < 1000000000) {
      ids.push(acct.id);
    }
  }
  return ids;
}

// Fetch total AUM from group_summary_curve (latest totalEquityUsd)
async function fetchGroupSummaryCurveAUM(): Promise<{ totalAUM: number; timeSeries: any[] }> {
  const data = await harukoFetch("/api/group_summary_curve", {
    group: HARUKO_VAULT_GROUP,
    includeAccountBreakdown: "false",
    includeEquity: "true",
    includePositions: "false",
    refreshLiveBalances: "false",
    notionalType: "DEFAULT",
  });

  const timeSeries: any[] = data.timeSeries ?? [];
  if (timeSeries.length === 0) {
    return { totalAUM: 0, timeSeries };
  }

  // Latest data point
  const latest = timeSeries[timeSeries.length - 1];
  const totalAUM: number = latest.equitySummary?.totalEquityUsd ?? 0;
  return { totalAUM, timeSeries };
}

// Fetch per-strategy summary from /api/summary (one call per strategy group)
async function fetchStrategySummaries(): Promise<Record<string, any>> {
  const results: Record<string, any> = {};
  const fetches = STRATEGY_GROUPS.map(async (g) => {
    try {
      const data = await harukoFetch("/api/summary", { group: g.groupId });
      results[g.groupId] = data;
    } catch (e: any) {
      console.warn(`[summary/${g.groupId}] ${e.message}`);
      results[g.groupId] = null;
    }
  });
  await Promise.all(fetches);
  return results;
}

// Fetch positions: try /api/aggregate/position first; if empty, fall back to
// individual /api/position calls per account (which reliably returns data).
async function fetchAggregatePositions(venueAccountIds: number[]): Promise<any> {
  if (venueAccountIds.length === 0) return null;

  // Try aggregate endpoint first
  try {
    const ids = venueAccountIds.join(",");
    const data = await harukoFetch("/api/aggregate/position", { venueAccountIds: ids });
    const futures = data.futuresPositions ?? [];
    if (futures.length > 0) return data;
  } catch {
    // fall through to individual calls
  }

  // Fallback: fetch per-account and merge
  const allFutures: any[] = [];
  const allBalances: any[] = [];
  await Promise.all(
    venueAccountIds.map(async (id) => {
      try {
        const data = await harukoFetch("/api/position", { venueAccountId: String(id) });
        allFutures.push(...(data.futuresPositions ?? []));
        allBalances.push(...(data.balances ?? []));
      } catch {
        // skip accounts that fail
      }
    })
  );

  return { futuresPositions: allFutures, balances: allBalances };
}

async function fetchLoans(): Promise<any> {
  return harukoFetch("/api/loans");
}

// Get DeFi venue account IDs from STRATEGY_BUCKET_ACCOUNTS
function getDefiAccountIds(venueAccounts: Record<string, VenueAccount>): number[] {
  const defiAccountNames = [
    ...(STRATEGY_BUCKET_ACCOUNTS["Prism DEFI"] ?? []),
    ...(STRATEGY_BUCKET_ACCOUNTS["Prism Overcollaterized Lending"] ?? []),
  ];
  const ids: number[] = [];
  for (const name of defiAccountNames) {
    if (venueAccounts[name]) {
      ids.push(venueAccounts[name].id);
    }
  }
  return ids;
}

async function fetchDefiDeployments(_venueAccounts: Record<string, VenueAccount>): Promise<any> {
  // Use /api/summary with group: "Prism DEFI".
  // This reliably returns all 8 DeFi sub-accounts including:
  //   - Hybrid-type "Position" accounts (JAAA, Neutrl, Steakhouse) which have populated
  //     balances[].equityUsd and balances[].borrowed but empty walletInventory
  //   - DeFi-type "ETHEREUM" wallet accounts which have populated walletInventory (ETH, PYUSD, etc.)
  //     but no positions/balances
  // Previous endpoints (/api/summary/accounts, /api/defi/wallet_deployments) were broken
  // (empty results or HTTP 500).
  try {
    const data = await harukoFetch("/api/summary", { group: "Prism DEFI" });
    const venues: any[] = data.result?.venues ?? [];
    const deployments: any[] = [];

    for (const venue of venues) {
      const protocol: string = venue.venueAccount ?? venue.venue ?? "unknown";
      let suppliedUSD = 0;
      let borrowedUSD = 0;

      // Hybrid/Position accounts: balances[] contains equityUsd and borrowed per asset
      for (const b of (venue.balances ?? []) as any[]) {
        const equityUsd: number = b.equityUsd ?? (b.equity ?? 0) * (b.refPx ?? 1);
        const borrowedUsd: number = (b.borrowed ?? 0) * (b.refPx ?? 1);
        if (equityUsd > 0) suppliedUSD += equityUsd;
        if (borrowedUsd > 0) borrowedUSD += borrowedUsd;
      }

      // DeFi wallet accounts: walletInventory[] contains on-chain token balances
      for (const t of (venue.walletInventory ?? []) as any[]) {
        const val: number = (t.equity ?? 0) * (t.refPx ?? 0);
        if (val > 0) suppliedUSD += val;
      }

      if (suppliedUSD > 0.01 || borrowedUSD > 0.01) {
        deployments.push({ protocol, suppliedUSD, borrowedUSD, equityUSD: suppliedUSD - borrowedUSD });
      }
    }

    if (deployments.length > 0) return { deployments };
  } catch (e: any) {
    console.warn(`[defi/summary] ${e.message}`);
  }

  return null;
}

// Fetch DeFi wallet balances as fallback for wallet_deployments (B.5)
// Uses /api/balance per DeFi wallet ID to get position values
interface DefiWalletBalance {
  walletId: number;
  walletName: string;
  groupName: string;
  totalEquityUSD: number;
  balances: { asset: string; equityUsd: number }[];
}

async function fetchDefiWalletBalances(venueAccounts: Record<string, VenueAccount>): Promise<DefiWalletBalance[]> {
  // Include all DeFi-typed accounts PLUS all lending group accounts regardless of venueType.
  // "Falconx Pareto Position" is CeFi-typed in Haruko but holds lending receipts (AA_FALCONXUSDC)
  // that must be included for correct Pareto lent amount tracking.
  const lendingAccountNames = new Set(STRATEGY_BUCKET_ACCOUNTS["Prism Overcollaterized Lending"] ?? []);
  const defiAccounts = Object.values(venueAccounts).filter(
    (a) => a.venueType === "DeFi" || lendingAccountNames.has(a.name)
  );
  if (defiAccounts.length === 0) return [];

  const results: DefiWalletBalance[] = [];
  await Promise.all(
    defiAccounts.map(async (acct) => {
      try {
        const data = await harukoFetch("/api/balance", { venueAccountId: String(acct.id) });
        const bals: any[] = data.balances ?? data ?? [];
        const nonZero = (Array.isArray(bals) ? bals : [])
          .filter((b: any) => Math.abs(b.equityUsd ?? b.equity ?? 0) > 0.01)
          .map((b: any) => ({
            asset: b.asset ?? "",
            equityUsd: b.equityUsd ?? (b.equity ?? 0) * (b.refPx ?? 1),
          }));
        const totalEquityUSD = nonZero.reduce((s, b) => s + b.equityUsd, 0);
        results.push({
          walletId: acct.id,
          walletName: acct.name,
          groupName: acct.groupName,
          totalEquityUSD,
          balances: nonZero,
        });
      } catch {
        // skip wallets that fail
      }
    })
  );

  return results;
}

// ═══════════════════════════════════════════════════════════════════
// PRISM RESERVE COMPOSITION API (authoritative source)
// ═══════════════════════════════════════════════════════════════════

interface PrismReserveData {
  prismSupply: number;
  xprismSupply: number;
  prismStaked: number;
  xprismPrice: number;
  aum: number;
  ratio: number;
  apy: number;
  strategies: Record<string, {
    aum: number;
    netApr: number;
    allocation: number;
    counterparty: string;
  }>;
}

async function fetchPrismReserveComposition(): Promise<PrismReserveData> {
  const res = await fetch(PRISM_RESERVE_API);
  if (!res.ok) {
    throw new Error(`PRISM reserve API failed: ${res.status}`);
  }
  const json = await res.json();
  return json as PrismReserveData;
}

// Fetch per-strategy PnL from group_summary_curve (one call per strategy group).
// The /api/strategy/pnl endpoint returns empty; instead we use group_summary_curve
// per strategy (same approach as the Python SDK notebooks).
interface StrategyPnlData {
  groupName: string;
  dailyPnl: number;
  totalEquityUsd: number;
  dailyReturnPct: number;
}

async function fetchStrategyPnlFromCurves(): Promise<StrategyPnlData[]> {
  const results: StrategyPnlData[] = [];

  await Promise.all(
    STRATEGY_GROUPS.map(async (g) => {
      try {
        const data = await harukoFetch("/api/group_summary_curve", {
          group: g.groupId,
          includeAccountBreakdown: "false",
          includeEquity: "true",
          includePositions: "false",
          refreshLiveBalances: "false",
          notionalType: "DEFAULT",
        });
        const ts: any[] = data.timeSeries ?? [];
        if (ts.length === 0) return;
        const latest = ts[ts.length - 1];
        const dailyPnl: number = Array.isArray(latest.equitySummary?.statistics?.pnl)
          ? (latest.equitySummary.statistics.pnl[0] ?? 0)
          : 0;
        const totalEquityUsd: number = latest.equitySummary?.totalEquityUsd ?? 0;
        const dailyReturnPct = totalEquityUsd > 0 ? (dailyPnl / totalEquityUsd) * 100 : 0;
        results.push({ groupName: g.name, dailyPnl, totalEquityUsd, dailyReturnPct });
      } catch (e: any) {
        // Strategy may not have curve data yet
      }
    })
  );

  return results;
}

// Fetch transfers — Haruko requires one venueAccountId per call (per Python SDK),
// so we loop over all CeFi accounts and merge results.
async function fetchTransfers(cefiAccountIds: number[]): Promise<any> {
  if (cefiAccountIds.length === 0) return null;
  const now = Date.now();
  const startOfMonth = new Date();
  startOfMonth.setUTCDate(1);
  startOfMonth.setUTCHours(0, 0, 0, 0);

  const allEntries: any[] = [];
  await Promise.all(
    cefiAccountIds.map(async (id) => {
      try {
        const data = await harukoFetch("/api/transfers", {
          venueAccountId: String(id),
          startTs: String(startOfMonth.getTime()),
          endTs: String(now),
          latest: "100",
        });
        const entries = data.entries ?? data ?? [];
        if (Array.isArray(entries)) {
          allEntries.push(...entries);
        }
      } catch (e: any) {
        // Some accounts may not support transfers — skip silently
      }
    })
  );

  return { entries: allEntries };
}

// Fetch balance adjustments — same pattern, one call per CeFi account
async function fetchBalanceAdjustments(cefiAccountIds: number[]): Promise<any> {
  if (cefiAccountIds.length === 0) return null;
  const now = Date.now();
  const sevenDaysAgo = now - 7 * 24 * 60 * 60 * 1000;

  const allEntries: any[] = [];
  await Promise.all(
    cefiAccountIds.map(async (id) => {
      try {
        const data = await harukoFetch("/api/balance_adjustments", {
          venueAccountIds: String(id),
          type: "FUNDING",
          startTs: String(sevenDaysAgo),
          endTs: String(now),
        });
        const entries = data.entries ?? data ?? [];
        if (Array.isArray(entries)) {
          allEntries.push(...entries);
        }
      } catch (e: any) {
        // Some accounts may not support balance adjustments — skip silently
      }
    })
  );

  return { entries: allEntries };
}

// ═══════════════════════════════════════════════════════════════════
// ON-CHAIN DATA FETCHING
// ═══════════════════════════════════════════════════════════════════

// Returns a Chainlink USD price (e.g. 3200.50 for ETH/USD)
async function getChainlinkPriceUSD(provider: ethers.Provider, feedAddress: string): Promise<number> {
  const feed = new ethers.Contract(feedAddress, CHAINLINK_ABI, provider);
  const [decimals, roundData] = await Promise.all([
    feed.decimals() as Promise<number>,
    feed.latestRoundData() as Promise<[bigint, bigint, bigint, bigint, bigint]>,
  ]);
  const answer: bigint = roundData[1]; // int256 answer
  if (answer <= 0n) throw new Error(`Chainlink feed ${feedAddress} returned non-positive answer`);
  return Number(answer) / Math.pow(10, decimals);
}

// Fetch Maple Finance "High Yield Secured Lending" pool LTV via GraphQL.
//
// METHODOLOGY NOTE: Maple Finance open-term institutional loans manage collateral
// entirely off-chain through the pool delegate (not stored on-chain or exposed via API).
// The GraphQL API returns only the collateral asset TYPE (BTC, ETH, PYUSD, XRP),
// not the collateral amount. We therefore estimate the pool's LTV using Maple's
// published target LTV requirements per collateral type:
//
//   Volatile crypto (BTC, ETH)    → 70% LTV
//   Mid-cap crypto  (XRP, SOL)    → 65% LTV
//   USD stablecoins (PYUSD, USDC) → 95% LTV
//
// The resulting blended LTV reflects the pool's actual loan composition (live principalOwed
// per collateral type from GraphQL) and changes as the pool's loan book evolves over time.
//
// Returns pool-level aggregate borrow (total principalOwed) and estimated collateral USD.
// PRISM's effective LTV as a depositor equals the pool-level LTV (proportional exposure).
async function fetchMapleLTV(): Promise<MapleLTVData> {
  const query = `{
    poolV2(id: "${MAPLE_POOL_ADDRESS}") {
      openTermLoans(first: 100, where: { state: Active }) {
        id principalOwed collateral { asset }
      }
    }
  }`;

  const res = await fetch(MAPLE_GRAPHQL_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`Maple GraphQL HTTP ${res.status}`);

  const json = await res.json();
  if (json.errors?.length) throw new Error(`Maple GraphQL: ${json.errors[0].message}`);

  const loans: any[] = json.data?.poolV2?.openTermLoans ?? [];
  if (loans.length === 0) throw new Error("Maple GraphQL: no active loans returned for pool");

  // Target LTV by collateral asset type (Maple's published standard requirements)
  const TARGET_LTV_BY_ASSET: Record<string, number> = {
    BTC: 0.70, WBTC: 0.70,
    ETH: 0.70, WETH: 0.70,
    XRP: 0.65, SOL: 0.65, AVAX: 0.65,
    PYUSD: 0.95, USDC: 0.95, USDT: 0.95, DAI: 0.95, GUSD: 0.95,
  };
  const DEFAULT_LTV = 0.70;

  let totalBorrowUSD = 0;
  let totalEstimatedCollateralUSD = 0;
  let loanCount = 0;

  for (const loan of loans) {
    const principalRaw = BigInt(loan.principalOwed ?? "0");
    if (principalRaw <= 0n) continue;

    // principalOwed is in USDC base units (6 decimals)
    const principalUSD = Number(principalRaw) / 1e6;
    const asset = (loan.collateral?.asset ?? "").toUpperCase();
    const ltvTarget = TARGET_LTV_BY_ASSET[asset] ?? DEFAULT_LTV;

    // Estimate collateral from target LTV: collateral = principal / LTV
    const estimatedCollateralUSD = principalUSD / ltvTarget;

    totalBorrowUSD += principalUSD;
    totalEstimatedCollateralUSD += estimatedCollateralUSD;
    loanCount++;
  }

  if (loanCount === 0 || totalEstimatedCollateralUSD === 0) {
    throw new Error("Maple: no valid loan principal data returned");
  }

  return { totalBorrowUSD, totalCollateralUSD: totalEstimatedCollateralUSD, loanCount };
}

// Fetch Pareto/FalconX lending LTV.
//
// PRISM holds AA_FALCONXUSDC tokens in the Idle CDO credit vault (FalconX Pareto).
// PRISM is a DEPOSITOR (lender) in the vault, NOT a Morpho borrower — confirmed on-chain:
// PRISM's position on the AA_FALCONXUSDC/USDC Morpho market has zero borrowShares and zero
// collateral. The underlying credit facility's LTV (FalconX borrow / FalconX collateral)
// is managed off-chain and not exposed through any public API.
//
// For now this function throws to signal that Pareto LTV is unavailable. The Pareto position
// is still recorded in the dashboard (via paretoLentAmountUSD from Haruko) but does not
// contribute to the weighted average LTV calculation.
async function fetchParetoMorphoLTV(
  _provider: ethers.Provider,
  _paretoLentAmountUSD: number,
): Promise<ParetoMorphoData> {
  throw new Error(
    "Pareto LTV unavailable: PRISM holds AA_FALCONXUSDC as a vault depositor; " +
    "the underlying FalconX credit facility LTV is not exposed via any public API or on-chain contract."
  );
}

interface OnChainData {
  prismTotalSupply: number;   // human-readable (divided by 10^decimals)
  prismDecimals: number;
  xprismExchangeRate: number; // assets per 1 xPRISM (human-readable)
  xprismTotalSupply: number;  // human-readable
}

async function fetchOnChainData(): Promise<OnChainData> {
  const provider = new ethers.JsonRpcProvider(ETH_RPC_URL);

  const prism = new ethers.Contract(PRISM_ADDRESS, ERC20_ABI, provider);
  const xprism = new ethers.Contract(XPRISM_ADDRESS, ERC4626_ABI, provider);

  const [prismSupplyRaw, prismDecimals, xprismDecimals, xprismSupplyRaw] =
    await Promise.all([
      prism.totalSupply() as Promise<bigint>,
      prism.decimals() as Promise<number>,
      xprism.decimals() as Promise<number>,
      xprism.totalSupply() as Promise<bigint>,
    ]);

  // convertToAssets(1 full xPRISM token) gives the exchange rate
  const oneShare = 10n ** BigInt(xprismDecimals);
  const assetsPerShareRaw: bigint = await xprism.convertToAssets(oneShare);

  const prismTotalSupply = Number(ethers.formatUnits(prismSupplyRaw, prismDecimals));
  const xprismTotalSupply = Number(ethers.formatUnits(xprismSupplyRaw, xprismDecimals));
  const xprismExchangeRate = Number(ethers.formatUnits(assetsPerShareRaw, prismDecimals));

  return {
    prismTotalSupply,
    prismDecimals,
    xprismExchangeRate,
    xprismTotalSupply,
  };
}

// xPRISM Net APY — sourced directly from PRISM reserve API (no historical calculation needed)

// ═══════════════════════════════════════════════════════════════════
// METRIC COMPUTATION
// ═══════════════════════════════════════════════════════════════════

function computeFromSummary(
  totalAUM: number,
  strategySummaries: Record<string, any>,
  primarySummaryData: any | null,
  positionsData: any | null = null
) {
  // Use the first available strategy summary for detailed breakdowns (balances, venue data, etc.)
  // Merge balances and venue data from ALL strategy summaries
  const mergedBalances: any[] = [];
  const mergedAssetProducts: any[] = [];
  const mergedVenueData: any[] = [];

  for (const groupId of Object.keys(strategySummaries)) {
    const data = strategySummaries[groupId];
    if (!data) continue;
    const summary = data.summary ?? data;
    mergedBalances.push(...(summary.balances ?? []));
    mergedAssetProducts.push(...(summary.summaryByAssetByProduct ?? []));
    mergedVenueData.push(...(summary.summaryByVenueByAsset ?? []));
  }

  // A.4 — Capital Allocation by Strategy
  const capitalAllocation: CapitalAllocationEntry[] = STRATEGY_GROUPS.map((g) => {
    const data = strategySummaries[g.groupId];
    const summary = data?.summary ?? data;
    const equity: number = summary?.totalEquityUSD ?? 0;
    return {
      groupId: g.groupId,
      groupName: g.name,
      totalEquityUSD: equity,
      percentage: totalAUM > 0 ? (equity / totalAUM) * 100 : 0,
    };
  });

  // B.1 — Vault Gross Leverage
  // Gross leverage = (spot USD + |futures notional USD|) / AUM
  // Using absolute values of both legs so delta-neutral C&C books count correctly
  const assetProducts: any[] = mergedAssetProducts;
  const spotUSD = mergedBalances.reduce(
    (sum: number, b: any) => sum + (b.equity ?? 0) * (b.refPx ?? 1),
    0
  );
  const futuresAbsNotionalUSD = (positionsData?.futuresPositions ?? []).reduce(
    (sum: number, p: any) => sum + Math.abs(p.sizeUsd ?? 0),
    0
  );
  const totalGrossExposure = spotUSD + futuresAbsNotionalUSD;
  const vaultGrossLeverage = totalAUM > 0 ? totalGrossExposure / totalAUM : 0;

  // B.1 breakdown — by strategy bucket
  const grossLeverageByStrategy: GrossLeverageStrategyEntry[] = STRATEGY_GROUPS.map((g) => {
    const data = strategySummaries[g.groupId];
    const summary = data?.summary ?? data;
    const groupBalances: any[] = summary?.balances ?? [];
    const strategyEquityUSD: number = summary?.totalEquityUSD ?? 0;
    const gSpotUSD = groupBalances.reduce(
      (sum: number, b: any) => sum + (b.equity ?? 0) * (b.refPx ?? 1),
      0
    );
    return { strategy: g.name, spotUSD: gSpotUSD, futuresAbsNotionalUSD: 0, grossExposureUSD: gSpotUSD, strategyEquityUSD, grossLeverage: strategyEquityUSD > 0 ? gSpotUSD / strategyEquityUSD : 0 };
  });
  // All futures positions belong to C&C
  const ccStratEntry = grossLeverageByStrategy.find((e) => e.strategy === "Prism Cash & Carry");
  if (ccStratEntry) {
    ccStratEntry.futuresAbsNotionalUSD = futuresAbsNotionalUSD;
    ccStratEntry.grossExposureUSD += futuresAbsNotionalUSD;
    ccStratEntry.grossLeverage = ccStratEntry.strategyEquityUSD > 0 ? ccStratEntry.grossExposureUSD / ccStratEntry.strategyEquityUSD : 0;
  }

  // B.1 breakdown — by deployment (venue)
  const venueToStrategy = new Map<string, string>();
  for (const g of STRATEGY_GROUPS) {
    const data = strategySummaries[g.groupId];
    const summary = data?.summary ?? data;
    for (const item of summary?.summaryByVenueByAsset ?? []) {
      if (item.venue) venueToStrategy.set(item.venue, g.name);
    }
  }
  const venueSpotMap = new Map<string, number>();
  for (const item of mergedVenueData) {
    const venue: string = item.venue ?? "unknown";
    venueSpotMap.set(venue, (venueSpotMap.get(venue) ?? 0) + (item.totalEquityUSD ?? 0));
  }
  const venueFuturesMap = new Map<string, number>();
  for (const p of positionsData?.futuresPositions ?? []) {
    const venue: string = p.venue ?? "unknown";
    venueFuturesMap.set(venue, (venueFuturesMap.get(venue) ?? 0) + Math.abs(p.sizeUsd ?? 0));
  }
  const allVenues = new Set([...venueSpotMap.keys(), ...venueFuturesMap.keys()]);
  const grossLeverageByDeployment: GrossLeverageDeploymentEntry[] = Array.from(allVenues)
    .map((venue) => {
      const spotEquityUSD = venueSpotMap.get(venue) ?? 0;
      const futAbsNotional = venueFuturesMap.get(venue) ?? 0;
      const grossExposureUSD = spotEquityUSD + futAbsNotional;
      return {
        venue,
        strategy: venueToStrategy.get(venue) ?? "Unknown",
        spotEquityUSD,
        futuresAbsNotionalUSD: futAbsNotional,
        grossExposureUSD,
        pctOfAUM: totalAUM > 0 ? grossExposureUSD / totalAUM : 0,
      };
    })
    .sort((a, b) => b.grossExposureUSD - a.grossExposureUSD);

  // B.6 — Portfolio Liquidity Profile
  let liquidAssetsUSD = 0;
  let illiquidAssetsUSD = 0;

  for (const item of assetProducts) {
    const asset: string = item.asset ?? "";
    const value: number = item.totalEquityUSD ?? 0;
    const isLiquid =
      LIQUID_ASSETS.includes(asset) ||
      Math.abs(item.futuresPosition ?? 0) > 0 ||
      Math.abs(item.optionsPosition ?? 0) > 0;

    if (isLiquid) {
      liquidAssetsUSD += value;
    } else {
      illiquidAssetsUSD += value;
    }
  }

  const portfolioLiquidity = {
    liquidAssetsUSD,
    illiquidAssetsUSD,
    liquidityRatio: totalAUM > 0 ? liquidAssetsUSD / totalAUM : 0,
  };

  // B.7 — Counterparty Exposure
  const venueData: any[] = mergedVenueData;
  const counterpartyMap = new Map<string, number>();

  for (const item of venueData) {
    const venue: string = item.venue ?? "unknown";
    const equity: number = item.totalEquityUSD ?? 0;
    counterpartyMap.set(venue, (counterpartyMap.get(venue) ?? 0) + equity);
  }

  const counterpartyExposure: CounterpartyExposureEntry[] = Array.from(counterpartyMap.entries())
    .map(([venue, totalExposureUSD]) => ({
      venue,
      totalExposureUSD,
      percentage: totalAUM > 0 ? (totalExposureUSD / totalAUM) * 100 : 0,
    }))
    .sort((a, b) => b.totalExposureUSD - a.totalExposureUSD);

  // B.8 — Stablecoin Holdings (consolidated by asset)
  const balances: any[] = mergedBalances;
  const stablecoinMap = new Map<string, number>();
  let totalStablecoinUSD = 0;

  for (const bal of balances) {
    const asset: string = bal.asset ?? "";
    if (STABLECOINS.includes(asset)) {
      const valueUSD = (bal.equity ?? 0) * (bal.refPx ?? 1);
      if (valueUSD > 0) {
        stablecoinMap.set(asset, (stablecoinMap.get(asset) ?? 0) + valueUSD);
        totalStablecoinUSD += valueUSD;
      }
    }
  }

  const stablecoinHoldings: StablecoinHolding[] = Array.from(stablecoinMap.entries())
    .map(([asset, balanceUSD]) => ({
      asset,
      balanceUSD,
      percentage: totalStablecoinUSD > 0 ? (balanceUSD / totalStablecoinUSD) * 100 : 0,
    }))
    .sort((a, b) => b.balanceUSD - a.balanceUSD);

  // B.9 — Novel Stablecoin Risk
  const novelStablecoinHoldings: StablecoinHolding[] = [];

  for (const bal of balances) {
    const asset: string = bal.asset ?? "";
    if (NOVEL_STABLECOINS.includes(asset)) {
      const valueUSD = (bal.equity ?? 0) * (bal.refPx ?? 1);
      if (valueUSD > 0) {
        novelStablecoinHoldings.push({
          asset,
          balanceUSD: valueUSD,
          percentage: totalAUM > 0 ? (valueUSD / totalAUM) * 100 : 0,
        });
      }
    }
  }
  novelStablecoinHoldings.sort((a, b) => b.balanceUSD - a.balanceUSD);

  // B.10 — Non-core Token Allocation (C&C)
  let coreUSD = 0;
  let nonCoreUSD = 0;
  const nonCoreDetails: NonCoreAllocationDetail[] = [];

  for (const item of assetProducts) {
    const asset: string = item.asset ?? "";
    const value: number = item.totalEquityUSD ?? 0;
    const isCore = CORE_ASSETS.includes(asset);

    if (isCore) {
      coreUSD += value;
    } else {
      nonCoreUSD += value;
    }
    if (value > 0) {
      nonCoreDetails.push({ asset, balanceUSD: value, isCore });
    }
  }

  const nonCoreTokenAllocation = {
    coreUSD,
    nonCoreUSD,
    nonCoreRatio: totalAUM > 0 ? nonCoreUSD / totalAUM : 0,
    details: nonCoreDetails.sort((a, b) => b.balanceUSD - a.balanceUSD),
  };

  return {
    totalAUM,
    capitalAllocation,
    vaultGrossLeverage,
    grossLeverageByStrategy,
    grossLeverageByDeployment,
    portfolioLiquidity,
    counterpartyExposure,
    stablecoinHoldings,
    novelStablecoinHoldings,
    nonCoreTokenAllocation,
  };
}

function computeCexMarginTypes(data: any): CexMarginEntry[] {
  const positions: any[] = data.futuresPositions ?? [];
  return positions.map((p: any) => ({
    venue: p.venue ?? "",
    asset: p.coin ?? p.asset ?? "",
    symbol: p.symbol ?? "",
    side: p.side ?? "",
    notionalUSD: Math.abs(p.sizeUsd ?? 0),
    marginMode: p.marginMode ?? "UNKNOWN",
    isCross: p.marginMode === "CROSS",
  }));
}

function computeAverageBasis(data: any): {
  weightedAnnualizedBasisPct: number;
  entries: BasisEntry[];
} {
  const positions: any[] = data.futuresPositions ?? [];
  const entries: BasisEntry[] = [];
  let totalWeightedBasis = 0;
  let totalNotional = 0;
  const now = Date.now();

  for (const p of positions) {
    const futuresPrice: number = p.markPx ?? 0;
    const spotPrice: number = p.underlyingSpotPx ?? 0;
    const notionalUSD: number = Math.abs(p.sizeUsd ?? 0);
    const maturityMs: number = p.staticData?.maturity ?? 0;
    const isPerpetual: boolean = p.staticData?.perpetual === true || maturityMs === 0;

    if (spotPrice <= 0 || notionalUSD <= 0) continue;

    const basis = (futuresPrice - spotPrice) / spotPrice;
    let annualizedBasis: number;

    if (isPerpetual) {
      // For perpetual swaps: use live funding rate if available, annualized.
      // liveFundingRate is per-interval (usually hourly → 8760 intervals/year).
      const liveFundingRate: number = p.liveFundingRate ?? 0;
      const fundingResolution: string = p.staticData?.fundingResolutionType ?? "HOURLY";
      const intervalsPerYear = fundingResolution === "HOURLY" ? 8760 : 365;
      if (liveFundingRate !== 0) {
        annualizedBasis = liveFundingRate * intervalsPerYear * 100;
      } else {
        // Fallback: annualize instantaneous mark-spot basis
        annualizedBasis = basis * 365 * 100;
      }
    } else {
      // For dated futures: annualize using days-to-expiry
      const daysToExpiry = (maturityMs - now) / (1000 * 60 * 60 * 24);
      if (daysToExpiry <= 0) continue;
      annualizedBasis = basis * (365 / daysToExpiry) * 100;
    }

    entries.push({
      asset: p.coin ?? p.asset ?? "",
      venue: p.venue ?? "",
      spotPrice,
      futuresPrice,
      maturityTs: maturityMs,
      notionalUSD,
      annualizedBasisPct: annualizedBasis,
    });

    totalWeightedBasis += annualizedBasis * notionalUSD;
    totalNotional += notionalUSD;
  }

  return {
    weightedAnnualizedBasisPct: totalNotional > 0 ? totalWeightedBasis / totalNotional : 0,
    entries,
  };
}

function computeAverageLTV(
  reserve: PrismReserveData | null,
  defiWalletBalances: DefiWalletBalance[] | null,
  mapleLTVData: MapleLTVData | null,
  paretoMorphoData: ParetoMorphoData | null,
): {
  weightedLTV: number;
  loans: LoanEntry[];
  mapleLentAmountUSD: number | null;
  paretoLentAmountUSD: number | null;
} {
  // --- Step 1: Resolve lent amounts from Haruko wallet balances (used for reporting) ---
  // Multiple wallet entries per protocol (e.g. "Falconx Pareto Position" + "FalconX Pareto ETHEREUM")
  // share the same on-chain address — sum all matching entries.
  let mapleLentAmountUSD: number | null = null;
  let paretoLentAmountUSD: number | null = null;

  if (defiWalletBalances) {
    const mapleTotal = defiWalletBalances
      .filter((w) => w.walletName.toLowerCase().includes("maple"))
      .reduce((sum, w) => sum + w.totalEquityUSD, 0);
    if (mapleTotal > 0.01) mapleLentAmountUSD = mapleTotal;

    const paretoTotal = defiWalletBalances
      .filter((w) => w.walletName.toLowerCase().includes("pareto"))
      .reduce((sum, w) => sum + w.totalEquityUSD, 0);
    if (paretoTotal > 0.01) paretoLentAmountUSD = paretoTotal;
  }

  // Reserve API fallback for Maple (combined Maple + Pareto total — can't split)
  if (mapleLentAmountUSD === null && reserve?.strategies?.overcollateralizedLending) {
    mapleLentAmountUSD = reserve.strategies.overcollateralizedLending.aum;
  }

  // --- Step 2: Build loan entries and compute weighted LTV from real on-chain/API data ---
  const activeLoans: LoanEntry[] = [];
  let totalBorrowUSD = 0;
  let totalCollateralUSD = 0;

  // Maple: uses real per-loan principal + BTC/ETH collateral from Maple Finance GraphQL API
  if (mapleLTVData && mapleLTVData.totalCollateralUSD > 0) {
    const ltv = mapleLTVData.totalBorrowUSD / mapleLTVData.totalCollateralUSD;
    activeLoans.push({
      borrowValue: mapleLTVData.totalBorrowUSD,
      collateralValue: mapleLTVData.totalCollateralUSD,
      ltv,
      maturityTs: Number.MAX_SAFE_INTEGER,
    });
    totalBorrowUSD    += mapleLTVData.totalBorrowUSD;
    totalCollateralUSD += mapleLTVData.totalCollateralUSD;
  } else if (mapleLentAmountUSD !== null && mapleLentAmountUSD > 0.01) {
    // Maple data unavailable — record position without LTV contribution
    activeLoans.push({ borrowValue: mapleLentAmountUSD, collateralValue: 0, ltv: 0, maturityTs: Number.MAX_SAFE_INTEGER });
  }

  // Pareto: borrow = USDC borrowed by PRISM on Morpho Blue; collateral = AA token value (Haruko)
  if (paretoMorphoData && paretoMorphoData.collateralUSD > 0.01) {
    const ltv = paretoMorphoData.collateralUSD > 0
      ? paretoMorphoData.borrowUSD / paretoMorphoData.collateralUSD
      : 0;
    activeLoans.push({
      borrowValue: paretoMorphoData.borrowUSD,
      collateralValue: paretoMorphoData.collateralUSD,
      ltv,
      maturityTs: Number.MAX_SAFE_INTEGER,
    });
    totalBorrowUSD    += paretoMorphoData.borrowUSD;
    totalCollateralUSD += paretoMorphoData.collateralUSD;
  } else if (paretoLentAmountUSD !== null && paretoLentAmountUSD > 0.01) {
    // Morpho data unavailable — record position without LTV contribution
    activeLoans.push({ borrowValue: paretoLentAmountUSD, collateralValue: 0, ltv: 0, maturityTs: Number.MAX_SAFE_INTEGER });
  }

  return {
    weightedLTV: totalCollateralUSD > 0 ? totalBorrowUSD / totalCollateralUSD : 0,
    loans: activeLoans,
    mapleLentAmountUSD,
    paretoLentAmountUSD,
  };
}

function computeDefiLeverage(data: any): {
  averageLTV: number;
  deployments: DefiDeploymentEntry[];
} | null {
  if (!data) return null;

  const allDeployments: any[] = data.deployments ?? [];
  const entries: DefiDeploymentEntry[] = [];
  let totalSupplied = 0;
  let totalBorrowed = 0;

  for (const deployment of allDeployments) {
    let suppliedUSD = 0;
    let borrowedUSD = 0;
    const protocol: string = deployment.protocol ?? deployment.name ?? "unknown";

    // Use pre-computed suppliedUSD/borrowedUSD from /api/summary/accounts if available
    if (deployment.suppliedUSD > 0 || deployment.borrowedUSD > 0) {
      suppliedUSD = deployment.suppliedUSD ?? 0;
      borrowedUSD = deployment.borrowedUSD ?? 0;
    } else {
      // Fallback: derive from token-level data (/api/defi/wallet_deployments format)
      const tokens: any[] = deployment.tokens ?? [];
      for (const token of tokens) {
        const balanceUSD = Math.abs((token.token?.balance ?? 0) * (token.token?.refPx ?? 0));
        if (token.deploymentTokenType === "DEPLOYMENTEXPOSURE") {
          suppliedUSD += balanceUSD;
        } else if (token.deploymentTokenType === "DEPLOYMENTREWARD") {
          borrowedUSD += balanceUSD;
        }
      }
    }

    const ltv = suppliedUSD > 0 ? borrowedUSD / suppliedUSD : 0;
    entries.push({ protocol, suppliedUSD, borrowedUSD, ltv });
    totalSupplied += suppliedUSD;
    totalBorrowed += borrowedUSD;
  }

  return {
    averageLTV: totalSupplied > 0 ? totalBorrowed / totalSupplied : 0,
    deployments: entries,
  };
}

function computeStrategyPerformance(data: any): StrategyPnlEntry[] {
  const entries: any[] = data.entries ?? [];
  return entries.map((e: any) => ({
    strategyId: e.strategyId ?? "",
    strategyName: e.strategyName ?? "",
    pnl: e.pnl ?? 0,
    timestamp: e.timestamp ?? 0,
  }));
}

function computeMonthlyYieldTransfers(data: any): YieldTransferEntry[] {
  const entries: any[] = data.entries ?? [];

  return entries
    .filter((t: any) => {
      if (YIELD_TRANSFER_COUNTERPARTY && t.counterpartyName !== YIELD_TRANSFER_COUNTERPARTY) {
        return false;
      }
      if (t.type !== "WITHDRAWAL") return false;
      if (t.asset !== YIELD_TRANSFER_ASSET) return false;
      return true;
    })
    .map((t: any) => ({
      counterparty: t.counterpartyName ?? "",
      asset: t.asset ?? "",
      amount: t.size ?? 0,
      timestamp: t.timestamp ?? 0,
      type: t.type ?? "",
    }));
}

// ── Yield Volatility (30d) — std dev of daily NAV returns ───────────
interface YieldVolatilityResult {
  stdDev30d: number;         // standard deviation of daily returns (decimal)
  stdDev30dPct: number;      // same, expressed as percentage
  meanDailyReturn: number;   // average daily return (decimal)
  meanDailyReturnPct: number;
  dataPointsUsed: number;
}

function computeYieldVolatility(timeSeries: any[]): YieldVolatilityResult | null {
  if (!timeSeries || timeSeries.length < 2) return null;

  // Extract daily PnL and equity from time series, sorted by timestamp.
  // Using PnL/equity for returns (instead of equity changes) to exclude capital flows.
  const dataPoints: { ts: number; equity: number; dailyPnl: number }[] = timeSeries
    .map((pt: any) => ({
      ts: pt.timestamp ?? 0,
      equity: pt.equitySummary?.totalEquityUsd ?? 0,
      dailyPnl: Array.isArray(pt.equitySummary?.statistics?.pnl)
        ? (pt.equitySummary.statistics.pnl[0] ?? 0)  // index 0 = daily
        : 0,
    }))
    .filter((v) => v.equity > 0)
    .sort((a, b) => a.ts - b.ts);

  // Use last 30 data points
  const recent = dataPoints.slice(-30);
  if (recent.length < 2) return null;

  // Compute daily returns: dailyPnl / equity (PnL-based, excludes deposits/withdrawals)
  const dailyReturns: number[] = [];
  for (const pt of recent) {
    if (pt.equity > 0) {
      dailyReturns.push(pt.dailyPnl / pt.equity);
    }
  }

  if (dailyReturns.length === 0) return null;

  const mean = dailyReturns.reduce((s, r) => s + r, 0) / dailyReturns.length;
  const variance =
    dailyReturns.reduce((s, r) => s + Math.pow(r - mean, 2), 0) / dailyReturns.length;
  const stdDev = Math.sqrt(variance);

  return {
    stdDev30d: stdDev,
    stdDev30dPct: stdDev * 100,
    meanDailyReturn: mean,
    meanDailyReturnPct: mean * 100,
    dataPointsUsed: dailyReturns.length,
  };
}

// ── Exchange Asset Distribution — % assets per exchange ────────────
interface ExchangeDistributionEntry {
  exchange: string;
  totalEquityUSD: number;
  percentage: number;
}

function computeExchangeAssetDistribution(
  counterpartyExposure: CounterpartyExposureEntry[] | null,
  totalAUM: number
): ExchangeDistributionEntry[] | null {
  if (!counterpartyExposure || counterpartyExposure.length === 0) return null;

  // counterpartyExposure already has venue -> totalExposureUSD
  // Re-map to "exchange" terminology and compute % of total AUM
  return counterpartyExposure.map((ce) => ({
    exchange: ce.venue,
    totalEquityUSD: ce.totalExposureUSD,
    percentage: totalAUM > 0 ? (ce.totalExposureUSD / totalAUM) * 100 : 0,
  }));
}

function computeFundingRatePayouts(data: any): {
  totalFundingUSD: number;
  byAsset: FundingRateEntry[];
} {
  const entries: any[] = data.entries ?? [];
  const byAssetMap = new Map<string, { total: number; count: number }>();
  let totalFundingUSD = 0;

  for (const adj of entries) {
    if (!FUNDING_TYPES.includes(adj.type)) continue;

    const amount: number = adj.size ?? 0;
    const asset: string = adj.asset ?? "unknown";

    totalFundingUSD += amount;

    const existing = byAssetMap.get(asset) ?? { total: 0, count: 0 };
    existing.total += amount;
    existing.count += 1;
    byAssetMap.set(asset, existing);
  }

  const byAsset: FundingRateEntry[] = Array.from(byAssetMap.entries())
    .map(([asset, { total, count }]) => ({ asset, totalFunding: total, count }))
    .sort((a, b) => b.totalFunding - a.totalFunding);

  return { totalFundingUSD, byAsset };
}

// ═══════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════

async function main() {
  // 1. First fetch venue accounts (needed for dynamic IDs)
  const venueAccountsResult = await safeFetch("venues", fetchVenueAccounts);
  const venueAccounts = venueAccountsResult.data ?? {};
  const ccAccountIds = getCashCarryAccountIds(venueAccounts);
  const allCefiIds = getAllCefiAccountIds(venueAccounts);

  if (ccAccountIds.length > 0) {
    console.log(`Cash & Carry venue account IDs (dynamic): [${ccAccountIds.join(", ")}]`);
  } else {
    console.warn("No Cash & Carry venue accounts found — positions/funding won't be fetched");
  }
  console.log(`All CeFi venue account IDs: [${allCefiIds.join(", ")}]`);
  const defiIds = getDefiAccountIds(venueAccounts);
  console.log(`DeFi venue account IDs: [${defiIds.join(", ")}]`);

  // 2. Fetch PRISM reserve API (authoritative) + all Haruko endpoints concurrently
  const [
    reserveResult,
    aumResult,
    strategySummariesResult,
    positionsResult,
    defiResult,
    strategyPnlResult,
    transfersResult,
    adjustmentsResult,
    onChainResult,
    defiBalancesResult,
  ] = await Promise.all([
    safeFetch("prism/reserve-composition", fetchPrismReserveComposition),
    safeFetch("group_summary_curve/AUM", fetchGroupSummaryCurveAUM),
    safeFetch("strategy_summaries", fetchStrategySummaries),
    safeFetch("aggregate/position", () => fetchAggregatePositions(ccAccountIds)),
    safeFetch("defi/deployments", () => fetchDefiDeployments(venueAccounts)),
    safeFetch("strategy/pnl_curves", fetchStrategyPnlFromCurves),
    safeFetch("transfers", () => fetchTransfers(allCefiIds)),
    safeFetch("balance_adjustments", () => fetchBalanceAdjustments(ccAccountIds)),
    safeFetch("on-chain/prism+xprism", fetchOnChainData),
    safeFetch("defi/wallet_balances", () => fetchDefiWalletBalances(venueAccounts)),
  ]);

  // 2b. LTV-specific fetches
  const [mapleLTVResult, paretoMorphoResult] = await Promise.all([
    safeFetch("maple/ltv", fetchMapleLTV),
    // Pareto LTV: always throws — PRISM is a vault depositor, not a Morpho borrower.
    // paretoLentAmountUSD from Haruko is still reported in the dashboard via computeAverageLTV.
    safeFetch("pareto/morpho-ltv", () => fetchParetoMorphoLTV(new ethers.JsonRpcProvider(ETH_RPC_URL), 0)),
  ]);

  const reserve = reserveResult.data;

  // 3. Collect errors
  const fetchErrors: string[] = [
    venueAccountsResult, reserveResult, aumResult, strategySummariesResult,
    positionsResult, defiResult, strategyPnlResult,
    transfersResult, adjustmentsResult, onChainResult, defiBalancesResult,
    mapleLTVResult, paretoMorphoResult,
  ]
    .map((r) => r.error)
    .filter((e): e is string => e !== null);

  // 4. Compute metrics — PRISM reserve API is authoritative; Haruko is supplementary
  const totalAUM = reserve?.aum ?? aumResult.data?.totalAUM ?? 0;
  const strategySummaries = strategySummariesResult.data ?? {};
  const summaryMetrics = totalAUM > 0
    ? computeFromSummary(totalAUM, strategySummaries, null, positionsResult.data)
    : null;
  const onChain = onChainResult.data;
  const collateralizationRatio = reserve?.ratio
    ?? (onChain && onChain.prismTotalSupply > 0 && totalAUM > 0
      ? (totalAUM / onChain.prismTotalSupply) * 100 : null);
  const navPerShare = reserve
    ? reserve.aum / reserve.prismSupply
    : (onChain && onChain.prismTotalSupply > 0 && totalAUM > 0
      ? totalAUM / onChain.prismTotalSupply : null);

  // Compute detail arrays
  const cexMarginTypes = positionsResult.data ? computeCexMarginTypes(positionsResult.data) : [];
  const averageBasis = positionsResult.data ? computeAverageBasis(positionsResult.data) : { weightedAnnualizedBasisPct: 0, entries: [] };
  const averageLTV = computeAverageLTV(reserve, defiBalancesResult.data, mapleLTVResult.data, paretoMorphoResult.data);
  const defiLeverage = defiResult.data ? computeDefiLeverage(defiResult.data) : null;
  const strategyPerformance = strategyPnlResult.data ?? [];
  const monthlyYieldTransfers = transfersResult.data ? computeMonthlyYieldTransfers(transfersResult.data) : [];
  const fundingRatePayouts = adjustmentsResult.data ? computeFundingRatePayouts(adjustmentsResult.data) : { totalFundingUSD: 0, byAsset: [] };
  const yieldVolatility = aumResult.data?.timeSeries ? computeYieldVolatility(aumResult.data.timeSeries) : null;

  // Build DeFi protocol concentration — always use per-wallet data for consistent protocol names
  // (aggregated strategy-level data is already captured in the `strategies` array)
  const PROTOCOL_NAME_MAP: Record<string, string> = {
    "Maple Secured Inst Lending pool ETHEREUM": "Maple (Overcollateralized Lending)",
    "PRISM Monarq Sentora PYUSD Vault ETHEREUM": "Sentora PYUSD Vault (DeFi Yield)",
    "Open_Eden_August_Monarq_1 ETHEREUM": "August/Monarq (DeFi Yield)",
    "PRISM Monarq Operations (Centrifuge) ETHEREUM": "Centrifuge (DeFi Yield)",
    "PRISM Monarq Operations (Neutrl) ETHEREUM": "Neutrl (DeFi Yield)",
    "PRISM Monarq Steakhouse AUSD ETHEREUM": "Steakhouse AUSD (DeFi Yield)",
  };

  /** Derive a friendly display name from a raw Haruko wallet name.
   *  Known wallets use the explicit map; unknown wallets get auto-cleaned. */
  function toProtocolName(walletName: string): string {
    if (PROTOCOL_NAME_MAP[walletName]) return PROTOCOL_NAME_MAP[walletName];
    // Auto-clean: strip chain suffix, common prefixes, underscores
    let name = walletName
      .replace(/\s+ETHEREUM$/i, "")
      .replace(/^PRISM\s+Monarq\s+(Operations\s*\(?)?/i, "")
      .replace(/^Open_Eden_/i, "")
      .replace(/_/g, " ")
      .replace(/\)+$/, "")
      .trim();
    return name || walletName;
  }

  const defiProtocolConcentration: { protocol: string; suppliedUSD: number; concentrationPct: number }[] = (() => {
    const wallets = defiBalancesResult.data ?? [];
    if (wallets.length === 0) return [];
    const totalDefiUSD = wallets.reduce((sum: number, w: DefiWalletBalance) => sum + w.totalEquityUSD, 0);
    return wallets
      .filter((w: DefiWalletBalance) => w.totalEquityUSD > 0)
      .map((w: DefiWalletBalance) => ({
        protocol: toProtocolName(w.walletName),
        suppliedUSD: w.totalEquityUSD,
        concentrationPct: totalDefiUSD > 0 ? (w.totalEquityUSD / totalDefiUSD) * 100 : 0,
      }))
      .sort((a, b) => b.concentrationPct - a.concentrationPct);
  })();

  // ═══════════════════════════════════════════════════════════════════
  // BUILD SINGLE DOCUMENT — all metrics in one collection
  // ═══════════════════════════════════════════════════════════════════

  const now = new Date();
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const dateKey = date.toISOString().slice(0, 10);
  const snapshotId = now.toISOString(); // unique per run — allows multiple snapshots per day

  // Build strategy allocation array
  const strategies: any[] = [];
  if (reserve?.strategies) {
    const stratNames: Record<string, string> = {
      cashAndCarryArbitrage: "Cash & Carry",
      deFiYieldStrategies: "DeFi Yield",
      overcollateralizedLending: "Overcollateralized Lending",
      tokenizedUsTreasuries: "Tokenized US Treasuries",
    };
    for (const [key, s] of Object.entries(reserve.strategies)) {
      const perfMatch = strategyPerformance.find((sp) =>
        sp.groupName.toLowerCase().includes(key.toLowerCase().replace(/strategies|arbitrage/g, "").slice(0, 8))
      );
      strategies.push({
        strategy: stratNames[key] ?? key,
        strategyKey: key,
        aumUSD: s.aum,
        allocationPct: s.allocation,
        netAprPct: s.netApr,
        counterparty: s.counterparty,
        dailyPnlUSD: perfMatch?.dailyPnl ?? null,
        dailyReturnBps: perfMatch ? perfMatch.dailyReturnPct * 100 : null,
      });
    }
  }
  if (strategies.length === 0 && strategyPerformance.length > 0) {
    // Map Haruko group names to canonical strategy names (matching PRISM API naming)
    const groupToCanonical: Record<string, string> = {
      "Prism Cash & Carry": "Cash & Carry",
      "Prism Monarq Operations": "Tokenized US Treasuries",
      "Prism DEFI": "DeFi Yield",
      "Prism Overcollateralized Lending": "Overcollateralized Lending",
    };
    for (const sp of strategyPerformance) {
      strategies.push({
        strategy: groupToCanonical[sp.groupName] ?? sp.groupName,
        strategyKey: sp.groupName,
        aumUSD: sp.totalEquityUsd,
        allocationPct: totalAUM > 0 ? (sp.totalEquityUsd / totalAUM) * 100 : 0,
        netAprPct: null,
        counterparty: null,
        dailyPnlUSD: sp.dailyPnl,
        dailyReturnBps: sp.dailyReturnPct * 100,
      });
    }
  }

  // Build DeFi wallet positions array
  const defiWalletPositions = (defiBalancesResult.data ?? [])
    .filter((w: DefiWalletBalance) => w.totalEquityUSD > 0)
    .map((w: DefiWalletBalance) => ({
      protocol: w.walletName,
      groupName: w.groupName,
      totalEquityUSD: w.totalEquityUSD,
    }));

  const doc = {
    snapshotId,
    date,
    dateKey,

    // A. Portfolio Overview
    totalAUM,
    collateralizationRatio,
    navPerShare,
    apyPct: reserve?.apy ?? null,

    // On-chain
    prismTotalSupply: onChain?.prismTotalSupply ?? null,
    xprismTotalSupply: onChain?.xprismTotalSupply ?? null,
    xprismExchangeRate: onChain?.xprismExchangeRate ?? null,

    // PRISM Reserve API raw values
    prismSupply: reserve?.prismSupply ?? null,
    xprismSupply: reserve?.xprismSupply ?? null,
    prismStaked: reserve?.prismStaked ?? null,
    xprismPrice: reserve?.xprismPrice ?? null,

    // B. Key Risk Indicators (scalars)
    vaultGrossLeverage: summaryMetrics?.vaultGrossLeverage ?? null,
    grossLeverageByStrategy: summaryMetrics?.grossLeverageByStrategy ?? [],
    grossLeverageByDeployment: summaryMetrics?.grossLeverageByDeployment ?? [],
    averageBasisAnnualizedPct: averageBasis.weightedAnnualizedBasisPct,
    averageLTVPct: averageLTV.weightedLTV * 100,
    activeLoanCount: averageLTV.loans.length,
    mapleLentAmountUSD: averageLTV.mapleLentAmountUSD,
    paretoLentAmountUSD: averageLTV.paretoLentAmountUSD,
    defiLeverageLTV: defiLeverage?.averageLTV ?? null,
    liquidAssetsUSD: summaryMetrics?.portfolioLiquidity?.liquidAssetsUSD ?? null,
    illiquidAssetsUSD: summaryMetrics?.portfolioLiquidity?.illiquidAssetsUSD ?? null,
    liquidityRatio: summaryMetrics?.portfolioLiquidity?.liquidityRatio ?? null,
    coreAssetsUSD: summaryMetrics?.nonCoreTokenAllocation?.coreUSD ?? null,
    nonCoreAssetsUSD: summaryMetrics?.nonCoreTokenAllocation?.nonCoreUSD ?? null,
    nonCoreRatio: summaryMetrics?.nonCoreTokenAllocation?.nonCoreRatio ?? null,
    fundingRatePayoutsUSD: fundingRatePayouts.totalFundingUSD,

    // Yield Volatility
    yieldVol30dStdDevPct: yieldVolatility?.stdDev30dPct ?? null,
    yieldVol30dMeanReturnPct: yieldVolatility?.meanDailyReturnPct ?? null,
    yieldVol30dDataPoints: yieldVolatility?.dataPointsUsed ?? null,

    // Arrays (embedded in same document)
    cexPositions: cexMarginTypes,
    basisEntries: averageBasis.entries,
    strategies,
    counterpartyExposure: summaryMetrics?.counterpartyExposure ?? [],
    stablecoinHoldings: summaryMetrics?.stablecoinHoldings ?? [],
    defiWalletPositions,
    defiProtocolConcentration,
    monthlyYieldTransfers,
    fundingRatePayouts: fundingRatePayouts.byAsset,

    // Metadata
    fetchErrorCount: fetchErrors.length,
    fetchErrors,
    fetchedAt: new Date(),
  };

  // ── Persist to MongoDB ────────────────────────────────────────────
  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  try {
    const db = client.db(MONGODB_DB);
    const col = db.collection(MONGODB_COLLECTION);

    // Drop old unique dateKey index if it exists (was one-per-day, now allows multiple)
    try { await col.dropIndex("dateKey_1"); } catch (_) { /* may not exist */ }

    const result = await col.insertOne(doc);
    console.log(`[${MONGODB_COLLECTION}] Inserted: ${result.insertedId} (${snapshotId})`);

    // Ensure indexes — snapshotId is unique (partial: only docs that have it), dateKey for day queries
    await col.createIndex(
      { snapshotId: 1 },
      { unique: true, partialFilterExpression: { snapshotId: { $exists: true } } }
    );
    await col.createIndex({ dateKey: 1 });
    await col.createIndex({ date: 1 });

    // ── Pretty-print full summary ────────────────────────────────────
    const fmt = (n: number | null | undefined, decimals = 2) =>
      n != null ? n.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals }) : "N/A";
    const fmtUSD = (n: number | null | undefined) =>
      n != null ? "$" + fmt(n) : "N/A";
    const fmtPct = (n: number | null | undefined) =>
      n != null ? fmt(n) + "%" : "N/A";
    const divider = "─".repeat(60);

    console.log("\n" + divider);
    console.log("  PRISM Risk Metrics Summary — " + dateKey);
    console.log(divider);

    // A. Portfolio Overview
    console.log("\n  A. PORTFOLIO OVERVIEW");
    console.log(`  A.1  Total AUM:                ${fmtUSD(doc.totalAUM)}`);
    console.log(`        Collateralization Ratio:  ${fmtPct(doc.collateralizationRatio)}`);
    console.log(`        NAV per Share:            ${doc.navPerShare != null ? "$" + fmt(doc.navPerShare, 6) : "N/A"}`);
    console.log(`        APY:                      ${fmtPct(doc.apyPct)}`);
    if (strategies.length > 0) {
      console.log(`  A.4  Capital Allocation:`);
      for (const sr of strategies) {
        const bps = sr.dailyReturnBps != null ? sr.dailyReturnBps.toFixed(2) + " bps" : "N/A";
        console.log(`        • ${sr.strategy}: ${fmtUSD(sr.aumUSD)} (${fmt(sr.allocationPct)}%) | APR: ${sr.netAprPct != null ? fmt(sr.netAprPct) + "%" : "N/A"} | PnL: ${fmtUSD(sr.dailyPnlUSD)} (${bps})`);
      }
    }

    // B. Key Risk Indicators
    console.log("\n  B. KEY RISK INDICATORS");
    console.log(`  B.1  Vault Gross Leverage:      ${fmt(doc.vaultGrossLeverage)}x`);
    if (doc.grossLeverageByStrategy?.length > 0) {
      console.log(`        By Strategy:`);
      for (const s of doc.grossLeverageByStrategy) {
        console.log(`          • ${s.strategy}: spot=${fmtUSD(s.spotUSD)} futures=${fmtUSD(s.futuresAbsNotionalUSD)} gross=${fmtUSD(s.grossExposureUSD)} equity=${fmtUSD(s.strategyEquityUSD)} → ${fmt(s.grossLeverage)}x`);
      }
    }
    if (doc.grossLeverageByDeployment?.length > 0) {
      console.log(`        By Deployment:`);
      for (const d of doc.grossLeverageByDeployment) {
        const lev = (d.pctOfAUM * 100).toFixed(1);
        console.log(`          • ${d.venue} [${d.strategy}]: spot=${fmtUSD(d.spotEquityUSD)} futures=${fmtUSD(d.futuresAbsNotionalUSD)} gross=${fmtUSD(d.grossExposureUSD)} (${lev}% AUM)`);
      }
    }

    if (cexMarginTypes.length > 0) {
      console.log(`  B.2  CEX Margin Types:          ${cexMarginTypes.length} positions (${cexMarginTypes.filter((m) => m.isCross).length} cross-margin)`);
      for (const m of cexMarginTypes) {
        console.log(`        • ${m.asset} ${m.symbol} @ ${m.venue} | ${m.side} | ${fmtUSD(m.notionalUSD)} | ${m.marginMode}`);
      }
    } else {
      console.log(`  B.2  CEX Margin Types:          No open positions`);
    }

    console.log(`  B.3  Average Basis (C&C):       ${fmtPct(doc.averageBasisAnnualizedPct)} annualized`);
    for (const e of averageBasis.entries) {
      console.log(`        • ${e.asset} @ ${e.venue}: ${fmt(e.annualizedBasisPct)}% ann | spot=${fmt(e.spotPrice, 4)} | futures=${fmt(e.futuresPrice, 4)} | notional=${fmtUSD(e.notionalUSD)}`);
    }

    console.log(`  B.4  Average LTV (Lending):     ${fmtPct(doc.averageLTVPct)} (${doc.activeLoanCount} active loans)`);
    if (doc.mapleLentAmountUSD || doc.paretoLentAmountUSD) {
      console.log(`        Overcollateralized Lending:`);
      if (mapleLTVResult.data) {
        const maple = mapleLTVResult.data;
        const mapleLTV = maple.totalCollateralUSD > 0 ? (maple.totalBorrowUSD / maple.totalCollateralUSD) * 100 : 0;
        console.log(`        • Maple (${maple.loanCount} loans): ${fmtUSD(maple.totalBorrowUSD)} borrow / ${fmtUSD(maple.totalCollateralUSD)} collateral → LTV ${fmt(mapleLTV)}%`);
      } else if (doc.mapleLentAmountUSD) {
        console.log(`        • Maple: ${fmtUSD(doc.mapleLentAmountUSD)} lent [LTV data unavailable: ${mapleLTVResult.error}]`);
      }
      if (paretoMorphoResult.data) {
        const pareto = paretoMorphoResult.data;
        const paretoLTV = pareto.collateralUSD > 0 ? (pareto.borrowUSD / pareto.collateralUSD) * 100 : 0;
        console.log(`        • Pareto/Morpho: ${fmtUSD(pareto.borrowUSD)} borrow / ${fmtUSD(pareto.collateralUSD)} collateral (AA tokens) → LTV ${fmt(paretoLTV)}%`);
      } else if (doc.paretoLentAmountUSD) {
        console.log(`        • Pareto (FalconX): ${fmtUSD(doc.paretoLentAmountUSD)} lent [Morpho LTV unavailable: ${paretoMorphoResult.error}]`);
      }
    }

    if (defiLeverage) {
      console.log(`  B.5  DeFi Leverage (LTV):       ${fmtPct(defiLeverage.averageLTV * 100)}`);
    } else if (defiWalletPositions.length > 0) {
      console.log(`  B.5  DeFi Wallet Positions:`);
      for (const w of defiWalletPositions) {
        console.log(`        • ${w.protocol}: ${fmtUSD(w.totalEquityUSD)}`);
      }
    } else {
      console.log(`  B.5  DeFi Leverage (LTV):       N/A`);
    }

    console.log(`  B.6  Portfolio Liquidity:        ${fmtPct(doc.liquidityRatio != null ? doc.liquidityRatio * 100 : null)} liquid`);
    console.log(`        Liquid:   ${fmtUSD(doc.liquidAssetsUSD)}`);
    console.log(`        Illiquid: ${fmtUSD(doc.illiquidAssetsUSD)}`);

    const ceList = doc.counterpartyExposure;
    if (ceList.length > 0) {
      console.log(`  B.7  Counterparty Exposure:`);
      for (const ce of ceList) {
        console.log(`        • ${ce.venue}: ${fmtUSD(ce.totalExposureUSD)} (${fmtPct(ce.percentage)})`);
      }
    }

    const shList = doc.stablecoinHoldings;
    if (shList.length > 0) {
      console.log(`  B.8  Stablecoin Holdings:`);
      for (const sh of shList) {
        console.log(`        • ${sh.asset}: ${fmtUSD(sh.balanceUSD)} (${fmtPct(sh.percentage)})`);
      }
    } else {
      console.log(`  B.8  Stablecoin Holdings:       None`);
    }

    console.log(`  B.9  Novel Stablecoin Risk:     ${(summaryMetrics?.novelStablecoinHoldings?.length ?? 0) > 0 ? "Present" : "None (clean)"}`);
    console.log(`  B.10 Non-core Allocation (C&C): ${fmtPct(doc.nonCoreRatio != null ? doc.nonCoreRatio * 100 : null)}`);
    console.log(`        Core: ${fmtUSD(doc.coreAssetsUSD)} | Non-core: ${fmtUSD(doc.nonCoreAssetsUSD)}`);
    console.log(`  B.11 Monthly Yield Transfers:   ${monthlyYieldTransfers.length > 0 ? monthlyYieldTransfers.length + " transfers" : "None this period"}`);
    console.log(`  B.12 Funding Rate Payouts:      ${fmtUSD(doc.fundingRatePayoutsUSD)} (${fundingRatePayouts.byAsset.length} assets)`);

    // Additional metrics
    console.log("\n  ADDITIONAL METRICS");
    console.log(`  On-chain:  PRISM supply: ${fmt(doc.prismTotalSupply, 2)} | xPRISM supply: ${fmt(doc.xprismTotalSupply, 2)} | rate: ${fmt(doc.xprismExchangeRate, 6)}`);
    if (yieldVolatility) {
      console.log(`  Yield Vol (30d):  ${fmtPct(doc.yieldVol30dStdDevPct)} std dev | mean: ${fmtPct(doc.yieldVol30dMeanReturnPct)}/day (${doc.yieldVol30dDataPoints} pts)`);
    }

    if (defiProtocolConcentration.length > 0) {
      console.log(`  DeFi Protocol Concentration:`);
      for (const dp of defiProtocolConcentration) {
        console.log(`        • ${dp.protocol}: ${fmtUSD(dp.suppliedUSD)} (${fmtPct(dp.concentrationPct)})`);
      }
    }

    if (fetchErrors.length > 0) {
      console.log("\n  ⚠ FETCH ERRORS (" + fetchErrors.length + "):");
      fetchErrors.forEach((e) => console.warn(`    • ${e}`));
    } else {
      console.log("\n  ✓ All data sources fetched successfully");
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
